# Работа с tournament

## Контекст и границы проекта

Это NestJS-приложение одного бессрочного турнира. Оно получает
события ставок из RabbitMQ, транзакционно обновляет PostgreSQL и предоставляет
HTTP API лидерборда. PostgreSQL — источник истины; Redis и отдельного кэша рейтинга
в текущей реализации нет.

У проекта собственные `package.json`, lockfile, настройки pnpm, Dockerfile и
Compose. Внешний producer используется для межсервисных демо и E2E. Unit-тесты
этого проекта работают без внешней инфраструктуры; интеграционные тесты требуют
PostgreSQL и RabbitMQ, а broker-тесты сами публикуют сообщения. HTTP E2E проверяют
запущенный API без producer. Внешние HTTP-клиенты получают контракт по
`/openapi.json`; согласованность AMQP проверяется отдельно от HTTP OpenAPI.

Перед изменением читай затрагиваемый код и сохраняй пользовательские правки.
`README.md` описывает запуск, `DECISIONS.md` — обоснования и историю решений.
Актуальные команды проверяй в `package.json`: историческое описание не заменяет
текущую реализацию. Существенные решения дописывай в `DECISIONS.md`, сохраняя историю.

## Карта кода и потоки

| Путь                                                             | Ответственность                                                                                                                                           |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/main.ts`                                                    | Bootstrap Nest/Express, JSON body limit 16 KiB, подключение метрик и Swagger, shutdown hooks                                                              |
| `src/app.module.ts`                                              | Сборка модулей приложения, включая `LoggerModule`                                                                                                         |
| `src/swagger.ts`                                                 | Настройка Swagger UI и `/openapi.json`                                                                                                                    |
| `src/logger/`                                                    | Pino, request ID, redaction заголовков и выбор транспорта логов                                                                                           |
| `src/observability/`                                             | OpenTelemetry-инструменты, метрики маршрутов и декоратор `RecordMetrics`                                                                                  |
| `src/config/`                                                    | Глобальный `AppConfigModule`, `AppConfig`, Zod-валидация окружения и курсов                                                                               |
| `src/contracts/bet-event.ts`                                     | AMQP-контракт, идентификаторы, деньги, время и формат очков                                                                                               |
| `src/tournament/services/bet-events.service.ts` и `bet-batch.ts` | Пакетная запись: settlement, rollback, идемпотентность, блокировки и транзакции; `planBatch` применяет события пачки в порядке доставки                   |
| `src/tournament/services/leaderboard.service.ts`                 | SQL чтения топ-100 и персональной позиции                                                                                                                 |
| `src/tournament/controllers/` и `src/tournament/dto.ts`          | HTTP-контроллер и Zod/Swagger-контракты лидерборда                                                                                                        |
| `src/tournament/utils.ts`                                        | Точная арифметика и канонизация десятичных строк                                                                                                          |
| `src/tournament/messaging/`                                      | `BetConsumer`: delivery, классы ошибок БД, пауза при недоступном PostgreSQL, топология RabbitMQ; `EventBatcher`: пачки и полосы игроков; разбор `x-death` |
| `src/tournament/errors/`                                         | `PermanentEventError` для постоянных доменных ошибок                                                                                                      |
| `src/db/entities/`                                               | Drizzle-схемы `bets`, `player_scores`, индексы и ограничения                                                                                              |
| `src/db/database.service.ts` и `src/db/failures.ts`              | Пулы записи событий и чтения лидерборда, Drizzle, health checks, закрытие пулов; классификация ошибок PostgreSQL                                          |
| `drizzle/migrate.ts` и `drizzle/migrations/`                     | Отдельный CLI миграций, SQL и журнал генерации                                                                                                            |
| `src/cli/`                                                       | CLI `dist/redrive-dlq.js`: возврат событий из DLQ с исходным routing key                                                                                  |
| `src/health/`                                                    | Liveness процесса и readiness PostgreSQL/consumer                                                                                                         |
| `rspack.config.mjs` и `scripts/dev.mjs`                          | ESM-сборка и пересборка с перезапуском в разработке                                                                                                       |

Основные цепочки:

- AMQP: `BetConsumer.processingMessage` → `BetConsumer.handleDelivery` →
  полоса игрока в `EventBatcher` → пачка `BetEventsService.handleBatch` → commit → ACK.
- HTTP: `TournamentController` → `LeaderboardService` → один SQL-запрос через пул чтения.
- Readiness: `HealthService` → `DatabaseService.healthCheck` и
  `BetConsumer.isReady`.

`TournamentModule` регистрирует `BetEventsService`, `LeaderboardService` и один
`BetConsumer`; экспортирует consumer для `HealthModule`. `HealthModule` импортирует
`TournamentModule` и `DatabaseModule`. `DatabaseModule` экспортирует общий
`DatabaseService` с двумя пулами: `pool`/`db` для записи событий и
`readerPool`/`reader` для HTTP. Не дублируй consumer или пулы PostgreSQL в
провайдерах фич.

## Стек, сборка и импорты

Закреплённый стенд: Node.js **26.13.1** (`.nvmrc`), pnpm **12.21.0**,
PostgreSQL **18.7**, RabbitMQ **4.4.1**. Основные зависимости: NestJS 12,
TypeScript 6, Rspack 2.3.3, Zod 4, Drizzle и `pg`. Версии бери из `package.json`
и lockfile; обновления согласовывай с Dockerfile, Compose и документацией.

- Используй pnpm и `pnpm install --frozen-lockfile`. Точные версии зависимостей
  закреплены в `package.json` и lockfile. В `pnpm-workspace.yaml` включены
  `saveExact` и строгие peers; автоматическая установка peers отключена.
  Build разрешён для `esbuild`, отключён для `@scarf/scarf` и `protobufjs`.
- Проект использует ESM (`type: module`). Локальные TypeScript-импорты приложения
  и тестов пишутся без `.js` и завершающего `/index`: `../contracts`, `./app.module`.
  Публичные API каталогов остаются в `index.ts`; соседние файлы одной папки
  импортируются напрямую. Используй `import type` / `export type`, объединяй
  импорты из одного пути.
- Внутри tournament сохраняй прямые runtime-импорты `BetEventsService` → `../utils`
  и `TournamentController` → `../dto`, чтобы не создавать цикл через модуль и
  публичный API родительского каталога. Type-only импорты DTO через `..` допустимы.
  Не реэкспортируй точки запуска, мигратор, тесты и CLI-конфиги.
- Нативные `.mjs`-инструменты сохраняют расширения локальных импортов:
  `scripts/dev.mjs` импортирует `../rspack.config.mjs`.
- Rspack собирает `src/main.ts` → `dist/main.js`, `drizzle/migrate.ts` →
  `dist/migrate.js` и `src/cli/redrive-dlq.ts` → `dist/redrive-dlq.js`. Runtime-зависимости внешние: для запуска нужен production
  `node_modules`. Сохраняй SWC `legacyDecorator` / `decoratorMetadata` и
  TypeScript `experimentalDecorators` / `emitDecoratorMetadata` для NestJS.
  Минификация отключена, имена классов и обработчиков сохраняются.
- TypeScript использует `module: ESNext`, `moduleResolution: Bundler` и строгие
  проверки. Rspack не проверяет типы: `pnpm typecheck` отдельно запускает
  `tsc --noEmit`. `tsconfig.build.json` не используется командой `pnpm build`.
- `pnpm dev` использует Rspack watch, перед пересборкой завершает старый Node,
  ждёт `close` и запускает новый процесс после успешной компиляции. SIGINT/SIGTERM
  закрывают приложение, watcher и compiler; таймаут завершения дочернего процесса —
  10 секунд. Запускается только `dist/main.js`, миграции автоматически не применяются.

## HTTP API, конфигурация и логи

| Endpoint            | Поведение                                                        |
| ------------------- | ---------------------------------------------------------------- |
| `GET /leaderboard`  | Требует `X-Player-Id`, возвращает `{ top, player }`              |
| `GET /health/live`  | Проверка живого процесса                                         |
| `GET /health/ready` | Проверяет БД и готовность AMQP consumer; при сбое возвращает 503 |
| `GET /docs`         | Swagger UI                                                       |
| `GET /openapi.json` | OpenAPI для внешних клиентов                                     |

`X-Player-Id` — тестовый идентификатор, не авторизация. Идентификаторы состоят из
1–128 ASCII-символов: первый — буква или цифра, остальные также могут быть `._:-`.
Отсутствующий или некорректный заголовок возвращает 400. `top` содержит не более
100 записей; `player` — запись с полной позицией либо `null`, если строки игрока
нет в `player_scores`. Игрок с нулевым балансом может оставаться в рейтинге.
Очки передаются десятичными строками, позиции — положительными целыми числами.

Runtime-конфигурация принадлежит `src/config/`:

| Переменная                               | Требование                                                                                                                                                     |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NODE_ENV`                               | `development`, `production` или `test`; по умолчанию `development`                                                                                             |
| `PORT`                                   | Целое 1–65535, по умолчанию 3000                                                                                                                               |
| `DATABASE_URL`                           | Обязательный PostgreSQL URL                                                                                                                                    |
| `DATABASE_MAX_POOL`                      | Положительное целое, по умолчанию 8; пул записи событий: пачки consumer и проверка восстановления PostgreSQL                                                   |
| `DATABASE_HTTP_MAX_POOL`                 | Положительное целое, по умолчанию 8; пул чтения лидерборда                                                                                                     |
| `DATABASE_HTTP_STATEMENT_TIMEOUT_MS`     | Целое 100–600000, по умолчанию 2000; `statement_timeout` пула чтения                                                                                           |
| `DATABASE_HTTP_CONNECTION_TIMEOUT_MS`    | Целое 100–60000, по умолчанию 1000; ожидание свободного соединения перед ответом 503                                                                           |
| `RABBITMQ_URL`                           | Обязательный AMQP/AMQPS URL                                                                                                                                    |
| `RABBITMQ_EXCHANGE`                      | Непустая строка, по умолчанию `bets`                                                                                                                           |
| `RABBITMQ_QUEUE`                         | Непустая строка, по умолчанию `tournament.bets`                                                                                                                |
| `RABBITMQ_DEAD_LETTER_EXCHANGE`          | Непустая строка, по умолчанию `bets.dlx`                                                                                                                       |
| `RABBITMQ_DEAD_LETTER_QUEUE`             | Непустая строка, по умолчанию `tournament.bets.dlq`                                                                                                            |
| `RABBITMQ_DEAD_LETTER_ROUTING_KEY`       | Непустая строка, по умолчанию `failed`                                                                                                                         |
| `RABBITMQ_PREFETCH`                      | Целое 1–65535, по умолчанию 1000; должен вмещать несколько пачек                                                                                               |
| `RABBITMQ_MAX_DELIVERY_ATTEMPTS`         | Положительное целое, по умолчанию 5; передаётся в `x-delivery-limit` как число повторных доставок                                                              |
| `EVENT_BATCH_SIZE`                       | Целое 1–1000, по умолчанию 250; наибольшее число событий в одной транзакции записи                                                                             |
| `EVENT_BATCH_CONCURRENCY`                | Целое 1–32, по умолчанию 2; число одновременных пачек, не больше `DATABASE_MAX_POOL`. Произведение с `EVENT_BATCH_SIZE` — не больше 2000                       |
| `EXCHANGE_RATES`                         | Обязательная строка до 10000 символов вида `USD=1;EUR=1.1;RUB=0.01`; коды валют — три заглавные буквы, значения — положительные десятичные строки, USD равен 1 |
| `LOG_LEVEL`                              | `fatal`, `error`, `warn`, `info`, `debug`, `trace` или `silent`; по умолчанию `error`, без дополнительной нормализации                                         |
| `LOKI_URL`                               | Необязательный абсолютный HTTP(S) URL; отсутствие или пустая строка отключает транспорт Loki                                                                   |
| `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL` | Оба обязательны для интеграционных тестов                                                                                                                      |
| `TOURNAMENT_URL`                         | URL API для HTTP E2E, по умолчанию `http://localhost:3000`                                                                                                     |

Значения по умолчанию применяются при отсутствии переменной; переданные
некорректные значения отклоняются схемой. Если `NODE_ENV` ещё не задан,
Vitest устанавливает `test`, который разрешён в `ConfigSchema`.

Start, dev, мигратор и тестовые команды читают `.env` через Node
`--env-file-if-exists`. Образец окружения — `.env.example`; его пароли служат
только локальному демо. Новые входы и переменные требуют runtime-валидации;
HTTP-контракты отражай в Swagger через соответствующие Zod-схемы.

Compose использует `EXCHANGE_RATES=USD=1;EUR=1.1;RUB=0.01` по умолчанию;
при переопределении переменной сохраняй формат пар ключ=значение.
Compose передаёт API `DATABASE_MAX_POOL`, `DATABASE_HTTP_*` и `EVENT_BATCH_*` со
значениями по умолчанию: на экземпляр приходится до 16 соединений PostgreSQL.
PostgreSQL в Compose запускается с `max_connections=160`, `shared_buffers=1GB`,
`wal_compression=lz4` и `max_wal_size=4GB`.

`src/logger/logger.module.ts` настраивает Pino, request ID и redaction.
`src/logger/transports.ts` выбирает транспорт: при заданном `LOKI_URL` логи
отправляются только в Loki; без него при `NODE_ENV != production` и
`process.stdout.isTTY` используется `pino-pretty`, в остальных случаях —
структурированный JSON в stdout.

Используй структурированные Pino-логи. Не выводи пароли, URL с учётными данными,
authorization/cookie-заголовки и произвольные ошибки драйверов с SQL-параметрами.
Ошибки конфигурации сообщают имена полей, а не отвергнутые значения. Учитывай,
что consumer пишет в `error`-логи фиксированный код причины, проверенный `kind`,
размер сообщения, число предыдущих доставок и безопасный код SQLSTATE или
сетевой ошибки, когда он известен. При недоступности PostgreSQL пишется одна
запись о паузе. Тело события, значения полей и текст исключения в логи не идут.

## Метрики методов и маршрутов

`src/observability/metrics.ts` объявляет OpenTelemetry Counter и Histogram;
гистограммы длительности используют секунды. `registerRouteMetrics` из
`src/observability/routes.ts` измеряет ответ `GET /leaderboard` до события
`finish` и подключается в bootstrap.

`RecordMetrics` из `src/observability/metric.decorator.ts` принимает массив
`{ metric: Counter | Histogram, getAttributes?: (result: Result | null) => Attributes }`.
После завершения синхронного метода или Promise все счётчики увеличиваются на 1,
а гистограммы записывают общий замер длительности, вычисленный до `getAttributes`.
При исключении или отклонении Promise метрики тоже записываются: `getAttributes`
получает `null`; при успехе — результат метода после разрешения Promise.
Callbacks должны обрабатывать `null` и не выбрасывать исключения.
Обёртка сохраняет `this`, аргументы, синхронный результат и Reflect-метаданные
NestJS. Метрики consumer и запроса лидерборда подключаются этим декоратором.
Атрибут `outcome` обработки принимает также `data_error`, `infrastructure_error` и
`deferred`. Наблюдаемая метрика `tournament_consumer_paused` равна 1, пока consumer
на паузе из-за недоступности PostgreSQL.

## Деньги, события и время

- Входное сообщение — JSON события напрямую, без обёртки `{ pattern, data }`
  из NestJS Microservices. Routing key задаёт
  `bet.settled` или `bet.rollback`. Строгое тело содержит ровно шесть полей:
  `betId`, `playerId`, `currency`, `stake`, `winAmount`, `settledAt`.
- `currency` — три заглавные ASCII-буквы. `stake` и `winAmount` — неотрицательные
  десятичные строки, до 18 целых и 6 дробных знаков, без экспоненты и лишних ведущих
  нулей. Курсы имеют ту же точность, строго положительны; USD равен 1.
- Очки равны `winAmount × курс к USD`; `stake` не вычитается. Сохраняй арифметику
  через `bigint`: два множителя с масштабом 6 дают масштаб результата 12.
  Суммы и вклад — PostgreSQL `numeric(60,12)`, курс — `numeric(24,6)`.
  `formatPoints` убирает лишние конечные нули. Не используй float-арифметику,
  `parseFloat` и округление при начислении.
- `settledAt` — фактическое время расчёта ставки в источнике, с часовым поясом
  и не более 6 дробных знаков секунды. В БД `settled_at` и `score_reached_at` —
  `timestamptz(6)`, Drizzle использует строковый режим. Не преобразуй время в
  JavaScript `Date` при хранении и сравнении: это теряет микросекунды.
- У rollback денежные поля, валюта и время проходят тот же контракт, но для
  списания используются идентичность ставки и её сохранённый вклад.

## Транзакции, идемпотентность и рейтинг

- `bets` хранит состояние ставки, tombstone, канонизированный settlement, курс, вклад и
  время. `player_scores` — транзакционная проекция баланса и времени достижения.
  Изменения ставки и баланса выполняются одной транзакцией `READ COMMITTED`.
- Порядок advisory-блокировок обязателен: ставка
  `pg_advisory_xact_lock(hashtextextended(betId, 0))`, затем игрок
  `pg_advisory_xact_lock(9137, hashtext(playerId))`, затем чтение состояния.
  Пространства bigint-ключа ставки и пары int-ключей игрока разделены. Коллизии
  хеша уменьшают параллелизм, но не заменяют проверку идентичности по данным.
- Settlement фиксирует курс и вклад при первом начислении; неизвестная курсам
  валюта вызывает постоянную ошибку до записи новой ставки. Баланс обновляется
  атомарным upsert `points + contribution`; не заменяй это чтением и записью в JS.
- Повторный settlement нормализует десятичные `stake`/`winAmount`, сравнивает
  остальные значимые поля и момент времени через PostgreSQL `timestamptz`.
  Эквивалентные часовые пояса допустимы; изменённые данные или микросекунды —
  конфликт. Дубликат ничего не начисляет и не применяет новый курс.
- Чужой `playerId` для занятого `betId` вызывает `PermanentEventError` для обоих
  типов события. Rollback окончателен: неизвестная ставка создаёт tombstone без
  создания игрока, дальнейший settlement игнорируется, повторный rollback
  идемпотентен. Идентификатор отменённой ставки нельзя использовать заново.
- Rollback рассчитанной ставки списывает сохранённый вклад, обнуляет его и
  пересчитывает время в той же транзакции. Строка игрока остаётся при нулевом
  балансе. Не пересчитывай историю по новым курсам и не допускай отрицательных сумм.
- Время достижения текущей суммы — максимальный `settled_at` активных ставок с
  положительным вкладом. Нулевой вклад его не меняет; rollback исключает отменённую
  ставку и пересчитывает максимум. При нулевом балансе время `NULL`.
- Сначала сравниваются очки по убыванию. **Только при равенстве очков** сравнивается
  время достижения по возрастанию с `NULLS LAST`. При совпадении очков и времени,
  включая два `NULL`, используется `player_id COLLATE "C" ASC`. Позиции уникальны,
  начинаются с 1; порядок доставки сообщений на преимущество не влияет.
- Топ-100 и персональная позиция читаются одним SQL-запросом из одного MVCC snapshot
  с одинаковыми правилами сравнения. Персональная позиция может быть за пределами
  топа. Не разделяй чтения так, чтобы они видели разные состояния базы.
- `player_scores_leaderboard_idx` обслуживает верхнюю часть рейтинга;
  `bets_active_player_settled_at_idx` помогает пересчитывать время активных
  положительных вкладов. Персональная позиция — сумма четырёх подсчётов, каждый —
  диапазон `player_scores_leaderboard_idx` (`Index Only Scan`): больше очков; те же
  очки и меньшая пара (время, id); при `NULL`-времени — все с временем и меньший id.
  Стоимость растёт с позицией, а не с размером таблицы; `OR` внутри одного
  `count(*)` возвращает полный `Seq Scan`. Index-only scan зависит от visibility map,
  поэтому миграция `0002` ускоряет autovacuum `player_scores`. Изменения оптимизации
  проверяй замерами и планами SQL, сохраняя единый порядок и точность.

## RabbitMQ и жизненный цикл

- Топология объявляется методом `BetConsumer.assertTopology`, настройки находятся
  в `AppConfig.rabbitmq` рядом с `url`:
  durable topic exchange `bets`, quorum queue `tournament.bets`, routing keys
  `bet.settled` / `bet.rollback`, prefetch 1000. Durable direct DLX `bets.dlx`
  направляет routing key `failed` в quorum DLQ `tournament.bets.dlq`.
  Основная очередь использует `x-dead-letter-strategy: at-least-once` и
  `x-overflow: reject-publish`. Изменения согласовывай с внешними publisher-ами.
  Политика брокера `delivery-limit: -1` относится только к DLQ. Compose применяет
  её при старте RabbitMQ. Для внешнего брокера это часть его конфигурации.
- `handleDelivery` проверяет JSON, routing key, Zod-контракт и лимит тела 16384 байта.
  Redrive публикует через default exchange прямо в очередь турнира. Для такой
  доставки тип события берётся из AMQP-свойства `type`.
- Доставка at-least-once; ACK отправляется только после успешного завершения
  транзакции, на том канале, который получил сообщение. Потеря ACK после commit
  допускает безопасную повторную доставку.
- Невалидный JSON/контракт, неизвестный routing key, превышение размера и
  `PermanentEventError` получают `nack(message, false, false)` и уходят в DLQ.
  Ошибки хранилища классифицирует `classifyDatabaseFailure` по SQLSTATE и кодам
  во всей цепочке `cause` (Drizzle оборачивает ошибки драйвера в `DrizzleQueryError`):
  - недоступность PostgreSQL (сетевые ошибки, таймаут ожидания pg-pool, классы `08`,
    `28`, `3D`, `42`, `53`, `57`, `58`) — `nack(message, false, true)` без
    задержки; consumer отменяет подписку и проверяет пул записи `SELECT 1` через
    1, 2, 4 … 30 с, после успеха подписывается снова. Шаг паузы сбрасывается только
    после сохранённого события. Доставки, дошедшие до хранилища во время паузы,
    возвращаются тем же nack с исходом `deferred`.
  - ошибки данных (классы `22`, `23`) — `nack(message, false, false)`, сразу в DLQ.
  - конфликт блокировок (`55P03`) и прочие ошибки — задержка
    `retryBackoffMs(x-delivery-count)`: 1, 2, 4, 8, 16, 30 с, затем
    `reject(message, true)`. Не заменяй этот reject на nack с requeue: счётчик quorum
    delivery-limit RabbitMQ ≥4.3 зависит от этого различия.
- Хранилище вызывается через `EventBatcher`: игроки распределены по
  `EVENT_BATCH_CONCURRENCY` полосам по хешу `playerId`, полоса пишет одну пачку за раз
  (до `EVENT_BATCH_SIZE` событий) через `BetEventsService.handleBatch`. События одного
  игрока идут в порядке доставки, ожидание не занимает соединение пула и не упирается
  в его `connectionTimeoutMillis`. Доменная ошибка события приходит в его слот.
  При недоступности PostgreSQL или конфликте блокировок повторяются все события
  пачки. При остальных ошибках события записываются по одному, чтобы ошибку получило
  только событие с проблемой. Advisory locks берутся для всех ставок, затем для всех
  игроков, по возрастанию ключа, и остаются защитой между экземплярами.
- `ack()` вызывается после `try`: его исключение (закрытый канал) не запускает
  задержку и `reject`, а отклоняет `handleDelivery`. При вызове из `processingMessage`
  исключения `ack()`/`nack()`/`reject()` перехватывает callback `.catch()`: он пишет
  предупреждение и завершает `work` значением `UnknownError`. Shutdown- и pause-вызовы
  `nack()` выполняются до создания `work` и этим callback не обрабатываются.
- `x-delivery-limit: 5` допускает пять повторных доставок после исходной, то есть
  шесть попыток суммарно; затем сообщение уходит в DLQ.
- Соединение: heartbeat 5 с, reconnect 2 с, connection timeout 5 с. Начальная
  настройка соединения, топологии и consumer ограничена 10 с. Readiness требует
  готового consumer, живого соединения, отсутствия shutdown и паузы из-за PostgreSQL.
- Disconnect, закрытие канала, ошибка setup и broker cancellation сбрасывают
  readiness. Отдельное восстановление канала откладывается через
  `setTimeout(..., 0)`: amqplib закрывает каналы раньше события disconnect.
  Синхронный reconnect нарушает работу connection manager; на полном disconnect
  запланированное восстановление отменяется.
- Shutdown прекращает новые доставки: отменяет таймеры восстановления канала и
  проверки PostgreSQL, отменяет consumer и прерывает задержки повторов (сообщение
  сразу получает `reject`), дожидается `Promise.allSettled(pending)`, затем
  закрывает канал и соединение.
  Доставка после начала shutdown возвращается через nack с requeue.
  Оба пула PostgreSQL закрываются в `DatabaseService.onApplicationShutdown`.
  Пул записи: max `DATABASE_MAX_POOL` (по умолчанию 8), connection timeout 5 с,
  idle timeout 30 с, statement timeout 10 с, lock timeout 5 с, idle in transaction
  timeout 30 с. Пул чтения: max `DATABASE_HTTP_MAX_POOL` (8), ожидание соединения
  `DATABASE_HTTP_CONNECTION_TIMEOUT_MS` (1 с), statement timeout
  `DATABASE_HTTP_STATEMENT_TIMEOUT_MS` (2 с); перегрузка или недоступность БД дают 503.

## Команды, миграции и Docker

Команды выполняются из корня tournament:

| Команда                          | Назначение                                                                                   |
| -------------------------------- | -------------------------------------------------------------------------------------------- |
| `pnpm install --frozen-lockfile` | Установка закреплённых зависимостей                                                          |
| `pnpm build`                     | Сборка приложения и мигратора Rspack                                                         |
| `pnpm start`                     | Запуск собранного приложения                                                                 |
| `pnpm dev`                       | Пересборка и перезапуск при изменении исходников                                             |
| `pnpm typecheck`                 | Проверка типов приложения, тестов и Vitest-конфигов                                          |
| `pnpm check`                     | Build → typecheck → lint → format check → unit-тесты                                         |
| `pnpm test`                      | Unit-тесты без внешней инфраструктуры                                                        |
| `pnpm test:integration`          | PostgreSQL- и RabbitMQ-проверки с изоляцией                                                  |
| `pnpm test:e2e`                  | HTTP-проверки запущенного API без producer                                                   |
| `pnpm db:generate`               | Генерация новой миграции из `src/db/entities/*.entity.ts`                                    |
| `pnpm db:migrate`                | Явное применение собранного мигратора; предварительно нужен build                            |
| `pnpm dlq:redrive`               | Возврат событий из DLQ прямо в очередь турнира; без `--apply` только размер DLQ; нужен build |

Обычные build/check не требуют работающих PostgreSQL, RabbitMQ, producer или сети
после установки зависимостей. При изменениях форматируй затронутые файлы.

SQL-миграции в `drizzle/migrations/` и метаданные в `drizzle/migrations/meta/`
входят в репозиторий. Применённые миграции не
переписывай; изменение схемы сопровождай новой миграцией. `0001` добавляет времена,
заполняет их из сохранённых settlement и перестраивает индекс рейтинга; ранее
утраченную точность времени эта миграция не восстанавливает. `0002` включает для
`player_scores` autovacuum после 1% изменений: каждое начисление обновляет
индексированную строку игрока, а подсчёт позиции полагается на index-only scan.

Мигратор берёт session advisory lock `(194857312, 1)` и применяет миграции на том
же подключении. Сохраняй взаимное расположение `dist/migrate.js` и
`drizzle/migrations/`: SQL находится через
`new URL('../drizzle/migrations', import.meta.url)`. Тот же путь работает
относительно исходного `drizzle/migrate.ts`. Для переносимости
Rspack сохраняет native `import.meta` и `new URL` через
`parser.javascript.importMeta: false` и `url: false`. Старт API и dev не запускают
миграции; Drizzle push на старте не используется.

Compose поднимает PostgreSQL, RabbitMQ, одноразовый `migrate` и API; API ждёт
успешную миграцию и здоровый брокер. Prometheus загружает правила
`observability/alerts.yml`: сообщения в DLQ, пауза consumer, отсутствие consumers.
Для локальной разработки можно поднять только
`postgres rabbitmq`, затем выполнить `pnpm build`, `pnpm db:migrate`, `pnpm dev`.
Перед запуском проверь существующие процессы и порты 3000/5432/5672/15672.

Dockerfile делает frozen install, Rspack build и `pnpm prune --prod`; runtime
получает production dependencies, `dist` и `drizzle/migrations`, работает от пользователя
`node`. В Compose API имеет read-only filesystem с tmpfs `/tmp`, опубликованные
порты привязаны к localhost. Не ослабляй эти настройки без необходимости задачи.

## Проверки по области изменения

| Область                                                                           | Проверки                                                                                                                         |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Схемы событий, деньги, время, конфигурация и проверка пулов при запуске           | `event-contracts.test.ts`, `domain-points.test.ts`, `config.test.ts`, `database-service.test.ts`                                 |
| ACK, валидация, классы ошибок БД, пауза, пачки, полосы игроков и лимит транзакций | `handle-delivery.test.ts`, `database-failures.test.ts`, `event-batcher.test.ts`                                                  |
| Reconnect, закрытие канала, пауза и возобновление, shutdown                       | `bet-consumer.test.ts`                                                                                                           |
| Начисления, блокировки, rollback, SQL рейтинга, таймауты пулов                    | `storage.integration.test.ts`; ответ 503 лидерборда — `leaderboard-service.test.ts`                                              |
| Пакетная запись равна обработке по одному, параллельные пачки                     | `batch-equivalence.integration.test.ts` сравнивает с `test/support/reference-storage.ts`                                         |
| Миграции и backfill                                                               | `storage.integration.test.ts` применяет миграции к чистой схеме; backfill старых данных и повторный запуск CLI проверяй отдельно |
| Quorum delivery counter и DLQ                                                     | `broker.integration.test.ts` на настоящем RabbitMQ                                                                               |
| Redrive DLQ                                                                       | `dead-letter.test.ts`, `redrive.integration.test.ts`; точная доставка, повторный redrive, отсутствующая цель                     |
| HTTP и Swagger                                                                    | `http.e2e.test.ts` против запущенного нового приложения                                                                          |
| Метрики методов                                                                   | `metric.decorator.test.ts`: Counter/Histogram, sync/async, атрибуты результата и ошибок, метаданные NestJS                       |
| Сборка и упаковка                                                                 | `pnpm check`, Docker build, запуск собранных файлов, dev restart; для мигратора также перенос `dist`/`drizzle`                   |

Указанные тестовые файлы находятся в `test/`. Отдельного автоматизированного
теста backfill старых данных сейчас нет; при изменениях миграций добавляй такую
проверку. При изменении начислений сохраняй
покрытие конкурентных дубликатов, разных ставок одного игрока, rollback до/после
settlement, чужого `playerId`, неизвестной валюты и точных дробей. При изменении
рейтинга проверяй равные очки, микросекунды, перерасчёт времени после rollback,
нулевой баланс и игрока за пределами топ-100. Выбирай проверки по риску; для правок
только документации достаточно проверить точность текста и форматирование.

Интеграционные тесты требуют обе TEST-переменные и выполняются по файлам
последовательно. PostgreSQL-тесты создают UUID-схемы с собственными `search_path`
и `migrationsSchema`; TRUNCATE и cleanup относятся только к этим схемам.
Broker-тесты создают exchange и quorum queue с префиксом `test.broker.<UUID>`,
а также временную mirror queue с серверным именем, `exclusive` и `autoDelete`.
Cleanup удаляет только созданные тестом ресурсы.
Сохраняй эти гарантии и используй отдельную тестовую БД.

Для проверки настоящего CLI миграций нужна отдельная временная БД: мигратор не
задаёт `migrationsSchema` и использует `drizzle.__drizzle_migrations`, поэтому
одного `search_path` недостаточно. Проверяй первый и повторный запуск.
Второй экземпляр API изолируй отдельной БД и RabbitMQ vhost: смена HTTP-порта
не мешает consumer забирать сообщения из очереди работающего приложения.
Удаляй только созданные проверкой ресурсы; не очищай пользовательские таблицы,
очереди и volumes и не перезапускай существующий брокер ради теста.

HTTP E2E этого проекта проверяют API без producer; межсервисные демо и E2E
принадлежат внешнему producer. CI выполняет install → `.env.example` → check →
Compose с ожиданием readiness → integration → HTTP E2E. Его
`docker compose down -v` относится только к одноразовому CI-стенду.

## Codebase Memory

Для структурного поиска сначала используй MCP-граф ближайшего проекта:

1. В начале сессии и после compaction подтверди корень проекта и поколение через
   `list_projects` / `index_status`. Не используй граф соседнего или старого checkout.
2. `search_graph` находит символы, `trace_path` проверяет нужные направления вызовов,
   `get_code_snippet` даёт точный код. Проверяй и дочитывай пагинацию.
3. По умолчанию используй Verify: целевые запросы, релевантные связи и исходники
   для существенных утверждений. Scout даёт предварительные положительные находки;
   Auditor требует полной проверки ограниченной области и описания ограничений.
4. После нахождения кандидатов вызови `check_index_coverage` для всех путей;
   для отрицательных или исчерпывающих утверждений добавь `scopes`.
   Отсутствие зарегистрированного пробела не доказывает полноту.
5. Для partial/skipped/excluded/stale/not-tracked файлов читай актуальные исходники
   и указанные диапазоны. Для литералов, конфигов и недостаточных результатов
   графа допустим `rg`. DI, декораторы и callbacks могут требовать прямой проверки:
   отсутствие ребра не доказывает отсутствие вызова.
6. Перед делегированием передай уровень проверки, проект, поколение и свежесть,
   область, запросы и пагинацию, символы/пути, цепочки, пробелы покрытия и выполненный
   source fallback. Агент без MCP опирается на эти данные и точный исходник.
