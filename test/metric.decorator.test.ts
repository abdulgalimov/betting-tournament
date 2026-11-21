import { Get, RequestMethod } from '@nestjs/common';
import { METHOD_METADATA, PATH_METADATA } from '@nestjs/common/constants';
import type { Counter, Histogram } from '@opentelemetry/api';
import { metrics } from '@opentelemetry/api';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RecordMetrics } from '../src/observability';

afterEach(() => vi.restoreAllMocks());

describe('RecordMetrics', () => {
  it('preserves Nest route metadata when wrapping a decorated handler', () => {
    const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;

    class Controller {
      @RecordMetrics([{ metric: counter }] as const)
      @Get('leaderboard')
      run() {
        return 42;
      }
    }

    expect(Reflect.getMetadata(PATH_METADATA, Controller.prototype.run)).toBe(
      'leaderboard',
    );
    expect(Reflect.getMetadata(METHOD_METADATA, Controller.prototype.run)).toBe(
      RequestMethod.GET,
    );
    expect(new Controller().run()).toBe(42);
    expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, undefined);
  });

  it('increments a counter with result attributes and preserves this, arguments and the result', () => {
    const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;

    class Service {
      readonly prefix = 'player';

      @RecordMetrics([
        {
          metric: counter,
          getAttributes: (result: { id: string } | null) => ({
            id: result?.id ?? 'unknown',
          }),
        },
      ])
      run(id: number) {
        return { id: `${this.prefix}-${id}` };
      }
    }

    const result = new Service().run(42);
    expect(result).toEqual({ id: 'player-42' });
    expect(result).not.toBeInstanceOf(Promise);
    expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, {
      id: 'player-42',
    });
  });

  it('waits for an async result before deriving attributes and incrementing a counter', async () => {
    const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;
    const result = { status: 'settled' };
    const getAttributes = vi.fn((value: typeof result | null) => ({
      status: value?.status ?? 'failed',
    }));
    let finish!: (value: typeof result) => void;
    const pending = new Promise<typeof result>((resolve) => {
      finish = resolve;
    });

    class Service {
      @RecordMetrics([{ metric: counter, getAttributes }])
      run() {
        return pending;
      }
    }

    const execution = new Service().run();
    expect(counter.add).not.toHaveBeenCalled();
    expect(getAttributes).not.toHaveBeenCalled();
    finish(result);
    await expect(execution).resolves.toBe(result);
    expect(getAttributes).toHaveBeenCalledExactlyOnceWith(result);
    expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, {
      status: 'settled',
    });
  });

  it.each([false, true])(
    'records mixed metrics once with the same elapsed seconds for an async=%s method',
    async (isAsync) => {
      const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;
      const attributedCounter = {
        add: vi.fn<Counter['add']>(),
      } satisfies Counter;
      const firstHistogram = {
        record: vi.fn<Histogram['record']>(),
      } satisfies Histogram;
      const secondHistogram = {
        record: vi.fn<Histogram['record']>(),
      } satisfies Histogram;
      const now = vi.spyOn(performance, 'now').mockReturnValue(100);
      const result = { status: 'settled' };
      const getCounterAttributes = vi.fn((value: typeof result | null) => {
        now.mockReturnValue(900);
        return { counterStatus: value?.status ?? 'failed' };
      });
      const getFirstAttributes = vi.fn((value: typeof result | null) => ({
        firstStatus: value?.status ?? 'failed',
      }));
      const getSecondAttributes = vi.fn((value: typeof result | null) => ({
        secondStatus: value?.status ?? 'failed',
      }));
      const execute = vi.fn(() => {
        if (isAsync) {
          return Promise.resolve().then(() => {
            now.mockReturnValue(350);
            return result;
          });
        }
        now.mockReturnValue(350);
        return result;
      });

      class Service {
        @RecordMetrics([
          { metric: counter },
          { metric: attributedCounter, getAttributes: getCounterAttributes },
          { metric: firstHistogram, getAttributes: getFirstAttributes },
          { metric: secondHistogram, getAttributes: getSecondAttributes },
        ])
        run() {
          return execute();
        }
      }

      const execution = new Service().run();
      if (isAsync) {
        expect(counter.add).not.toHaveBeenCalled();
        expect(attributedCounter.add).not.toHaveBeenCalled();
        expect(firstHistogram.record).not.toHaveBeenCalled();
        expect(secondHistogram.record).not.toHaveBeenCalled();
        expect(getCounterAttributes).not.toHaveBeenCalled();
        expect(getFirstAttributes).not.toHaveBeenCalled();
        expect(getSecondAttributes).not.toHaveBeenCalled();
      } else {
        expect(execution).toBe(result);
      }
      expect(await execution).toBe(result);
      expect(execute).toHaveBeenCalledExactlyOnceWith();
      expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, undefined);
      expect(attributedCounter.add).toHaveBeenCalledExactlyOnceWith(1, {
        counterStatus: 'settled',
      });
      expect(firstHistogram.record).toHaveBeenCalledExactlyOnceWith(0.25, {
        firstStatus: 'settled',
      });
      expect(secondHistogram.record).toHaveBeenCalledExactlyOnceWith(0.25, {
        secondStatus: 'settled',
      });
      for (const callback of [
        getCounterAttributes,
        getFirstAttributes,
        getSecondAttributes,
      ]) {
        expect(callback).toHaveBeenCalledExactlyOnceWith(result);
        expect(callback.mock.calls[0]?.[0]).toBe(result);
      }
    },
  );

  it('records a thrown call with attributes derived from null and preserves the error', () => {
    const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;
    const histogram = {
      record: vi.fn<Histogram['record']>(),
    } satisfies Histogram;
    const secondHistogram = {
      record: vi.fn<Histogram['record']>(),
    } satisfies Histogram;
    const now = vi.spyOn(performance, 'now').mockReturnValue(100);
    histogram.record.mockImplementation(() => now.mockReturnValue(900));
    const getAttributes = vi.fn((result: { status: string } | null) => ({
      status: result?.status ?? 'failed',
    }));
    const failure = new Error('failed');

    class Service {
      @RecordMetrics([
        { metric: counter, getAttributes },
        { metric: histogram, getAttributes },
        { metric: secondHistogram, getAttributes },
      ])
      run(): never {
        now.mockReturnValue(350);
        throw failure;
      }
    }

    try {
      new Service().run();
      expect.fail('Expected the decorated method to throw');
    } catch (error) {
      expect(error).toBe(failure);
    }
    expect(histogram.record).toHaveBeenCalledExactlyOnceWith(0.25, {
      status: 'failed',
    });
    expect(secondHistogram.record).toHaveBeenCalledExactlyOnceWith(0.25, {
      status: 'failed',
    });
    expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, {
      status: 'failed',
    });
    expect(getAttributes.mock.calls).toEqual([[null], [null], [null]]);
  });

  it('records a rejected call with attributes derived from null and preserves the rejection', async () => {
    const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;
    const histogram = {
      record: vi.fn<Histogram['record']>(),
    } satisfies Histogram;
    const secondHistogram = {
      record: vi.fn<Histogram['record']>(),
    } satisfies Histogram;
    const now = vi.spyOn(performance, 'now').mockReturnValue(100);
    histogram.record.mockImplementation(() => now.mockReturnValue(900));
    const getAttributes = vi.fn((result: { status: string } | null) => ({
      status: result?.status ?? 'failed',
    }));
    const failure = new Error('failed');

    class Service {
      @RecordMetrics([
        { metric: counter, getAttributes },
        { metric: histogram, getAttributes },
        { metric: secondHistogram, getAttributes },
      ])
      async run(): Promise<never> {
        await Promise.resolve();
        now.mockReturnValue(350);
        throw failure;
      }
    }

    await expect(new Service().run()).rejects.toBe(failure);
    expect(histogram.record).toHaveBeenCalledExactlyOnceWith(0.25, {
      status: 'failed',
    });
    expect(secondHistogram.record).toHaveBeenCalledExactlyOnceWith(0.25, {
      status: 'failed',
    });
    expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, {
      status: 'failed',
    });
    expect(getAttributes.mock.calls).toEqual([[null], [null], [null]]);
  });

  it.each([
    [false, false],
    [false, true],
    [true, false],
    [true, true],
  ])(
    'counts failures without histograms for async=%s with attributes=%s',
    async (isAsync, withAttributes) => {
      const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;
      const getAttributes = vi.fn((result: { status: string } | null) => ({
        status: result?.status ?? 'failed',
      }));
      const failure = new Error('failed');
      const options = withAttributes
        ? [{ metric: counter, getAttributes }]
        : [{ metric: counter }];

      class Service {
        @RecordMetrics(options)
        run(): never | Promise<never> {
          if (isAsync) {
            return Promise.reject(failure);
          }
          throw failure;
        }
      }

      if (isAsync) {
        const execution = new Service().run();
        expect(counter.add).not.toHaveBeenCalled();
        await expect(execution).rejects.toBe(failure);
      } else {
        try {
          new Service().run();
          expect.fail('Expected the decorated method to throw');
        } catch (error) {
          expect(error).toBe(failure);
        }
      }
      expect(counter.add).toHaveBeenCalledExactlyOnceWith(
        1,
        withAttributes ? { status: 'failed' } : undefined,
      );
      if (withAttributes) {
        expect(getAttributes).toHaveBeenCalledExactlyOnceWith(null);
      } else {
        expect(getAttributes).not.toHaveBeenCalled();
      }
    },
  );

  it('supports a void return without an attributes callback', () => {
    const counter = { add: vi.fn<Counter['add']>() } satisfies Counter;

    class Service {
      @RecordMetrics([{ metric: counter }])
      run() {}
    }

    expect(new Service().run()).toBeUndefined();
    expect(counter.add).toHaveBeenCalledExactlyOnceWith(1, undefined);
  });

  it('accepts real OpenTelemetry instruments without a registered provider', () => {
    const meter = metrics.getMeter('metric-decorator-test');

    class Service {
      @RecordMetrics([
        { metric: meter.createCounter('calls') },
        { metric: meter.createHistogram('duration', { unit: 's' }) },
      ])
      run() {
        return 42;
      }
    }

    expect(new Service().run()).toBe(42);
  });
});
