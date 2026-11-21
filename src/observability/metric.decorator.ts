import 'reflect-metadata';
import type { Attributes, Counter, Histogram } from '@opentelemetry/api';

export interface MetricOptions<Result = unknown> {
  metric: Counter | Histogram;
  getAttributes?: (result: Result | null) => Attributes;
}

function isPromiseLike<Result>(
  value: Result | PromiseLike<Result>,
): value is PromiseLike<Result> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    'then' in value &&
    typeof value.then === 'function'
  );
}

/**
 * Increments counters and records duration in seconds on success and failure.
 * Attributes receive the resolved result on success and null on failure.
 */
export function RecordMetrics<Result = unknown>(
  metrics: readonly MetricOptions<Result>[],
) {
  const hasHistograms = metrics.some(({ metric }) => 'record' in metric);

  return function <
    This,
    Args extends unknown[],
    Return extends Result | PromiseLike<Result>,
  >(
    _target: object,
    _propertyKey: string | symbol,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Return>,
  ): void {
    const original = descriptor.value;
    if (!original) {
      throw new TypeError('RecordMetric can only decorate methods');
    }

    const wrapped = function (this: This, ...args: Args): Return {
      const startedAt = hasHistograms ? performance.now() : 0;

      const record = (result: Result | null): void => {
        const duration = hasHistograms
          ? (performance.now() - startedAt) / 1000
          : 0;
        for (const { metric, getAttributes } of metrics) {
          const attributes = getAttributes?.(result);
          if ('add' in metric) {
            metric.add(1, attributes);
          } else {
            metric.record(duration, attributes);
          }
        }
      };

      let result: Result | PromiseLike<Result>;
      try {
        result = original.apply(this, args);
      } catch (error) {
        record(null);
        throw error;
      }

      if (isPromiseLike<Result>(result)) {
        return Promise.resolve(result).then(
          (value) => {
            record(value);
            return value;
          },
          (error: unknown) => {
            record(null);
            throw error;
          },
        ) as Return;
      }

      record(result);
      return result as Return;
    };

    for (const key of Reflect.getOwnMetadataKeys(original)) {
      Reflect.defineMetadata(
        key,
        Reflect.getOwnMetadata(key, original),
        wrapped,
      );
    }
    descriptor.value = wrapped;
  };
}
