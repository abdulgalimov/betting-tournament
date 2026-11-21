import { describe, expect, it } from 'vitest';
import {
  calculatePoints,
  canonicalDecimal,
  toMicroUnits,
} from '../src/tournament';

describe('exact decimal points', () => {
  it('preserves fractions without binary floating-point error', () => {
    expect(calculatePoints('0.1', '0.2')).toBe('0.02');
    expect(calculatePoints('0.000001', '0.000001')).toBe('0.000000000001');
  });

  it('supports amounts above Number.MAX_SAFE_INTEGER', () => {
    expect(calculatePoints('999999999999999999.999999', '1.123456')).toBe(
      '1123455999999999999.999998876544',
    );
  });

  it('keeps zero and whole numbers canonical', () => {
    expect(calculatePoints('0', '1.1')).toBe('0');
    expect(calculatePoints('10.000000', '1.100000')).toBe('11');
    expect(canonicalDecimal('12.340000')).toBe('12.34');
  });

  it.each(['-1', '1e3', 'NaN', '01', '1.0000001', '1000000000000000000', ''])(
    'rejects values outside the supported decimal contract: %s',
    (value) => {
      expect(() => toMicroUnits(value)).toThrow('Invalid decimal');
    },
  );

  it('requires positive exchange rates', () => {
    expect(() => calculatePoints('1', '0')).toThrow('positive');
  });
});
