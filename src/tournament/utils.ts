const SCALE = 6;
const UNIT = 10n ** BigInt(SCALE);
const DECIMAL = /^(0|[1-9]\d{0,17})(?:\.(\d{1,6}))?$/;

/** Convert a decimal string to micro-units without passing through Number. */
export function toMicroUnits(value: string): bigint {
  const match = DECIMAL.exec(value);
  if (!match) throw new Error(`Invalid decimal: ${value}`);
  return BigInt(match[1]!) * UNIT + BigInt((match[2] ?? '').padEnd(SCALE, '0'));
}

/** Format an integer containing 12 decimal places as a canonical decimal string. */
export function formatPoints(value: bigint): string {
  const negative = value < 0n;
  const digits = (negative ? -value : value).toString().padStart(13, '0');
  const fraction = digits.slice(-12).replace(/0+$/, '');
  return `${negative ? '-' : ''}${digits.slice(0, -12)}${fraction ? `.${fraction}` : ''}`;
}

export function calculatePoints(winAmount: string, rate: string): string {
  const exchangeRate = toMicroUnits(rate);
  if (exchangeRate <= 0n) throw new Error('Exchange rate must be positive');
  return formatPoints(toMicroUnits(winAmount) * exchangeRate);
}

/** PostgreSQL numeric values are returned as strings to preserve precision. */
export function canonicalDecimal(value: string): string {
  const [whole, fraction = ''] = value.split('.');
  const trimmed = fraction.replace(/0+$/, '');
  return `${whole}${trimmed ? `.${trimmed}` : ''}`;
}
