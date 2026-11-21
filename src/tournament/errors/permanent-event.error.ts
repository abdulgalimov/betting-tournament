/** A malformed/conflicting business event cannot succeed on redelivery. */
export class PermanentEventError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentEventError';
  }
}
