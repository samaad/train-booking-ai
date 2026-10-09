/** A train as the assistant's searchTrains tool or the tier-3 fallback returns it. */
export interface TrainOptionLike {
  trainNumber: string
  trainName: string
  from: string
  to: string
  departure: string
  arrival: string
  durationMinutes: number
  fareClasses: string[]
}

export function isTrainOptions(value: unknown): value is TrainOptionLike[] {
  return Array.isArray(value) && value.length > 0
    && value.every((v) => v && typeof v === 'object' && 'trainNumber' in v && 'departure' in v)
}
