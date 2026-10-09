// Mirrors of the services' response records. Times are ISO strings (UTC after Jackson); dates are yyyy-MM-dd.

export interface Station {
  code: string
  name: string
  city: string
}

export interface JourneyOption {
  trainNumber: string
  trainName: string
  from: string
  to: string
  departure: string
  arrival: string
  durationMinutes: number
  distanceKm: number
  fareClasses: string[]
}

export interface ClassAvailability {
  fareClass: string
  total: number
  available: number
  availableSeats: string[]
}

export interface AvailabilityResponse {
  trainNumber: string
  travelDate: string
  classes: ClassAvailability[]
}

export type HoldStatus = 'ACTIVE' | 'COMMITTED' | 'RELEASED' | 'EXPIRED'

export interface Hold {
  holdId: string
  trainNumber: string
  travelDate: string
  fareClass: string
  seatNo: string
  status: HoldStatus | string
  expiresAt: string
}

export type BookingStatus = 'PENDING_PAYMENT' | 'CONFIRMED' | 'CANCELLED'
export type SagaState = 'PAYMENT_PENDING' | 'SEATS_COMMITTED' | 'TICKETED' | 'CANCELLED'

export interface Booking {
  bookingId: string
  pnr: string
  status: BookingStatus
  sagaState: SagaState
  holdId: string
  trainNumber: string
  travelDate: string
  fareClass: string
  seatNo: string
  fromStation: string
  toStation: string
  departure: string
  arrival: string
  passengerName: string
  amount: number
  currency: string
  cancelReason: string | null
  createdAt: string
}

export type PaymentStatus = 'PENDING' | 'AUTHORIZED' | 'FAILED' | 'REFUNDED'

export interface Payment {
  paymentId: string
  bookingId: string
  amount: number
  currency: string
  status: PaymentStatus
  pspReference: string | null
  failureReason: string | null
  createdAt: string
}

export interface Ticket {
  pnr: string
  status: 'ISSUED' | 'VOIDED'
  passengerName: string
  trainNumber: string
  travelDate: string
  fareClass: string
  seatNo: string
  fromStation: string
  toStation: string
  departure: string
  qrToken: string | null
  issuedAt: string
  voidedAt: string | null
}

export type SeatPreference = 'WINDOW' | 'AISLE' | 'NO_PREFERENCE'

export interface Profile {
  userId: string
  displayName: string | null
  email: string | null
  phone: string | null
  preferredFareClass: 'BUSINESS' | 'ECONOMY' | null
  seatPreference: SeatPreference
  emailNotifications: boolean
  smsNotifications: boolean
  loyaltyNumber: string | null
  loyaltyPoints: number
  version: number
}

export type ProfileUpdate = Omit<Profile, 'userId' | 'email' | 'loyaltyNumber' | 'loyaltyPoints'>

export interface Traveller {
  id: string
  fullName: string
  dateOfBirth: string | null
  documentType: 'PASSPORT' | 'EMIRATES_ID' | 'NATIONAL_ID' | null
  documentNumberMasked: string | null
}

export interface NewTraveller {
  fullName: string
  dateOfBirth?: string
  documentType?: string
  documentNumber?: string
}

/** seat-availability-streams: only non-free seats are listed. */
export interface Occupancy {
  trainNumber: string
  travelDate: string
  classes: { fareClass: string; held: number; committed: number }[]
  seats: Record<string, 'HELD' | 'COMMITTED'>
  updatedAt: string | null
}

export interface ConversationSummary {
  conversationId: string
  title: string | null
  updatedAt: string
}

export interface MessageView {
  seq: number
  role: string
  content: string
  toolCalls: string[] | null
  createdAt: string
}
