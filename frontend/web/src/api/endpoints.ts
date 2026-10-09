import { request, rawRequest } from './http'
import type {
  AvailabilityResponse, Booking, ConversationSummary, Hold, JourneyOption, MessageView, NewTraveller,
  Payment, Profile, ProfileUpdate, Station, Ticket, Traveller,
} from './types'

const q = (params: Record<string, string>) => new URLSearchParams(params).toString()

export const api = {
  // catalog — public
  stations: () => request<Station[]>('/api/catalog/stations', { auth: 'optional' }),
  search: (from: string, to: string, date: string) =>
    request<JourneyOption[]>(`/api/catalog/routes/search?${q({ from, to, date })}`, { auth: 'optional' }),

  // inventory
  availability: (trainNumber: string, travelDate: string) =>
    request<AvailabilityResponse>(`/api/inventory/availability?${q({ trainNumber, travelDate })}`, { auth: 'optional' }),
  hold: (body: { trainNumber: string; travelDate: string; fareClass: string; seatNo?: string }, idempotencyKey: string) =>
    request<Hold>('/api/inventory/holds', { method: 'POST', body, idempotencyKey }),
  getHold: (holdId: string) => request<Hold>(`/api/inventory/holds/${holdId}`),
  releaseHold: (holdId: string) => request<void>(`/api/inventory/holds/${holdId}`, { method: 'DELETE' }),

  // booking
  myBookings: () => request<Booking[]>('/api/booking/bookings'),
  booking: (id: string) => request<Booking>(`/api/booking/bookings/${id}`),
  createBooking: (
    body: { holdId: string; fromStation: string; toStation: string; passengerName: string; contactEmail: string },
    idempotencyKey: string,
  ) => request<Booking>('/api/booking/bookings', { method: 'POST', body, idempotencyKey }),
  cancelBooking: (id: string, reason?: string) =>
    request<Booking>(`/api/booking/bookings/${id}/cancel`, { method: 'POST', body: { reason: reason || null } }),

  // payment
  pay: (body: { bookingId: string; amount: number; currency: string; paymentToken: string }, idempotencyKey: string) =>
    request<Payment>('/api/payment/payments', { method: 'POST', body, idempotencyKey }),

  // ticketing
  ticket: (pnr: string) => request<Ticket>(`/api/ticketing/tickets/${pnr}`),
  ticketQr: async (pnr: string) => (await rawRequest(`/api/ticketing/tickets/${pnr}/qr`, { accept: 'image/png' })).blob(),
  ticketPdf: async (pnr: string) => (await rawRequest(`/api/ticketing/tickets/${pnr}/pdf`, { accept: 'application/pdf' })).blob(),

  // profile
  profile: () => request<Profile>('/api/profile/me'),
  updateProfile: (body: ProfileUpdate) => request<Profile>('/api/profile/me', { method: 'PUT', body }),
  travellers: () => request<Traveller[]>('/api/profile/me/travellers'),
  addTraveller: (body: NewTraveller) => request<Traveller>('/api/profile/me/travellers', { method: 'POST', body }),
  removeTraveller: (id: string) => request<void>(`/api/profile/me/travellers/${id}`, { method: 'DELETE' }),

  // assistant
  conversations: () => request<ConversationSummary[]>('/api/assistant/conversations'),
  messages: (id: string) => request<MessageView[]>(`/api/assistant/conversations/${id}/messages`),
}
