import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { api } from '../api/endpoints'
import { ApiError } from '../api/http'
import type { NewTraveller, Profile, ProfileUpdate } from '../api/types'
import { useAuth } from '../auth/session'
import { Card, Empty, ErrorBox, Spinner } from '../components/ui'
import { formatTravelDate, railwayToday, titleCase } from '../lib/format'

export function ProfilePage() {
  const profile = useQuery({ queryKey: ['profile'], queryFn: api.profile })
  const auth = useAuth()
  // Lives here, not in the form: a save bumps the version, which remounts the form.
  const [savedVersion, setSavedVersion] = useState<number | null>(null)
  if (profile.isPending) return <Spinner label="Loading profile…" />
  if (profile.error) return <ErrorBox error={profile.error} onRetry={() => profile.refetch()} />
  const p = profile.data
  return (
    <div className="stack">
      <header className="page-head">
        <div>
          <h1>{p.displayName ?? auth.displayName}</h1>
          <p className="muted">{p.email}</p>
        </div>
        <div className="loyalty">
          {auth.loyaltyTier && <span className={`tier tier-${auth.loyaltyTier.toLowerCase()}`}>{titleCase(auth.loyaltyTier)}</span>}
          <span className="muted small">{p.loyaltyPoints.toLocaleString()} points{p.loyaltyNumber ? ` · ${p.loyaltyNumber}` : ''}</span>
        </div>
      </header>
      <div className="checkout-grid">
        {/* Keyed by version: a saved or re-read profile resets the form to the server's values. */}
        <PreferencesForm key={p.version} profile={p} justSaved={savedVersion === p.version} onSaved={setSavedVersion} />
        <Travellers />
      </div>
    </div>
  )
}

function PreferencesForm({ profile, justSaved, onSaved }: { profile: Profile; justSaved: boolean; onSaved: (version: number) => void }) {
  const queryClient = useQueryClient()
  const [form, setForm] = useState<ProfileUpdate>({
    version: profile.version,
    displayName: profile.displayName,
    phone: profile.phone,
    preferredFareClass: profile.preferredFareClass,
    seatPreference: profile.seatPreference,
    emailNotifications: profile.emailNotifications,
    smsNotifications: profile.smsNotifications,
  })
  const [dirty, setDirty] = useState(false)
  const save = useMutation({
    mutationFn: () => api.updateProfile({ ...form, phone: form.phone || null, displayName: form.displayName || null }),
    onSuccess: (p) => {
      onSaved(p.version)
      queryClient.setQueryData(['profile'], p)
    },
  })
  // The service rejects a write based on a stale version instead of silently overwriting it.
  const stale = save.error instanceof ApiError && save.error.code === 'STALE_PROFILE'
  const set = <K extends keyof ProfileUpdate>(k: K, v: ProfileUpdate[K]) => {
    setDirty(true)
    setForm((f) => ({ ...f, [k]: v }))
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    save.mutate()
  }

  return (
    <Card>
      <h2>Preferences</h2>
      <form className="form" onSubmit={submit}>
        <label><span>Display name</span>
          <input value={form.displayName ?? ''} maxLength={200} onChange={(e) => set('displayName', e.target.value)} />
        </label>
        <label><span>Phone</span>
          <input value={form.phone ?? ''} pattern="\+?[0-9 ]{6,20}" placeholder="+971 50 123 4567"
            onChange={(e) => set('phone', e.target.value)} autoComplete="tel" />
        </label>
        <div className="two-col">
          <label><span>Preferred class</span>
            <select value={form.preferredFareClass ?? ''}
              onChange={(e) => set('preferredFareClass', (e.target.value || null) as ProfileUpdate['preferredFareClass'])}>
              <option value="">No preference</option>
              <option value="ECONOMY">Economy</option>
              <option value="BUSINESS">Business</option>
            </select>
          </label>
          <label><span>Seat</span>
            <select value={form.seatPreference} onChange={(e) => set('seatPreference', e.target.value as ProfileUpdate['seatPreference'])}>
              <option value="NO_PREFERENCE">No preference</option>
              <option value="WINDOW">Window</option>
              <option value="AISLE">Aisle</option>
            </select>
          </label>
        </div>
        <label className="check">
          <input type="checkbox" checked={form.emailNotifications} onChange={(e) => set('emailNotifications', e.target.checked)} />
          <span>Email me tickets and trip updates</span>
        </label>
        <label className="check">
          <input type="checkbox" checked={form.smsNotifications} onChange={(e) => set('smsNotifications', e.target.checked)} />
          <span>Text me trip updates</span>
        </label>
        {stale ? (
          <div className="alert alert-warn">
            Your profile changed somewhere else.{' '}
            <button type="button" className="btn btn-small" onClick={() => queryClient.invalidateQueries({ queryKey: ['profile'] })}>
              Load latest
            </button>
          </div>
        ) : save.error && <ErrorBox error={save.error} />}
        {justSaved && !dirty && <div className="alert alert-ok">Saved.</div>}
        <button className="btn btn-primary" disabled={save.isPending}>{save.isPending ? 'Saving…' : 'Save preferences'}</button>
      </form>
    </Card>
  )
}

const EMPTY_TRAVELLER: NewTraveller = { fullName: '', dateOfBirth: '', documentType: '', documentNumber: '' }

function Travellers() {
  const queryClient = useQueryClient()
  const travellers = useQuery({ queryKey: ['travellers'], queryFn: api.travellers })
  const [draft, setDraft] = useState<NewTraveller>(EMPTY_TRAVELLER)
  const [adding, setAdding] = useState(false)
  const [today] = useState(railwayToday)
  const add = useMutation({
    // Optional fields go as null, not "", so the service's patterns only judge real values.
    mutationFn: () => api.addTraveller({
      fullName: draft.fullName.trim(),
      dateOfBirth: draft.dateOfBirth || undefined,
      documentType: draft.documentType || undefined,
      documentNumber: draft.documentNumber || undefined,
    }),
    onSuccess: () => {
      setDraft(EMPTY_TRAVELLER)
      setAdding(false)
      void queryClient.invalidateQueries({ queryKey: ['travellers'] })
    },
  })
  const remove = useMutation({
    mutationFn: api.removeTraveller,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['travellers'] }),
  })
  const submit = (e: FormEvent) => {
    e.preventDefault()
    add.mutate()
  }

  return (
    <Card>
      <div className="row-between">
        <h2>Saved travellers</h2>
        {!adding && <button className="btn btn-small" onClick={() => setAdding(true)}>Add</button>}
      </div>
      {travellers.isPending && <Spinner />}
      {travellers.error && <ErrorBox error={travellers.error} />}
      {travellers.data?.length === 0 && !adding && (
        <Empty title="No saved travellers"><p className="muted small">Save family and colleagues to book for them faster.</p></Empty>
      )}
      <ul className="travellers">
        {travellers.data?.map((t) => (
          <li key={t.id}>
            <div>
              <strong>{t.fullName}</strong>
              <div className="muted small">
                {[t.dateOfBirth && `Born ${formatTravelDate(t.dateOfBirth)}`,
                  t.documentType && `${titleCase(t.documentType)} ${t.documentNumberMasked ?? ''}`].filter(Boolean).join(' · ')}
              </div>
            </div>
            <button className="btn btn-link" disabled={remove.isPending} onClick={() => remove.mutate(t.id)}
              aria-label={`Remove ${t.fullName}`}>Remove</button>
          </li>
        ))}
      </ul>
      {remove.error && <ErrorBox error={remove.error} />}
      {adding && (
        <form className="form" onSubmit={submit}>
          <label><span>Full name</span>
            <input value={draft.fullName} required maxLength={200} onChange={(e) => setDraft({ ...draft, fullName: e.target.value })} />
          </label>
          <label><span>Date of birth</span>
            <input type="date" value={draft.dateOfBirth} max={today}
              onChange={(e) => setDraft({ ...draft, dateOfBirth: e.target.value })} />
          </label>
          <div className="two-col">
            <label><span>Document</span>
              <select value={draft.documentType} onChange={(e) => setDraft({ ...draft, documentType: e.target.value })}>
                <option value="">None</option>
                <option value="PASSPORT">Passport</option>
                <option value="EMIRATES_ID">Emirates ID</option>
                <option value="NATIONAL_ID">National ID</option>
              </select>
            </label>
            <label><span>Number</span>
              <input value={draft.documentNumber} pattern="[A-Za-z0-9\-]{4,30}" disabled={!draft.documentType}
                required={!!draft.documentType} onChange={(e) => setDraft({ ...draft, documentNumber: e.target.value })} />
            </label>
          </div>
          <p className="muted small">Document numbers are stored securely and only ever shown masked.</p>
          {add.error && <ErrorBox error={add.error} />}
          <div className="row">
            <button className="btn btn-primary" disabled={add.isPending}>{add.isPending ? 'Saving…' : 'Save traveller'}</button>
            <button type="button" className="btn" onClick={() => setAdding(false)}>Cancel</button>
          </div>
        </form>
      )}
    </Card>
  )
}
