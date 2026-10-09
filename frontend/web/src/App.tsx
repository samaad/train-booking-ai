import { NavLink, Outlet, Route, Routes, Link } from 'react-router'
import { useAuth } from './auth/session'
import { RequireAuth } from './components/RequireAuth'
import { AssistantPage } from './pages/AssistantPage'
import { CallbackPage } from './pages/CallbackPage'
import { CheckoutPage } from './pages/CheckoutPage'
import { PayPage } from './pages/PayPage'
import { ProfilePage } from './pages/ProfilePage'
import { SearchPage } from './pages/SearchPage'
import { SeatsPage } from './pages/SeatsPage'
import { TripDetailPage } from './pages/TripDetailPage'
import { TripsPage } from './pages/TripsPage'
import { Empty } from './components/ui'

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<SearchPage />} />
        <Route path="trains/:train/:date" element={<SeatsPage />} />
        <Route path="callback" element={<CallbackPage />} />
        <Route element={<RequireAuth><Outlet /></RequireAuth>}>
          <Route path="checkout/:holdId" element={<CheckoutPage />} />
          <Route path="trips" element={<TripsPage />} />
          <Route path="trips/:bookingId" element={<TripDetailPage />} />
          <Route path="trips/:bookingId/pay" element={<PayPage />} />
          <Route path="assistant/:conversationId?" element={<AssistantPage />} />
          <Route path="profile" element={<ProfilePage />} />
        </Route>
        <Route path="*" element={<Empty title="Page not found"><Link to="/">Back to search</Link></Empty>} />
      </Route>
    </Routes>
  )
}

function Layout() {
  const { user, login, logout, displayName, loyaltyTier, isStaff } = useAuth()
  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar-inner">
          <Link to="/" className="brand">
            <img src="/favicon.svg" alt="" width={28} height={28} />
            <span>Rail</span>
          </Link>
          <nav className="nav">
            <NavLink to="/" end>Search</NavLink>
            <NavLink to="/trips">Trips</NavLink>
            <NavLink to="/assistant">Assistant</NavLink>
          </nav>
          <div className="user">
            {user ? (
              <>
                <NavLink to="/profile" className="user-name">
                  {displayName}
                  {loyaltyTier && <span className={`tier tier-${loyaltyTier.toLowerCase()}`}>{loyaltyTier}</span>}
                  {isStaff && <span className="badge badge-neutral">Staff</span>}
                </NavLink>
                <button className="btn btn-small" onClick={() => void logout()}>Sign out</button>
              </>
            ) : (
              <button className="btn btn-small btn-primary" onClick={() => void login()}>Sign in</button>
            )}
          </div>
        </div>
      </header>
      <main className="main">
        <Outlet />
      </main>
    </div>
  )
}
