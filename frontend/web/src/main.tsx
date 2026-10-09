import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router'
import { ApiError } from './api/http'
import { App } from './App'
import { AuthProvider } from './auth/AuthProvider'
import './index.css'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      refetchOnWindowFocus: false,
      // Reads are safe to retry, but not a 4xx: a 404 stays a 404.
      retry: (failures, error) => failures < 2 && !(error instanceof ApiError && error.status >= 400 && error.status < 500),
    },
    // Mutations are never retried automatically: a retry is a deliberate user action that replays the
    // same Idempotency-Key (06-resilience — no blind retries on seat holds or payments).
    mutations: { retry: false },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </AuthProvider>
    </QueryClientProvider>
  </StrictMode>,
)
