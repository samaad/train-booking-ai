/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  // Keycloak's train-web client only allows redirects to http://localhost:5173/*, and Kong's CORS
  // and the seat-map WebSocket only accept that origin — never let Vite drift to another port.
  server: { port: 5173, strictPort: true },
  preview: { port: 5173, strictPort: true },
  test: {
    environment: 'jsdom',
    setupFiles: ['src/test/setup.ts'],
  },
})
