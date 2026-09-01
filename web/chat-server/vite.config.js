import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const backend = 'http://localhost:8080';
const httpRoutes = [
  '/register', '/login', '/user', '/session', '/message', '/group',
  '/contact', '/api', '/mcp', '/livez', '/healthz',
];

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      ...Object.fromEntries(httpRoutes.map((route) => [route, { target: backend }])),
      '/wss': { target: backend, ws: true },
    },
  },
});
