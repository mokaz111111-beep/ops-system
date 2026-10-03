import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from './App';

async function bootstrap() {
  if (import.meta.env.VITE_ENABLE_MOCK === 'true') {
    const { startMockWorker } = await import('./mocks/browser');
    await startMockWorker();
  }
  const container = document.getElementById('root');
  if (!container) throw new Error('#root 不存在');
  createRoot(container).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
}

void bootstrap();
