import '@testing-library/jest-dom/vitest';

import { afterAll, afterEach, beforeAll } from 'vitest';

import { server } from '@/mocks/server';

/**
 * jsdom 下的 fetch 来自 undici，不接受相对 URL；而 API client 按契约用 `/api/v1/...`。
 * 在这里补上 origin，而不是让 client 为了测试环境改写 base path。
 */
const origin = 'http://localhost';
const originalFetch: typeof fetch = globalThis.fetch.bind(globalThis);
const fetchWithOrigin: typeof fetch = (input, init) => {
  if (typeof input === 'string' && input.startsWith('/')) {
    return originalFetch(new URL(input, origin).toString(), init);
  }
  return originalFetch(input, init);
};
globalThis.fetch = fetchWithOrigin;

// jsdom 不做布局。TanStack Virtual 用 offsetWidth/offsetHeight 量滚动容器，
// 再用 ResizeObserver（优先 borderBoxSize）跟进；行节点挂 data-index，
// measureElement 也会读 offsetHeight。尺寸为 0 时一行都不渲染，或被压成 0 后误触发续拉。
const SCROLLER_SIZE = { width: 1200, height: 800 };
const VIRTUAL_ROW_SIZE = { width: 1200, height: 25 };

function boxOf(el: Element): { width: number; height: number } | null {
  if (!(el instanceof HTMLElement)) return null;
  if (el.classList.contains('log-scroller')) return SCROLLER_SIZE;
  if (el.hasAttribute('data-index')) return VIRTUAL_ROW_SIZE;
  return null;
}

function rectFrom(size: { width: number; height: number }): DOMRect {
  return new DOMRect(0, 0, size.width, size.height);
}

const originalGetBoundingClientRect = Object.getOwnPropertyDescriptor(
  HTMLElement.prototype,
  'getBoundingClientRect',
)?.value as (this: HTMLElement) => DOMRect;
HTMLElement.prototype.getBoundingClientRect = function getBoundingClientRect(this: HTMLElement) {
  const box = boxOf(this);
  return box ? rectFrom(box) : originalGetBoundingClientRect.call(this);
};

function defineBoxGetter(
  prop: 'offsetWidth' | 'offsetHeight' | 'clientWidth' | 'clientHeight',
  axis: 'width' | 'height',
) {
  Object.defineProperty(HTMLElement.prototype, prop, {
    configurable: true,
    get(this: HTMLElement) {
      return boxOf(this)?.[axis] ?? 0;
    },
  });
}
defineBoxGetter('offsetWidth', 'width');
defineBoxGetter('offsetHeight', 'height');
defineBoxGetter('clientWidth', 'width');
defineBoxGetter('clientHeight', 'height');

class ImmediateResizeObserver implements ResizeObserver {
  readonly #callback: ResizeObserverCallback;

  constructor(callback: ResizeObserverCallback) {
    this.#callback = callback;
  }

  observe(target: Element) {
    const rect = target.getBoundingClientRect();
    const box: ResizeObserverSize = { inlineSize: rect.width, blockSize: rect.height };
    this.#callback(
      [
        {
          target,
          contentRect: rect,
          borderBoxSize: [box],
          contentBoxSize: [box],
          devicePixelContentBoxSize: [box],
        },
      ],
      this,
    );
  }

  unobserve() {}
  disconnect() {}
}

Object.defineProperty(globalThis, 'ResizeObserver', {
  writable: true,
  configurable: true,
  value: ImmediateResizeObserver,
});

if (!globalThis.crypto?.randomUUID) {
  Object.defineProperty(globalThis, 'crypto', {
    value: { ...globalThis.crypto, randomUUID: () => '00000000-0000-4000-8000-000000000000' },
  });
}

beforeAll(() => {
  server.listen({ onUnhandledRequest: 'error' });
});
afterEach(() => {
  server.resetHandlers();
});
afterAll(() => {
  server.close();
});
