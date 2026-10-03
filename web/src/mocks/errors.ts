import { HttpResponse } from 'msw';

import type { ApiErrorCode, ApiErrorPayload, SuggestedAction } from '@/api/types';

/** IF-007 §3 错误码表：码 → HTTP 状态 / 可重试 / 建议动作，mock 必须如实照抄。 */
const ERROR_TABLE: Record<
  ApiErrorCode,
  { status: number; retryable: boolean; action?: SuggestedAction }
> = {
  INVALID_PARAM: { status: 400, retryable: false, action: 'FIX_REQUEST' },
  TIME_RANGE_REQUIRED: { status: 400, retryable: false, action: 'FIX_REQUEST' },
  TIME_RANGE_TOO_LARGE: { status: 400, retryable: false, action: 'NARROW_TIME_RANGE' },
  SELECT_PATHS_REQUIRED: { status: 400, retryable: false, action: 'FIX_REQUEST' },
  TOO_MANY_SELECT_PATHS: { status: 400, retryable: false, action: 'REDUCE_SELECT_PATHS' },
  FIELD_PATH_NOT_FOUND: { status: 400, retryable: false, action: 'FIX_REQUEST' },
  QUERY_SYNTAX_ERROR: { status: 400, retryable: false, action: 'FIX_REQUEST' },
  UNSUPPORTED_SQL: { status: 400, retryable: false, action: 'FIX_REQUEST' },
  CURSOR_EXPIRED: { status: 400, retryable: false, action: 'RESTART_QUERY' },
  SCAN_LIMIT_EXCEEDED: { status: 422, retryable: false, action: 'USE_ASYNC_EXPORT' },
  QUOTA_EXCEEDED: { status: 429, retryable: true, action: 'RETRY_AFTER' },
  CONCURRENCY_QUEUE_TIMEOUT: { status: 429, retryable: true, action: 'RETRY_AFTER' },
  QUERY_TIMEOUT: { status: 504, retryable: true, action: 'NARROW_TIME_RANGE' },
  QUERY_CANCELLED: { status: 499, retryable: false },
  INTERNAL: { status: 500, retryable: true, action: 'RETRY_AFTER' },
};

export class MockApiError extends Error {
  readonly payload: ApiErrorPayload;
  readonly status: number;

  constructor(
    code: ApiErrorCode,
    message: string,
    extras: { details?: Record<string, unknown>; retryAfterSeconds?: number; queryId?: string } = {},
  ) {
    super(message);
    const spec = ERROR_TABLE[code];
    this.status = spec.status;
    this.payload = {
      code,
      message,
      retryable: spec.retryable,
      ...(spec.action ? { suggested_action: spec.action } : {}),
      ...(extras.details ? { details: extras.details } : {}),
      ...(extras.retryAfterSeconds !== undefined
        ? { retry_after_seconds: extras.retryAfterSeconds }
        : {}),
      ...(extras.queryId ? { query_id: extras.queryId } : {}),
    };
  }
}

export function errorResponse(err: unknown): Response {
  if (err instanceof MockApiError) {
    return HttpResponse.json({ error: err.payload }, { status: err.status });
  }
  const payload: ApiErrorPayload = {
    code: 'INTERNAL',
    message: err instanceof Error ? err.message : String(err),
    retryable: true,
    suggested_action: 'RETRY_AFTER',
  };
  return HttpResponse.json({ error: payload }, { status: 500 });
}

export function newQueryId(): string {
  const alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  let out = '';
  for (let n = 0; n < 26; n += 1) {
    out += alphabet[Math.floor(Math.random() * alphabet.length)];
  }
  return `q-${out}`;
}
