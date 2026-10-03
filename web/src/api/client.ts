import { ApiRequestError } from './errors';
import type {
  CreateExportRequest,
  GetLogRowResponse,
  ListDimensionsRequest,
  ListDimensionsResponse,
  ListFieldsRequest,
  ListFieldsResponse,
  LogContextRequest,
  LogContextResponse,
  LogFacetRequest,
  LogFacetResponse,
  LogHistogramRequest,
  LogHistogramResponse,
  SearchLogsRequest,
  SearchLogsResponse,
} from './types';

export const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? '/api/v1').replace(/\/$/, '');

interface RequestOptions {
  signal?: AbortSignal;
}

function newClientQueryId(): string {
  return crypto.randomUUID();
}

/**
 * IF-007 §2.5：客户端在 X-Client-Query-Id 里自带 uuid，才能 DELETE /queries/{id} 取消。
 * 不带这个头的查询不可取消，而用户改条件重查的频率很高 —— 所以这里是无条件带上，
 * 不做成调用方的可选项。
 */
async function request<TResponse>(
  path: string,
  init: { method: 'GET' | 'POST' | 'DELETE'; body?: unknown },
  options: RequestOptions = {},
): Promise<TResponse> {
  const clientQueryId = newClientQueryId();
  const headers: Record<string, string> = { 'X-Client-Query-Id': clientQueryId };
  if (init.body !== undefined) headers['Content-Type'] = 'application/json';

  const abortHandler = () => void cancelQuery(clientQueryId);
  options.signal?.addEventListener('abort', abortHandler, { once: true });

  let response: Response;
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      method: init.method,
      headers,
      ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
      ...(options.signal ? { signal: options.signal } : {}),
    });
  } finally {
    options.signal?.removeEventListener('abort', abortHandler);
  }

  if (!response.ok) {
    throw await toApiError(response);
  }
  if (response.status === 204) {
    return undefined as TResponse;
  }
  return (await response.json()) as TResponse;
}

async function toApiError(response: Response): Promise<ApiRequestError> {
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    payload = undefined;
  }
  const error = (payload as { error?: unknown } | undefined)?.error;
  if (error && typeof error === 'object' && 'code' in error && 'message' in error) {
    return new ApiRequestError(error as ApiRequestError['payload'], response.status);
  }
  // 非契约形状的失败（网关 5xx、反代 HTML 错误页）统一归一成 INTERNAL，
  // 这样 UI 只需处理一种错误形状。
  return new ApiRequestError(
    {
      code: 'INTERNAL',
      message: `HTTP ${String(response.status)} ${response.statusText}`,
      retryable: response.status >= 500,
      suggested_action: 'RETRY_AFTER',
    },
    response.status,
  );
}

/** 取消后服务端释放该租户的并发配额槽位（IF-007 §2.5）。失败静默：取消本身不该打扰用户。 */
async function cancelQuery(clientQueryId: string): Promise<void> {
  try {
    await fetch(`${API_BASE_URL}/queries/${clientQueryId}`, { method: 'DELETE', keepalive: true });
  } catch {
    /* noop */
  }
}

export const if7 = {
  listDimensions: (body: ListDimensionsRequest, options?: RequestOptions) =>
    request<ListDimensionsResponse>('/dimensions', { method: 'POST', body }, options),

  listFields: (body: ListFieldsRequest, options?: RequestOptions) =>
    request<ListFieldsResponse>('/fields', { method: 'POST', body }, options),

  searchLogs: (body: SearchLogsRequest, options?: RequestOptions) =>
    request<SearchLogsResponse>('/logs/search', { method: 'POST', body }, options),

  logHistogram: (body: LogHistogramRequest, options?: RequestOptions) =>
    request<LogHistogramResponse>('/logs/histogram', { method: 'POST', body }, options),

  logFacet: (body: LogFacetRequest, options?: RequestOptions) =>
    request<LogFacetResponse>('/logs/facet', { method: 'POST', body }, options),

  getLogRow: (rowRef: string, options?: RequestOptions) =>
    request<GetLogRowResponse>(
      `/logs/row/${encodeURIComponent(rowRef)}`,
      { method: 'GET' },
      options,
    ),

  logContext: (body: LogContextRequest, options?: RequestOptions) =>
    request<LogContextResponse>('/logs/context', { method: 'POST', body }, options),

  /** SCAN_LIMIT_EXCEEDED 的降级落点：suggested_action=USE_ASYNC_EXPORT 指向这里。 */
  createExport: (body: CreateExportRequest, options?: RequestOptions) =>
    request<{ task_id: string }>('/exports', { method: 'POST', body }, options),
};
