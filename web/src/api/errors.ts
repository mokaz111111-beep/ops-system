import type { ApiErrorCode, ApiErrorPayload, SuggestedAction } from './types';

/**
 * IF-007 §2.6 的结构化错误。
 *
 * 之所以要把它变成一个带 payload 的 Error 子类而不是直接抛 message：
 * `suggested_action` 是枚举而非文案，UI 要据此渲染成可点的按钮（§3 错误码表）。
 */
export class ApiRequestError extends Error {
  readonly payload: ApiErrorPayload;
  readonly httpStatus: number;

  constructor(payload: ApiErrorPayload, httpStatus: number) {
    super(payload.message);
    this.name = 'ApiRequestError';
    this.payload = payload;
    this.httpStatus = httpStatus;
  }

  get code(): ApiErrorCode {
    return this.payload.code;
  }

  get suggestedAction(): SuggestedAction | undefined {
    return this.payload.suggested_action;
  }

  get retryable(): boolean {
    return this.payload.retryable;
  }
}

export function isApiRequestError(err: unknown): err is ApiRequestError {
  return err instanceof ApiRequestError;
}

/** 错误码 → 面向用户的标题。文案与 IF-007 §3 的触发条件一一对应。 */
export const ERROR_CODE_TITLE: Record<ApiErrorCode, string> = {
  INVALID_PARAM: '请求参数有误',
  TIME_RANGE_REQUIRED: '缺少时间窗',
  TIME_RANGE_TOO_LARGE: '时间窗超出该接口上限',
  SELECT_PATHS_REQUIRED: '未声明要返回的字段路径',
  TOO_MANY_SELECT_PATHS: '投影字段过多',
  FIELD_PATH_NOT_FOUND: '字段路径在当前时间窗内没有数据',
  QUERY_SYNTAX_ERROR: '查询语句无法解析',
  UNSUPPORTED_SQL: 'SQL 超出允许的子集',
  CURSOR_EXPIRED: '翻页游标已过期',
  SCAN_LIMIT_EXCEEDED: '查询扫描量超过单次上限',
  QUOTA_EXCEEDED: '租户查询配额已耗尽',
  CONCURRENCY_QUEUE_TIMEOUT: '查询排队超时',
  QUERY_TIMEOUT: '查询执行超时',
  QUERY_CANCELLED: '查询已取消',
  INTERNAL: '服务内部错误',
};

/** suggested_action → 按钮文案。UI 把拒绝渲染成动作，而不是一段红字（DD-008 §3.9.3）。 */
export const SUGGESTED_ACTION_LABEL: Record<SuggestedAction, string> = {
  NARROW_TIME_RANGE: '缩小时间窗',
  REDUCE_SELECT_PATHS: '减少投影字段',
  USE_ASYNC_EXPORT: '改走异步导出',
  RETRY_AFTER: '稍后重试',
  RESTART_QUERY: '重新发起查询',
  FIX_REQUEST: '修正请求条件',
  CONTACT_SUPPORT: '联系支持',
};

export function describeApiError(err: unknown): {
  title: string;
  message: string;
  action?: SuggestedAction;
  actionLabel?: string;
  queryId?: string;
  retryAfterSeconds?: number;
} {
  if (!isApiRequestError(err)) {
    return {
      title: '请求失败',
      message: err instanceof Error ? err.message : String(err),
    };
  }
  const action = err.suggestedAction;
  return {
    title: ERROR_CODE_TITLE[err.code],
    message: err.payload.message,
    ...(action ? { action, actionLabel: SUGGESTED_ACTION_LABEL[action] } : {}),
    ...(err.payload.query_id ? { queryId: err.payload.query_id } : {}),
    ...(typeof err.payload.retry_after_seconds === 'number'
      ? { retryAfterSeconds: err.payload.retry_after_seconds }
      : {}),
  };
}
