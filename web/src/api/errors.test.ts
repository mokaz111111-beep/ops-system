import { describe, expect, it } from 'vitest';

import { ApiRequestError, describeApiError } from './errors';

describe('结构化错误的可操作渲染（DD-008 §3.9.3）', () => {
  it('把 suggested_action 枚举翻译成按钮文案，而不是丢一段红字', () => {
    const err = new ApiRequestError(
      {
        code: 'SCAN_LIMIT_EXCEEDED',
        message: '查询扫描量超过单次上限',
        retryable: false,
        suggested_action: 'USE_ASYNC_EXPORT',
        details: { scanned_rows: 523_000_000, limit: 200_000_000 },
        query_id: 'q-01HQ',
      },
      422,
    );
    expect(describeApiError(err)).toMatchObject({
      title: '查询扫描量超过单次上限',
      action: 'USE_ASYNC_EXPORT',
      actionLabel: '改走异步导出',
      queryId: 'q-01HQ',
    });
  });

  it('配额类错误透出退避秒数', () => {
    const err = new ApiRequestError(
      {
        code: 'QUOTA_EXCEEDED',
        message: '租户查询配额耗尽',
        retryable: true,
        suggested_action: 'RETRY_AFTER',
        retry_after_seconds: 30,
      },
      429,
    );
    expect(describeApiError(err).retryAfterSeconds).toBe(30);
    expect(err.retryable).toBe(true);
  });

  it('非契约形状的异常也能被统一描述', () => {
    expect(describeApiError(new Error('boom'))).toEqual({ title: '请求失败', message: 'boom' });
  });
});
