import { describeApiError } from '@/api/errors';
import type { SuggestedAction } from '@/api/types';

interface Props {
  error: unknown;
  /**
   * suggested_action → 真实可执行的动作。
   * DD-008 §3.9.3：UI 要把拒绝渲染成「缩小时间窗 / 改走异步导出 / 稍后重试」的按钮，
   * 而不是一段红色文字 —— 所以没有对应 handler 的动作不渲染按钮，不假装可点。
   */
  actions?: Partial<Record<SuggestedAction, () => void>>;
  variant?: 'error' | 'warn';
}

export function ErrorCallout({ error, actions, variant = 'error' }: Props) {
  const described = describeApiError(error);
  const handler = described.action ? actions?.[described.action] : undefined;

  return (
    <div className={`callout ${variant}`} role="alert">
      <div className="callout-body">
        <div className="callout-title">{described.title}</div>
        <div className="muted">{described.message}</div>
        {described.retryAfterSeconds !== undefined && (
          <div className="muted">建议 {described.retryAfterSeconds} 秒后重试</div>
        )}
        {(handler ?? described.actionLabel) && (
          <div className="callout-actions">
            {handler ? (
              <button type="button" className="primary" onClick={handler}>
                {described.actionLabel}
              </button>
            ) : (
              <span className="muted">建议动作：{described.actionLabel}</span>
            )}
          </div>
        )}
        {described.queryId && (
          <div className="muted mono" style={{ marginTop: 6, fontSize: 11 }}>
            query_id: {described.queryId}
          </div>
        )}
      </div>
    </div>
  );
}
