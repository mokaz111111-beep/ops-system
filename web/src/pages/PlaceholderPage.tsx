import { useLocation, useParams } from 'react-router-dom';

import { NAV_GROUPS } from '@/navigation';

/**
 * 占位页存在的唯一理由：让「一切皆可跳转」的链接构造在 M1 就是真的
 * —— 目标页还没做，但 URL 与上下文参数已经按最终形态传过来了（DD-008 §3.2）。
 * 页面把收到的上下文原样打出来，方便验证跳转没丢参数。
 */
export function PlaceholderPage() {
  const location = useLocation();
  const params = useParams();

  const item = NAV_GROUPS.flatMap((g) => g.items).find((i) =>
    location.pathname.startsWith(i.path),
  );

  return (
    <div className="placeholder-page">
      <h1>{item?.label ?? '未实现页面'}</h1>
      <p className="muted">
        计划里程碑：{item?.milestone ?? '待定'}。PLAN §3 明确「M1 前端只做检索页」，
        看板、服务拓扑、Live Tail、Trace 瀑布图都在 M2~M3。
      </p>
      {item?.note && <p className="muted">{item.note}</p>}

      <div className="ctx">
        <div>pathname: {location.pathname}</div>
        {Object.entries(params).map(([k, v]) => (
          <div key={k}>
            {k}: {v}
          </div>
        ))}
        <div>search: {location.search || '(空)'}</div>
      </div>
      <p className="muted" style={{ marginTop: 12 }}>
        上面这段 search 就是跳转携带的全局上下文（时间窗 / cluster / project）。
        页面实现后直接读它即可还原上下文。
      </p>
    </div>
  );
}
