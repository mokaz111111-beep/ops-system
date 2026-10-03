import { Navigate, Route, Routes } from 'react-router-dom';

import { AppShell } from '@/components/AppShell';
import { NAV_GROUPS } from '@/navigation';
import { LogsSearchPage } from '@/pages/logs/LogsSearchPage';
import { PlaceholderPage } from '@/pages/PlaceholderPage';

/** 「跳转目标」用到的详情路由。M1 全是占位，但地址与参数形态按最终形态定。 */
const DETAIL_PLACEHOLDER_ROUTES = ['/apm/traces/:traceId', '/apm/services/:service'];

export function AppRoutes() {
  const placeholderPaths = NAV_GROUPS.flatMap((g) => g.items)
    .filter((i) => !i.implemented)
    .map((i) => i.path);

  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Navigate to="/logs/search?from=now-1h&to=now" replace />} />
        <Route path="/logs/search" element={<LogsSearchPage />} />
        {[...placeholderPaths, ...DETAIL_PLACEHOLDER_ROUTES].map((path) => (
          <Route key={path} path={path} element={<PlaceholderPage />} />
        ))}
        <Route path="*" element={<Navigate to="/logs/search?from=now-1h&to=now" replace />} />
      </Route>
    </Routes>
  );
}
