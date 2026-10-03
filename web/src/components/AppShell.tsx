import { NavLink, Outlet, useLocation } from 'react-router-dom';

import { NAV_GROUPS } from '@/navigation';

import { GlobalContextBar } from './GlobalContextBar';

export function AppShell() {
  const { search } = useLocation();

  return (
    <div className="app">
      <GlobalContextBar />
      <nav className="sidebar" aria-label="一级导航">
        {NAV_GROUPS.map((group) => (
          <div key={group.title}>
            <div className="nav-group-title">{group.title}</div>
            {group.items.map((item) => (
              <NavLink
                key={item.path}
                // 导航也带上全局上下文：跳转 = 构造带完整上下文参数的 URL（DD-008 §3.2）
                to={{ pathname: item.path, search }}
                className={({ isActive }) => `nav-item${isActive ? ' active' : ''}`}
              >
                <span>{item.label}</span>
                {!item.implemented && <span className="milestone-tag">{item.milestone}</span>}
              </NavLink>
            ))}
          </div>
        ))}
      </nav>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
