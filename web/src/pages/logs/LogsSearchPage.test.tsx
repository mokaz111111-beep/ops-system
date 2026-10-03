import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { describe, expect, it } from 'vitest';

import { LogsSearchPage } from './LogsSearchPage';

function LocationProbe() {
  const location = useLocation();
  return <div data-testid="location-search">{location.search}</div>;
}

function renderPage(initialUrl = '/logs/search?from=now-1h&to=now') {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  const utils = render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[initialUrl]}>
        <LogsSearchPage />
        <LocationProbe />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...utils, search: () => screen.getByTestId('location-search').textContent ?? '' };
}

describe('日志检索页（DD-008 §3.4）', () => {
  it('对 mock 跑通：加载一页日志并渲染默认列', async () => {
    renderPage();
    expect(await screen.findByText(/已加载 100 行/, {}, { timeout: 5000 })).toBeInTheDocument();

    const header = document.querySelector('.log-header');
    expect(header).not.toBeNull();
    for (const label of ['时间', 'SEVERITY', 'SERVICE', 'CLUSTER', 'BODY', 'TRACE']) {
      expect(within(header as HTMLElement).getByText(label)).toBeInTheDocument();
    }
    expect(document.querySelectorAll('.log-row').length).toBeGreaterThan(0);
  });

  it('点 facet 取值把条件写进 URL，三段共享同一份过滤条件', async () => {
    const user = userEvent.setup();
    const { search } = renderPage();
    await screen.findByText(/已加载 100 行/, {}, { timeout: 5000 });

    // service facet 默认展开，等它的取值回来
    const serviceGroup = [...document.querySelectorAll('.facet-group')].find(
      (g) => g.querySelector('.facet-path')?.textContent === 'service',
    ) as HTMLElement;
    await waitFor(
      () => {
        expect(serviceGroup.querySelectorAll('.facet-value').length).toBeGreaterThan(0);
      },
      { timeout: 5000 },
    );

    const firstValue = serviceGroup.querySelector('.facet-value .v') as HTMLElement;
    const picked = firstValue.textContent ?? '';
    await user.click(firstValue);

    await waitFor(() => {
      expect(search()).toContain(`facet=service%3D${encodeURIComponent(picked)}`);
    });
  });

  it('展开一行会发起详情请求（不是纯前端展开），并把 row 写进 URL', async () => {
    const user = userEvent.setup();
    const { search } = renderPage();
    await screen.findByText(/已加载 100 行/, {}, { timeout: 5000 });

    const firstRow = document.querySelector('.log-row') as HTMLElement;
    await user.click(firstRow);

    await waitFor(() => {
      expect(search()).toContain('row=r-');
    });
    // 详情是 FE-07 整条读的结果：列表投影里根本没有 resource attributes
    expect(
      await screen.findByText('resource attributes', {}, { timeout: 5000 }),
    ).toBeInTheDocument();
  });

  it('删除一列会改变查询，并把列集合写进 URL（列配置是查询的一部分）', async () => {
    const user = userEvent.setup();
    const { search } = renderPage();
    await screen.findByText(/已加载 100 行/, {}, { timeout: 5000 });

    const removeButtons = document.querySelectorAll('.log-header .cell button[title^="移除该列"]');
    await user.click(removeButtons[removeButtons.length - 1] as HTMLElement);

    await waitFor(() => {
      expect(search()).toContain('cols=');
    });
    expect(search()).not.toContain('trace_id');
  });

  it('时间窗无法满足时把拒绝渲染成可操作提示，而不是红色报错', async () => {
    renderPage('/logs/search?from=now-60d&to=now');
    expect(
      await screen.findByText('时间窗超出该接口上限', {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: '缩小时间窗' }).length).toBeGreaterThan(0);
  });
});
