import { useDimensions } from '@/api/queries';
import { useGlobalContext } from '@/state/useLogsViewState';

import { DimensionFilter } from './DimensionFilter';
import { TimeRangePicker } from './TimeRangePicker';

/**
 * DD-008 §3.2 决策 2：时间选择器 + 项目/集群过滤器固定顶栏、跨页面保留，
 * 且 cluster 永远是第一维度 —— 所以它排在 project 前面，且每个查询都带上它，
 * 而不是在返回结果里过滤。
 *
 * 这些状态全部落在 URL 上，页面组件不持副本（见 state/urlState.ts）。
 */
export function GlobalContextBar() {
  const [global, setGlobal] = useGlobalContext();
  const dimensions = useDimensions(global.timeRange);

  const clusters = dimensions.data?.cluster ?? [];
  const projects = dimensions.data?.project ?? [];

  return (
    <header className="context-bar">
      <span className="brand">可观测性平台</span>

      <DimensionFilter
        label="集群"
        options={clusters}
        selected={global.clusterIds}
        loading={dimensions.isLoading}
        onChange={(clusterIds) => {
          setGlobal({ ...global, clusterIds });
        }}
      />
      <DimensionFilter
        label="项目"
        options={projects}
        selected={global.projectIds}
        loading={dimensions.isLoading}
        onChange={(projectIds) => {
          setGlobal({ ...global, projectIds });
        }}
      />

      <span className="spacer" />

      <TimeRangePicker
        value={global.timeRange}
        onChange={(timeRange) => {
          setGlobal({ ...global, timeRange });
        }}
      />
    </header>
  );
}
