/**
 * 一级导航骨架 = DD-008 §3.3 的信息架构原样落地。
 * M1 只实现日志检索一页（PLAN §3「M1 前端只做检索页」），其余全部是占位路由 ——
 * 列在这里是为了让「跳转目标」在链接构造时就有真实地址，页面补齐时调用方不用改。
 */
export interface NavItem {
  label: string;
  path: string;
  /** 上线里程碑，取自 DD-008 §3.3 的归属表。 */
  milestone: 'M1' | 'M2' | 'M3' | 'M4' | '二期' | '待定';
  implemented?: boolean;
  /** 占位页上解释「为什么现在没有」的依据。 */
  note?: string;
}

export interface NavGroup {
  title: string;
  items: NavItem[];
}

export const NAV_GROUPS: readonly NavGroup[] = [
  {
    title: '总览',
    items: [
      {
        label: '服务健康矩阵 + AI 入口',
        path: '/overview',
        milestone: 'M3',
        note: '矩阵依赖 span 派生的 RED 指标与 entity_registry，二者均为 M3 交付（DD-008 §3.3、§4）。',
      },
    ],
  },
  {
    title: 'AI 助手',
    items: [
      {
        label: '对话 / 巡检 / 报告',
        path: '/ai',
        milestone: 'M4',
        note: '走 DD-007 经 IF-8，不在 IF-7 范围内（IF-007 §8 FE-27）。',
      },
    ],
  },
  {
    title: '应用性能',
    items: [
      { label: '服务目录', path: '/apm/services', milestone: 'M3', note: 'FE-20，IF-007 §7 仅预留签名。' },
      { label: '链路追踪', path: '/apm/traces', milestone: 'M3', note: 'FE-14，IF-007 §7 仅预留签名。' },
      { label: '错误分析', path: '/apm/errors', milestone: 'M3', note: 'FE-18，IF-007 §7 仅预留签名。' },
      { label: '服务拓扑', path: '/apm/topology', milestone: 'M3', note: 'FE-19，依赖 entity_topo。' },
    ],
  },
  {
    title: '日志',
    items: [
      { label: '检索', path: '/logs/search', milestone: 'M1', implemented: true },
      {
        label: 'Live Tail',
        path: '/logs/live-tail',
        milestone: '待定',
        note: 'FE-09：IF-007 §5.2 明确 v1 不包含流式能力，三条实现路径均未选型。',
      },
      {
        label: '模式聚类',
        path: '/logs/patterns',
        milestone: '二期',
        note: 'DD-008 §3.8 列为二期项。',
      },
    ],
  },
  {
    title: '指标',
    items: [
      {
        label: 'Metrics Explorer',
        path: '/metrics/explorer',
        milestone: 'M2',
        note: 'FE-21 / FE-22，IF-007 §4.9~§4.10 已冻结，但 PLAN §3 把它排在 M2。',
      },
      {
        label: 'Dashboards',
        path: '/metrics/dashboards',
        milestone: 'M2',
        note: 'FE-23 Grafana 免密嵌入方案未定（IF-007 §6）。',
      },
    ],
  },
  {
    title: '告警',
    items: [
      { label: '规则 / 事件 / 静默', path: '/alerts', milestone: 'M3', note: 'FE-25，接口归属待定。' },
      { label: 'SLO', path: '/alerts/slo', milestone: 'M4', note: 'FE-26。' },
    ],
  },
  {
    title: '接入管理',
    items: [
      {
        label: '集群与 Agent 状态',
        path: '/ingest/agents',
        milestone: 'M2',
        note: 'FE-29 归属 IF-11（DD-001），契约尚不存在（IF-007 §6）。',
      },
      {
        label: '接入向导',
        path: '/ingest/wizard',
        milestone: 'M2',
        note: 'FE-30 归属 IF-11，需摄入侧计量旁路，不走检索路径。',
      },
    ],
  },
  {
    title: '管理',
    items: [
      {
        label: '项目 / 配额 / 留存 / 审计',
        path: '/admin',
        milestone: 'M4',
        note: 'FE-31 走控制面 API（DD-006 / IF-9），不走 IF-7。',
      },
    ],
  },
];
