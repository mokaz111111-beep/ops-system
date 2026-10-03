import { create } from 'zustand';
import { persist } from 'zustand/middleware';

/**
 * 纯视图状态：DD-008 §3.4 结论 1 明确「调整列顺序、列宽属于纯视图状态，不触发查询」。
 * 列顺序进 URL（分享链接要还原视觉顺序）但不进查询键；列宽连 URL 都不进，落 localStorage。
 *
 * pinnedFacets 对应 §3.4 的「创建 Facet」：把某个路径提升为常驻 facet。
 * 它的持久化归属（IF-7 还是控制面）在 DD-008 §4 仍是 Open Question，
 * 所以这里只放浏览器本地，不假设任何后端存储。
 */
interface ViewState {
  columnWidths: Record<string, number>;
  setColumnWidth: (path: string, width: number) => void;
  pinnedFacets: string[];
  togglePinnedFacet: (path: string) => void;
}

export const useViewState = create<ViewState>()(
  persist(
    (set, get) => ({
      columnWidths: {},
      setColumnWidth: (path, width) =>
        set((s) => ({ columnWidths: { ...s.columnWidths, [path]: Math.max(60, width) } })),
      pinnedFacets: [],
      togglePinnedFacet: (path) => {
        const pinned = get().pinnedFacets;
        set({
          pinnedFacets: pinned.includes(path)
            ? pinned.filter((p) => p !== path)
            : [...pinned, path],
        });
      },
    }),
    { name: 'ops-system.logs.view-state' },
  ),
);

export const DEFAULT_COLUMN_WIDTH: Record<string, number> = {
  ts: 170,
  severity: 84,
  service: 150,
  cluster_id: 150,
  trace_id: 150,
  body: 560,
};

export function columnWidthOf(widths: Record<string, number>, path: string): number {
  return widths[path] ?? DEFAULT_COLUMN_WIDTH[path] ?? 180;
}
