/**
 * IF-7 契约类型的唯一出口。
 *
 * 这里只做「从 openapi-typescript 生成物中取别名」，不新增、不改名字段。
 * 规格源文件：api/openapi/if-007-query-v1.yaml（`npm run gen` 重新生成）。
 */
import type { components, operations } from './gen/if-007';

type Schemas = components['schemas'];

export type TimeRange = Schemas['TimeRange'];
export type LogFilter = Schemas['LogFilter'];
export type Signal = Schemas['Signal'];
export type Severity = Schemas['Severity'];
export type QueryId = Schemas['QueryId'];
export type Cursor = Schemas['Cursor'];
export type RowRef = Schemas['RowRef'];
export type DimensionValue = Schemas['DimensionValue'];
export type FieldDescriptor = Schemas['FieldDescriptor'];
export type LogRow = Schemas['LogRow'];
export type LogRowFull = Schemas['LogRowFull'];
export type HistogramBucket = Schemas['HistogramBucket'];
export type FacetValue = Schemas['FacetValue'];
export type AggSeries = Schemas['AggSeries'];
export type ApiErrorPayload = Schemas['ApiError'];
export type ApiErrorCode = ApiErrorPayload['code'];
export type SuggestedAction = NonNullable<ApiErrorPayload['suggested_action']>;

type JsonBody<O> = O extends { requestBody: { content: { 'application/json': infer B } } }
  ? B
  : never;
type JsonOk<O> = O extends { responses: { 200: { content: { 'application/json': infer R } } } }
  ? R
  : never;

export type ListDimensionsRequest = JsonBody<operations['listDimensions']>;
export type ListDimensionsResponse = JsonOk<operations['listDimensions']>;

export type ListFieldsRequest = JsonBody<operations['listFields']>;
export type ListFieldsResponse = JsonOk<operations['listFields']>;

export type SearchLogsRequest = JsonBody<operations['searchLogs']>;
export type SearchLogsResponse = JsonOk<operations['searchLogs']>;

export type LogHistogramRequest = JsonBody<operations['logHistogram']>;
export type LogHistogramResponse = JsonOk<operations['logHistogram']>;

export type LogFacetRequest = JsonBody<operations['logFacet']>;
export type LogFacetResponse = JsonOk<operations['logFacet']>;

export type GetLogRowResponse = JsonOk<operations['getLogRow']>;

export type LogContextRequest = JsonBody<operations['logContext']>;
export type LogContextResponse = JsonOk<operations['logContext']>;

export type CreateExportRequest = JsonBody<operations['createExport']>;

export const SEVERITIES: readonly Severity[] = [
  'TRACE',
  'DEBUG',
  'INFO',
  'WARN',
  'ERROR',
  'FATAL',
] as const;

/**
 * IF-007 §4.3：这几个字段无论是否出现在 select_paths 中都强制返回。
 * DD-008 §3.2 决策 5 只强制了其中四个（ts/service/cluster_id/trace_id），
 * 契约给的是超集，前端按契约的超集处理。
 */
export const ALWAYS_RETURNED_PATHS = [
  'row_ref',
  'ts',
  'service',
  'cluster_id',
  'trace_id',
  'severity',
  'body',
] as const;

export type AlwaysReturnedPath = (typeof ALWAYS_RETURNED_PATHS)[number];

export function isAlwaysReturnedPath(path: string): path is AlwaysReturnedPath {
  return (ALWAYS_RETURNED_PATHS as readonly string[]).includes(path);
}
