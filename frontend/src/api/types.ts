export interface Workflow {
  workflow_id: string;
  name: string;
  owner_team: string;
  owner_email: string;
  sla_minutes: number;
  description: string | null;
  tags: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface Job {
  job_id: string;
  workflow_id: string;
  name: string;
  cron_expression: string | null;
  max_retries: number;
  retry_delay_seconds: number;
  max_concurrency: number;
  cluster_config: Record<string, unknown>;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export type ExecType = "SQL" | "PYSPARK" | "PYTHON" | "NOTEBOOK";

export interface Task {
  task_id: string;
  job_id: string;
  name: string;
  task_order: number;
  exec_type: ExecType;
  script_path: string | null;
  source_code: string | null;
  environment: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export type RunStatus = "PENDING" | "RUNNING" | "SUCCESS" | "FAILED" | "SKIPPED" | "CANCELLED";

export interface JobAudit {
  audit_id: string;
  job_id: string;
  run_id: string;
  status: RunStatus;
  started_at: string;
  ended_at: string | null;
  rows_processed: number | null;
  cost_usd: number | null;
  error_message: string | null;
  logs_url: string | null;
  created_at: string;
}

export type DependencyKind = "TASK" | "DATASET";

export interface JobDependency {
  dependency_id: string;
  task_id: string;
  depends_on_kind: DependencyKind;
  depends_on_task_id: string | null;
  depends_on_table: string | null;
  created_at: string;
}

export interface JobAlert {
  alert_id: string;
  job_id: string;
  alert_type: string;
  distribution_list: string[];
  routing_rule: Record<string, unknown>;
  is_active: boolean;
}

export interface ColumnLineage {
  target_table: string | null;
  target_column: string;
  source_table: string | null;
  source_column: string | null;
  transformation: string;
}

export interface LineageResult {
  statement_type: string;
  source_tables: string[];
  target_table: string | null;
  column_lineage: ColumnLineage[];
}

export interface ImpactedNode {
  node_id: string;
  depth: number;
  is_dataset: boolean;
}

export interface BlastRadiusResponse {
  root: string;
  impacted_nodes: ImpactedNode[];
}

export interface RcaResponse {
  root: string;
  root_cause_candidates: ImpactedNode[];
}

export interface NLQueryToolCall {
  name: string;
  input: Record<string, unknown>;
  result: Record<string, unknown>;
}

export interface NLQueryResponse {
  answer: string;
  tool_calls: NLQueryToolCall[];
}

export interface SqlComplexity {
  ctes: number;
  joins: number;
  tables: number;
  lines: number;
}

export interface SqlExample {
  task_id: string;
  task_name: string;
  category: string | null;
  description: string | null;
  domain: string | null;
  complexity: SqlComplexity | null;
  sql: string;
  job_id: string;
  job_name: string;
  workflow_id: string;
  workflow_name: string;
  owner_team: string;
}
