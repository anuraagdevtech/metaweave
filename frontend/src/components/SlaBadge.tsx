import type { RunStatus } from "../api/types";

const STYLES: Record<string, string> = {
  SUCCESS: "badge-ok",
  RUNNING: "badge-warn",
  PENDING: "badge-neutral",
  FAILED: "badge-fail",
  SKIPPED: "badge-neutral",
  CANCELLED: "badge-neutral",
};

export function SlaBadge({ status }: { status: RunStatus | string }) {
  return <span className={`badge ${STYLES[status] ?? "badge-neutral"}`}>{status}</span>;
}
