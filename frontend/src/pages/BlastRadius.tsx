import { useState } from "react";
import { api } from "../api/client";
import { GraphView } from "../components/GraphView";
import { ErrorBanner } from "../components/ErrorBanner";
import { Spinner } from "../components/Spinner";
import type { BlastRadiusResponse, RcaResponse } from "../api/types";

export function BlastRadius() {
  const [nodeId, setNodeId] = useState("");
  const [mode, setMode] = useState<"blast" | "rca">("blast");
  const [response, setResponse] = useState<BlastRadiusResponse | RcaResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function run() {
    if (!nodeId.trim()) return;
    setError(null);
    setLoading(true);
    try {
      const path = mode === "blast" ? `/blast-radius/${encodeURIComponent(nodeId)}` : `/rca/${encodeURIComponent(nodeId)}`;
      const res = await api.get<BlastRadiusResponse | RcaResponse>(path);
      setResponse(res);
    } catch (e) {
      setError(String(e));
      setResponse(null);
    } finally {
      setLoading(false);
    }
  }

  const nodes = response
    ? "impacted_nodes" in response
      ? response.impacted_nodes
      : response.root_cause_candidates
    : [];

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>Blast Radius / RCA</h2>
          <p className="page-subtitle">
            Trace downstream impact or upstream root causes across the task/dataset dependency graph.
          </p>
        </div>
      </div>
      <div className="card">
        <label className="field-label">Node ID</label>
        <input
          value={nodeId}
          onChange={(e) => setNodeId(e.target.value)}
          placeholder="dataset:sales.orders"
          onKeyDown={(e) => e.key === "Enter" && run()}
        />
        <p className="muted" style={{ marginTop: 6 }}>
          Enter a task ID (from Job Detail) or a dataset node as <code>dataset:schema.table</code>.
        </p>
        <div className="dag-row" style={{ marginTop: 10 }}>
          <button className={mode === "blast" ? "" : "btn-secondary"} onClick={() => setMode("blast")}>
            Blast Radius (downstream)
          </button>
          <button className={mode === "rca" ? "" : "btn-secondary"} onClick={() => setMode("rca")}>
            RCA (upstream)
          </button>
        </div>
        <button onClick={run} disabled={loading || !nodeId.trim()}>
          {loading ? <Spinner label="Running…" /> : "Run"}
        </button>
        {error && <ErrorBanner message={error} />}
      </div>

      {response && (
        <div className="card">
          <div className="card-header">
            <h3>{mode === "blast" ? "Downstream impact" : "Root-cause candidates"}</h3>
            <span className="card-header-meta">{nodes.length} node{nodes.length === 1 ? "" : "s"}</span>
          </div>
          <GraphView root={response.root} nodes={nodes} />
        </div>
      )}
    </div>
  );
}
