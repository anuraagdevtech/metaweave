import { useState } from "react";
import { api } from "../api/client";
import { GraphView } from "../components/GraphView";
import type { BlastRadiusResponse, RcaResponse } from "../api/types";

export function BlastRadius() {
  const [nodeId, setNodeId] = useState("");
  const [mode, setMode] = useState<"blast" | "rca">("blast");
  const [response, setResponse] = useState<BlastRadiusResponse | RcaResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function run() {
    if (!nodeId.trim()) return;
    setError(null);
    try {
      const path = mode === "blast" ? `/blast-radius/${encodeURIComponent(nodeId)}` : `/rca/${encodeURIComponent(nodeId)}`;
      const res = await api.get<BlastRadiusResponse | RcaResponse>(path);
      setResponse(res);
    } catch (e) {
      setError(String(e));
      setResponse(null);
    }
  }

  const nodes = response
    ? "impacted_nodes" in response
      ? response.impacted_nodes
      : response.root_cause_candidates
    : [];

  return (
    <div>
      <h2>Blast Radius / RCA</h2>
      <p className="muted">
        Enter a task ID (e.g. from Job Detail) or a dataset node in the form <code>dataset:schema.table</code>.
      </p>
      <div className="card">
        <label className="field-label">Node ID</label>
        <input value={nodeId} onChange={(e) => setNodeId(e.target.value)} placeholder="dataset:sales.orders" />
        <div className="dag-row" style={{ marginTop: 10 }}>
          <button onClick={() => setMode("blast")} style={{ background: mode === "blast" ? undefined : "#232838" }}>
            Blast Radius (downstream)
          </button>
          <button onClick={() => setMode("rca")} style={{ background: mode === "rca" ? undefined : "#232838" }}>
            RCA (upstream)
          </button>
        </div>
        <button onClick={run}>Run</button>
        {error && <p className="muted" style={{ color: "var(--danger)" }}>{error}</p>}
      </div>

      {response && (
        <div className="card">
          <h3>{mode === "blast" ? "Downstream impact" : "Root-cause candidates"}</h3>
          <GraphView root={response.root} nodes={nodes} />
        </div>
      )}
    </div>
  );
}
