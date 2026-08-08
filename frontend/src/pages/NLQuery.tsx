import { useState } from "react";
import { api } from "../api/client";
import type { NLQueryResponse } from "../api/types";

const EXAMPLES = [
  "Which downstream reporting tables will be affected if the churn_prediction job fails?",
  "Show me all PySpark jobs that touched the customer_dim table this week",
];

export function NLQuery() {
  const [question, setQuestion] = useState("");
  const [response, setResponse] = useState<NLQueryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function ask(q: string) {
    setQuestion(q);
    setLoading(true);
    setError(null);
    try {
      const res = await api.post<NLQueryResponse>("/nl-query", { question: q });
      setResponse(res);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <h2>Natural Language Query</h2>
      <div className="card">
        <label className="field-label">Ask about pipelines, jobs, or lineage</label>
        <textarea value={question} onChange={(e) => setQuestion(e.target.value)} style={{ minHeight: 60 }} />
        <button onClick={() => ask(question)} disabled={loading}>
          {loading ? "Thinking…" : "Ask"}
        </button>
        <div style={{ marginTop: 10 }}>
          {EXAMPLES.map((ex) => (
            <button key={ex} onClick={() => ask(ex)} style={{ background: "#232838", marginRight: 8 }}>
              {ex}
            </button>
          ))}
        </div>
        {error && <p className="muted" style={{ color: "var(--danger)" }}>{error}</p>}
      </div>

      {response && (
        <div className="card">
          <h3>Answer</h3>
          <p>{response.answer}</p>
          {response.tool_calls.length > 0 && (
            <>
              <h4>Tool calls</h4>
              {response.tool_calls.map((tc, i) => (
                <div key={i} style={{ marginBottom: 10 }}>
                  <span className="badge badge-neutral">{tc.name}</span>
                  <pre className="code-viewer">{JSON.stringify({ input: tc.input, result: tc.result }, null, 2)}</pre>
                </div>
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}
