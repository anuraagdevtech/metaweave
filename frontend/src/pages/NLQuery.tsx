import { useState } from "react";
import { api } from "../api/client";
import { ErrorBanner } from "../components/ErrorBanner";
import { Spinner } from "../components/Spinner";
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
    if (!q.trim()) return;
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
      <div className="page-header">
        <div>
          <h2>Natural Language Query</h2>
          <p className="page-subtitle">Ask about pipelines, jobs, or lineage in plain English.</p>
        </div>
      </div>
      <div className="card">
        <label className="field-label">Your question</label>
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          style={{ minHeight: 60 }}
          placeholder="e.g. Which jobs write to the analytics.customer_summary table?"
        />
        <button onClick={() => ask(question)} disabled={loading || !question.trim()}>
          {loading ? <Spinner label="Thinking…" /> : "Ask"}
        </button>
        <div className="chip-row" style={{ marginTop: 12 }}>
          {EXAMPLES.map((ex) => (
            <button key={ex} className="chip" onClick={() => ask(ex)} disabled={loading}>
              {ex}
            </button>
          ))}
        </div>
        {error && <ErrorBanner message={error} />}
      </div>

      {response && (
        <div className="card">
          <div className="card-header">
            <h3>Answer</h3>
          </div>
          <p>{response.answer}</p>
          {response.tool_calls.length > 0 && (
            <>
              <p className="field-label" style={{ margin: "14px 0 6px" }}>
                Tool calls
              </p>
              {response.tool_calls.map((tc, i) => (
                <div key={i} style={{ marginBottom: 10 }}>
                  <span className="badge badge-neutral">{tc.name}</span>
                  <pre className="code-viewer" style={{ marginTop: 6 }}>
                    <code style={{ display: "block", padding: 14 }}>
                      {JSON.stringify({ input: tc.input, result: tc.result }, null, 2)}
                    </code>
                  </pre>
                </div>
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}
