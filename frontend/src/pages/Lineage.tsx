import { useState } from "react";
import { api } from "../api/client";
import { ErrorBanner } from "../components/ErrorBanner";
import { Spinner } from "../components/Spinner";
import type { LineageResult } from "../api/types";

const SAMPLE_SQL = `INSERT INTO analytics.customer_summary (customer_id, total_orders, total_revenue)
SELECT c.customer_id, COUNT(o.order_id) AS total_orders, SUM(o.amount) AS total_revenue
FROM sales.customers c
JOIN sales.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id;`;

const SAMPLE_PYSPARK = `df = spark.table("sales.customers")
orders = spark.read.format("delta").load("staging.orders")
joined = df.join(orders, "customer_id")
result = joined.withColumn("total", col("amount") * 2)
result.write.format("delta").mode("overwrite").saveAsTable("analytics.customer_totals")`;

export function Lineage() {
  const [mode, setMode] = useState<"sql" | "pyspark">("sql");
  const [code, setCode] = useState(SAMPLE_SQL);
  const [results, setResults] = useState<LineageResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function parse() {
    setError(null);
    setLoading(true);
    try {
      const path = mode === "sql" ? "/lineage/parse-sql" : "/lineage/parse-pyspark";
      const body = mode === "sql" ? { sql: code } : { code };
      const res = await api.post<LineageResult[]>(path, body);
      setResults(res);
    } catch (e) {
      setError(String(e));
      setResults(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>Lineage Parser</h2>
          <p className="page-subtitle">
            Paste a SQL statement or PySpark script to extract table- and column-level lineage.
          </p>
        </div>
      </div>
      <div className="card">
        <div className="dag-row">
          <button
            className={mode === "sql" ? "" : "btn-secondary"}
            onClick={() => {
              setMode("sql");
              setCode(SAMPLE_SQL);
            }}
          >
            SQL
          </button>
          <button
            className={mode === "pyspark" ? "" : "btn-secondary"}
            onClick={() => {
              setMode("pyspark");
              setCode(SAMPLE_PYSPARK);
            }}
          >
            PySpark
          </button>
        </div>
        <label className="field-label">{mode === "sql" ? "SQL statement(s)" : "PySpark script"}</label>
        <textarea value={code} onChange={(e) => setCode(e.target.value)} />
        <div>
          <button onClick={parse} disabled={loading || !code.trim()}>
            {loading ? <Spinner label="Parsing…" /> : "Extract Lineage"}
          </button>
        </div>
        {error && <ErrorBanner message={error} />}
      </div>

      {results && (
        <div className="card">
          <div className="card-header">
            <h3>Results</h3>
            <span className="card-header-meta">{results.length} statement{results.length === 1 ? "" : "s"}</span>
          </div>
          {results.map((r, i) => (
            <div key={i} style={{ marginBottom: 20 }}>
              <p>
                <span className="badge badge-neutral">{r.statement_type}</span>{" "}
                {r.target_table && <strong>{r.target_table}</strong>} ← {r.source_tables.join(", ") || "—"}
              </p>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Target Column</th>
                      <th>Source Table</th>
                      <th>Source Column</th>
                      <th>Transformation</th>
                    </tr>
                  </thead>
                  <tbody>
                    {r.column_lineage.map((c, j) => (
                      <tr key={j}>
                        <td>{c.target_column}</td>
                        <td>{c.source_table ?? "—"}</td>
                        <td>{c.source_column ?? "—"}</td>
                        <td>
                          <code>{c.transformation}</code>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
