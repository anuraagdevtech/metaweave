import { useState } from "react";
import { api } from "../api/client";
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

  async function parse() {
    setError(null);
    try {
      const path = mode === "sql" ? "/lineage/parse-sql" : "/lineage/parse-pyspark";
      const body = mode === "sql" ? { sql: code } : { code };
      const res = await api.post<LineageResult[]>(path, body);
      setResults(res);
    } catch (e) {
      setError(String(e));
      setResults(null);
    }
  }

  return (
    <div>
      <h2>Lineage Parser</h2>
      <div className="card">
        <div className="dag-row">
          <button
            onClick={() => {
              setMode("sql");
              setCode(SAMPLE_SQL);
            }}
            style={{ background: mode === "sql" ? undefined : "#232838" }}
          >
            SQL
          </button>
          <button
            onClick={() => {
              setMode("pyspark");
              setCode(SAMPLE_PYSPARK);
            }}
            style={{ background: mode === "pyspark" ? undefined : "#232838" }}
          >
            PySpark
          </button>
        </div>
        <label className="field-label">{mode === "sql" ? "SQL statement(s)" : "PySpark script"}</label>
        <textarea value={code} onChange={(e) => setCode(e.target.value)} />
        <div>
          <button onClick={parse}>Extract Lineage</button>
        </div>
        {error && <p className="muted" style={{ color: "var(--danger)" }}>{error}</p>}
      </div>

      {results && (
        <div className="card">
          <h3>Results</h3>
          {results.map((r, i) => (
            <div key={i} style={{ marginBottom: 20 }}>
              <p>
                <span className="badge badge-neutral">{r.statement_type}</span>{" "}
                {r.target_table && <strong>{r.target_table}</strong>} ← {r.source_tables.join(", ") || "—"}
              </p>
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
          ))}
        </div>
      )}
    </div>
  );
}
