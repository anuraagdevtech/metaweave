import { useMemo, useState } from "react";
import { api } from "../api/client";
import { EmptyState } from "../components/EmptyState";
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
  const [resultView, setResultView] = useState<"table" | "attribute">("table");

  const attributeRows = useMemo(
    () =>
      (results ?? []).flatMap((r, statementIndex) =>
        r.column_lineage.map((c, columnIndex) => ({ ...c, statementIndex, columnIndex }))
      ),
    [results]
  );

  async function parse() {
    setError(null);
    setLoading(true);
    try {
      const path = mode === "sql" ? "/lineage/parse-sql" : "/lineage/parse-pyspark";
      const body = mode === "sql" ? { sql: code } : { code };
      const res = await api.post<LineageResult[]>(path, body);
      setResults(res);
      setResultView("table");
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

          <div className="chip-row" style={{ marginBottom: 16 }}>
            <button
              className={`chip${resultView === "table" ? " active" : ""}`}
              onClick={() => setResultView("table")}
            >
              Table Lineage
            </button>
            <button
              className={`chip${resultView === "attribute" ? " active" : ""}`}
              onClick={() => setResultView("attribute")}
            >
              Attribute Lineage ({attributeRows.length})
            </button>
          </div>

          {resultView === "table" ? (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Statement Type</th>
                    <th>Target Table</th>
                    <th>Source Tables</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => (
                    <tr key={i}>
                      <td>{i + 1}</td>
                      <td>
                        <span className="badge badge-neutral">{r.statement_type}</span>
                      </td>
                      <td>{r.target_table ? <strong>{r.target_table}</strong> : "—"}</td>
                      <td className="wrap">
                        {r.source_tables.length > 0 ? (
                          <div className="chip-row">
                            {r.source_tables.map((t) => (
                              <span key={t} className="chip" style={{ cursor: "default" }}>
                                {t}
                              </span>
                            ))}
                          </div>
                        ) : (
                          "—"
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : attributeRows.length === 0 ? (
            <EmptyState
              icon="⛓"
              title="No column-level lineage extracted"
              subtitle="This statement type doesn't produce column-to-column mappings."
            />
          ) : (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Target Table</th>
                    <th>Target Column</th>
                    <th>Source Table</th>
                    <th>Source Column</th>
                    <th>Transformation</th>
                  </tr>
                </thead>
                <tbody>
                  {attributeRows.map((c) => (
                    <tr key={`${c.statementIndex}-${c.columnIndex}`}>
                      <td>{c.statementIndex + 1}</td>
                      <td>{c.target_table ?? "—"}</td>
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
          )}
        </div>
      )}
    </div>
  );
}
