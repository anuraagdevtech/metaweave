import { useState } from "react";
import { highlightSql } from "../lib/sqlHighlight";

export function CodeViewer({ code, language }: { code: string; language?: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard API unavailable (e.g. insecure context) — silently ignore
    }
  }

  const isSql = language?.toUpperCase() === "SQL";

  return (
    <div className="code-viewer">
      <div className="code-viewer-toolbar">
        <p className="muted">{language ?? "code"}</p>
        <button className={`copy-btn${copied ? " copied" : ""}`} onClick={copy}>
          {copied ? "✓ Copied" : "Copy"}
        </button>
      </div>
      <pre>
        {isSql ? (
          <code dangerouslySetInnerHTML={{ __html: highlightSql(code) }} />
        ) : (
          <code>{code}</code>
        )}
      </pre>
    </div>
  );
}
