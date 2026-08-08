export function CodeViewer({ code, language }: { code: string; language?: string }) {
  return (
    <div>
      {language && <div className="muted" style={{ marginBottom: 4 }}>{language}</div>}
      <pre className="code-viewer">
        <code>{code}</code>
      </pre>
    </div>
  );
}
