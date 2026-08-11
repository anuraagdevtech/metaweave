export function Spinner({ label }: { label?: string }) {
  return (
    <span className="muted" style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
      <span className="spinner" aria-hidden="true" />
      {label ?? "Loading…"}
    </span>
  );
}
