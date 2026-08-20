export function SkeletonRows({ count = 4 }: { count?: number }) {
  return (
    <div>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="skeleton skeleton-row" style={{ width: `${85 - i * 8}%` }} />
      ))}
    </div>
  );
}
