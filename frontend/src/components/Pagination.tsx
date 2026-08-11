interface Props {
  page: number;
  pageCount: number;
  onChange: (page: number) => void;
}

export function Pagination({ page, pageCount, onChange }: Props) {
  if (pageCount <= 1) return null;
  return (
    <div className="pagination">
      <button
        className="btn-secondary btn-sm"
        onClick={() => onChange(page - 1)}
        disabled={page <= 1}
      >
        ← Prev
      </button>
      <span>
        Page {page} of {pageCount}
      </span>
      <button
        className="btn-secondary btn-sm"
        onClick={() => onChange(page + 1)}
        disabled={page >= pageCount}
      >
        Next →
      </button>
    </div>
  );
}
