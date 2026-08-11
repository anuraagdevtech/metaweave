export function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="error-banner" role="alert">
      <span className="error-icon" aria-hidden="true">
        !
      </span>
      <span>{message}</span>
    </div>
  );
}
