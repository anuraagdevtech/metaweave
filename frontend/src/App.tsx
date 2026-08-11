import { NavLink, Route, Routes } from "react-router-dom";
import { Dashboard } from "./pages/Dashboard";
import { JobDetail } from "./pages/JobDetail";
import { Lineage } from "./pages/Lineage";
import { BlastRadius } from "./pages/BlastRadius";
import { NLQuery } from "./pages/NLQuery";
import { SqlExamples } from "./pages/SqlExamples";

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", end: true, icon: "▤" },
  { to: "/sql-examples", label: "SQL Examples", icon: "▧" },
  { to: "/lineage", label: "Lineage", icon: "⇉" },
  { to: "/blast-radius", label: "Blast Radius / RCA", icon: "◎" },
  { to: "/nl-query", label: "NL Query", icon: "✦" },
];

export function App() {
  return (
    <div className="app-shell">
      <nav className="nav">
        <div className="nav-brand">
          <div className="nav-brand-mark">M</div>
          <h1>
            MetaWeave
            <span>Pipeline metadata platform</span>
          </h1>
        </div>
        <p className="nav-section-label">Navigate</p>
        <div className="nav-links">
          {NAV_ITEMS.map((item) => (
            <NavLink key={item.to} to={item.to} end={item.end} className={({ isActive }) => (isActive ? "active" : "")}>
              <span className="nav-icon" aria-hidden="true">
                {item.icon}
              </span>
              {item.label}
            </NavLink>
          ))}
        </div>
        <div className="nav-footer">MetaWeave v0.1.0</div>
      </nav>
      <main className="main">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/jobs/:jobId" element={<JobDetail />} />
          <Route path="/sql-examples" element={<SqlExamples />} />
          <Route path="/lineage" element={<Lineage />} />
          <Route path="/blast-radius" element={<BlastRadius />} />
          <Route path="/nl-query" element={<NLQuery />} />
        </Routes>
      </main>
    </div>
  );
}
