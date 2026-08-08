import { NavLink, Route, Routes } from "react-router-dom";
import { Dashboard } from "./pages/Dashboard";
import { JobDetail } from "./pages/JobDetail";
import { Lineage } from "./pages/Lineage";
import { BlastRadius } from "./pages/BlastRadius";
import { NLQuery } from "./pages/NLQuery";

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", end: true },
  { to: "/lineage", label: "Lineage" },
  { to: "/blast-radius", label: "Blast Radius / RCA" },
  { to: "/nl-query", label: "NL Query" },
];

export function App() {
  return (
    <div className="app-shell">
      <nav className="nav">
        <h1>MetaWeave</h1>
        {NAV_ITEMS.map((item) => (
          <NavLink key={item.to} to={item.to} end={item.end} className={({ isActive }) => (isActive ? "active" : "")}>
            {item.label}
          </NavLink>
        ))}
      </nav>
      <main className="main">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/jobs/:jobId" element={<JobDetail />} />
          <Route path="/lineage" element={<Lineage />} />
          <Route path="/blast-radius" element={<BlastRadius />} />
          <Route path="/nl-query" element={<NLQuery />} />
        </Routes>
      </main>
    </div>
  );
}
