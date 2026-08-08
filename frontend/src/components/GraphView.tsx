import type { ImpactedNode } from "../api/types";

interface Props {
  root: string;
  nodes: ImpactedNode[];
}

const COL_WIDTH = 220;
const ROW_HEIGHT = 44;
const NODE_W = 190;
const NODE_H = 30;
const PAD = 24;

/**
 * Hand-rolled SVG layered-DAG view: nodes are grouped into columns by depth
 * from `root` (as returned by the blast-radius/RCA API). Since the API
 * returns depths rather than an explicit edge list, we draw one flow line
 * per column transition (centroid-to-centroid) to indicate direction rather
 * than claiming exact node-to-node adjacency.
 */
export function GraphView({ root, nodes }: Props) {
  const depths = Array.from(new Set(nodes.map((n) => n.depth))).sort((a, b) => a - b);
  const columns: ImpactedNode[][] = [[{ node_id: root, depth: 0, is_dataset: root.startsWith("dataset:") }]];
  for (const d of depths) columns.push(nodes.filter((n) => n.depth === d));

  const maxRows = Math.max(...columns.map((c) => c.length));
  const width = PAD * 2 + columns.length * COL_WIDTH;
  const height = PAD * 2 + Math.max(1, maxRows) * ROW_HEIGHT;

  const centerY = (col: ImpactedNode[], i: number) => {
    const colHeight = col.length * ROW_HEIGHT;
    const offset = (height - colHeight) / 2;
    return offset + i * ROW_HEIGHT + ROW_HEIGHT / 2;
  };
  const colX = (c: number) => PAD + c * COL_WIDTH;
  const centroidY = (col: ImpactedNode[]) =>
    col.reduce((sum, _n, i) => sum + centerY(col, i), 0) / col.length;

  if (nodes.length === 0) {
    return <p className="muted">No connected nodes found.</p>;
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <svg width={width} height={height} role="img" aria-label={`Graph rooted at ${root}`}>
        {columns.slice(1).map((col, idx) => {
          const prevCol = columns[idx];
          const x1 = colX(idx) + NODE_W;
          const x2 = colX(idx + 1);
          const y1 = centroidY(prevCol);
          const y2 = centroidY(col);
          return (
            <line
              key={`flow-${idx}`}
              x1={x1}
              y1={y1}
              x2={x2}
              y2={y2}
              stroke="#3a4256"
              strokeWidth={1.5}
              markerEnd="url(#arrow)"
            />
          );
        })}
        <defs>
          <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" fill="#3a4256" />
          </marker>
        </defs>
        {columns.map((col, c) =>
          col.map((node, i) => (
            <g key={node.node_id} transform={`translate(${colX(c)}, ${centerY(col, i) - NODE_H / 2})`}>
              <rect
                width={NODE_W}
                height={NODE_H}
                rx={6}
                fill={c === 0 ? "#5b8def" : node.is_dataset ? "#232838" : "#131720"}
                stroke="#3a4256"
              />
              <text
                x={10}
                y={NODE_H / 2 + 4}
                fontSize={11}
                fill={c === 0 ? "#0b0e14" : "#e6e9ef"}
                fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace"
              >
                {truncate(node.node_id, 26)}
              </text>
            </g>
          ))
        )}
      </svg>
    </div>
  );
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
