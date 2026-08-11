import type { Task } from "../api/types";
import { EmptyState } from "./EmptyState";

interface Props {
  tasks: Task[];
  selectedTaskId?: string;
  onSelect?: (task: Task) => void;
}

export function DagView({ tasks, selectedTaskId, onSelect }: Props) {
  const ordered = [...tasks].sort((a, b) => a.task_order - b.task_order);
  if (ordered.length === 0) {
    return <EmptyState icon="⛓" title="No tasks yet" subtitle="Tasks will appear here once the job defines its DAG." />;
  }
  return (
    <div className="dag-row">
      {ordered.map((task, i) => (
        <div key={task.task_id} style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <button
            type="button"
            className={`dag-node${task.task_id === selectedTaskId ? " selected" : ""}`}
            style={{ margin: 0, cursor: onSelect ? "pointer" : "default" }}
            onClick={() => onSelect?.(task)}
          >
            {task.name} <span className="muted">({task.exec_type})</span>
          </button>
          {i < ordered.length - 1 && <span className="dag-arrow">&rarr;</span>}
        </div>
      ))}
    </div>
  );
}
