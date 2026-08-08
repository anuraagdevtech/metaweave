import type { Task } from "../api/types";

export function DagView({ tasks }: { tasks: Task[] }) {
  const ordered = [...tasks].sort((a, b) => a.task_order - b.task_order);
  return (
    <div>
      {ordered.map((task, i) => (
        <div className="dag-row" key={task.task_id}>
          <div className="dag-node">
            {task.name} <span className="muted">({task.exec_type})</span>
          </div>
          {i < ordered.length - 1 && <span className="dag-arrow">&rarr;</span>}
        </div>
      ))}
      {ordered.length === 0 && <p className="muted">No tasks yet.</p>}
    </div>
  );
}
