import { getComponentColor } from '../utils/colors';

export default function AnnotationTooltip({ annotation, position }) {
  if (!annotation || !position) return null;

  const color = getComponentColor(annotation.component.component_type);

  return (
    <div
      className="annotation-tooltip"
      style={{
        left: position.x,
        top: position.y,
      }}
    >
      <div className="tooltip-header">
        <span
          className="tooltip-badge"
          style={{ background: color.bg, borderColor: color.border }}
        >
          {color.label}
        </span>
        <span className="tooltip-node-type">{annotation.plan_node.node_type}</span>
      </div>

      <div className="tooltip-section">
        <strong>How:</strong>
        <p>{annotation.how}</p>
      </div>

      {annotation.why && (
        <div className="tooltip-section">
          <strong>Why:</strong>
          <p>{annotation.why}</p>
        </div>
      )}

      <div className="tooltip-cost">
        <span>Cost: <strong>{annotation.qep_cost.toFixed(2)}</strong></span>
        {annotation.plan_node.plan_rows > 0 && (
          <span> | Rows: <strong>{annotation.plan_node.plan_rows.toLocaleString()}</strong></span>
        )}
      </div>

      {Object.keys(annotation.alternative_costs).length > 0 && (
        <div className="tooltip-alternatives">
          <strong>Alternatives:</strong>
          <ul>
            {Object.entries(annotation.alternative_costs).map(([op, cost]) => (
              <li key={op}>
                {op}: {cost.toFixed(2)}
                <span className="alt-ratio">
                  ({(cost / annotation.qep_cost).toFixed(1)}x)
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
