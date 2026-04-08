export default function AQPSection({ qep, aqps, qepOperators }) {
  if (!aqps || aqps.length === 0) return null;

  const qepCost = qep[0].Plan['Total Cost'];

  const allCosts = [qepCost, ...aqps.map(a => a.total_cost)];
  const maxCost = Math.max(...allCosts);
  const minCost = Math.min(...allCosts);
  const useLog = maxCost / minCost > 10;

  function barWidth(cost) {
    if (useLog) {
      const logMin = Math.log10(Math.max(1, minCost));
      const logMax = Math.log10(Math.max(1, maxCost));
      const logCost = Math.log10(Math.max(1, cost));
      const range = logMax - logMin || 1;
      return Math.max(5, ((logCost - logMin) / range) * 100);
    }
    return Math.max(5, (cost / maxCost) * 100);
  }

  function formatCost(cost) {
    if (cost >= 1e6) return (cost / 1e6).toFixed(1) + 'M';
    if (cost >= 1e3) return (cost / 1e3).toFixed(1) + 'K';
    return cost.toFixed(0);
  }

  const OP_MAP = {
    'enable_hashjoin': 'Hash Join',
    'enable_mergejoin': 'Merge Join',
    'enable_nestloop': 'Nested Loop',
    'enable_seqscan': 'Seq Scan',
    'enable_indexscan': 'Index Scan',
    'enable_indexonlyscan': 'Index Only Scan',
    'enable_bitmapscan': 'Bitmap Scan',
    'enable_sort': 'Sort',
    'enable_hashagg': 'Hash Aggregate',
    'enable_material': 'Materialize',
    'enable_memoize': 'Memoize',
  };

  function formatOperator(op) {
    return OP_MAP[op] || op;
  }

  function formatDisabledLabel(aqp) {
    const ops = aqp.disabled_operators.map(formatOperator);
    if (ops.length === 1) return `Without ${ops[0]}`;
    return `Without ${ops.slice(0, -1).join(', ')} & ${ops[ops.length - 1]}`;
  }

  // Within 0.1% counts as same cost
  function isSameCost(aqpCost) {
    return Math.abs(aqpCost - qepCost) / qepCost < 0.001;
  }

  function getSameCostExplanation(aqp) {
    const disabledOps = aqp.disabled_operators.map(formatOperator);
    const qepOps = qepOperators || [];
    const notUsedByQep = disabledOps.filter(op => !qepOps.includes(op));
    if (notUsedByQep.length > 0) {
      return `Same cost because the QEP already uses ${qepOps.filter(o => !['Seq Scan', 'Sort', 'HashAggregate', 'Materialize'].includes(o)).join(', ') || qepOps.join(', ')} - disabling ${notUsedByQep.join(' and ')} has no effect.`;
    }
    return 'Same plan selected despite disabling these operators.';
  }

  const qepOps = qepOperators || [];
  const joinOps = qepOps.filter(o => ['Hash Join', 'Merge Join', 'Nested Loop'].includes(o));
  const scanOps = qepOps.filter(o => ['Seq Scan', 'Index Scan', 'Index Only Scan', 'Bitmap Heap Scan'].includes(o));
  const otherOps = qepOps.filter(o => !joinOps.includes(o) && !scanOps.includes(o));

  return (
    <div className="aqp-section">
      <h2>Alternative Query Plans (AQPs)</h2>

      <div className="aqp-selected-note">
        <strong>QEP (Selected Plan):</strong>{' '}
        The optimizer selected a plan using{' '}
        {joinOps.length > 0 && <><strong>{joinOps.join(', ')}</strong> for joins</>}
        {joinOps.length > 0 && scanOps.length > 0 && ' and '}
        {scanOps.length > 0 && <><strong>{scanOps.join(', ')}</strong> for table access</>}
        {otherOps.length > 0 && <>, with <strong>{otherOps.join(', ')}</strong></>}
        {' '}with a total estimated cost of <strong>{qepCost.toFixed(2)}</strong>.
        The alternatives below show how cost changes when certain operators are disabled.
      </div>

      <table className="aqp-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Plan</th>
            <th>Operators Used</th>
            <th>Total Cost</th>
            <th>vs QEP</th>
          </tr>
        </thead>
        <tbody>
          <tr className="qep-row">
            <td className="qep-badge-cell"><span className="qep-badge">Selected</span></td>
            <td>QEP &mdash; Original plan</td>
            <td>{qepOps.join(', ')}</td>
            <td>{qepCost.toFixed(2)}</td>
            <td>&mdash;</td>
          </tr>
          {aqps.map((aqp, i) => {
            const diff = ((aqp.total_cost - qepCost) / qepCost * 100).toFixed(1);
            const sameCost = isSameCost(aqp.total_cost);
            return (
              <tr key={i} className={sameCost ? 'aqp-same-cost-row' : ''}>
                <td>{i + 1}</td>
                <td>
                  {aqp.disabled_operators.map(formatOperator).join(', ')} disabled
                  {sameCost && (
                    <div className="aqp-same-cost-hint">
                      {getSameCostExplanation(aqp)}
                    </div>
                  )}
                </td>
                <td className="aqp-ops-cell">
                  {aqp.nodes && aqp.nodes.length > 0 && (() => {
                    const aqpNodeTypes = new Set();
                    const collectOps = (nodes) => {
                      for (const n of nodes) {
                        if (n.node_type && !['Hash', 'Materialize', 'Memoize', 'Gather', 'Gather Merge'].includes(n.node_type)) {
                          aqpNodeTypes.add(n.node_type);
                        }
                        if (n.children) collectOps(n.children);
                      }
                    };
                    collectOps(aqp.nodes);
                    const ops = [...aqpNodeTypes];
                    const changed = ops.filter(o => !qepOps.includes(o));
                    if (changed.length > 0) {
                      return changed.map(o => (
                        <span key={o} className="aqp-changed-op">{o}</span>
                      ));
                    }
                    return <span className="aqp-unchanged">Same operators</span>;
                  })()}
                </td>
                <td>{aqp.total_cost.toFixed(2)}</td>
                <td className={sameCost ? 'cost-same' : aqp.total_cost > qepCost ? 'cost-higher' : 'cost-lower'}>
                  {sameCost ? '0%' : `${aqp.total_cost > qepCost ? '+' : ''}${diff}%`}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <h3 className="aqp-chart-heading">Cost Comparison</h3>

      {useLog && (
        <div className="aqp-scale-note">Bar widths use logarithmic scale due to large cost differences.</div>
      )}

      <div className="aqp-bars">
        <div className="aqp-bar-row">
          <div className="bar-label">QEP ({joinOps[0] || qepOps[0] || 'Selected'})</div>
          <div className="bar-track">
            <div
              className="bar bar-qep"
              style={{ width: `${barWidth(qepCost)}%` }}
            />
            <span className="bar-value">{formatCost(qepCost)}</span>
          </div>
        </div>
        {aqps.map((aqp, i) => {
          const ratio = (aqp.total_cost / qepCost).toFixed(1);
          const sameCost = isSameCost(aqp.total_cost);
          return (
            <div key={i} className="aqp-bar-row">
              <div className="bar-label">{formatDisabledLabel(aqp)}</div>
              <div className="bar-track">
                <div
                  className={`bar ${sameCost ? 'bar-same' : 'bar-aqp'}`}
                  style={{ width: `${barWidth(aqp.total_cost)}%` }}
                />
                <span className="bar-value">
                  {formatCost(aqp.total_cost)} ({ratio}x)
                  {sameCost && ' - no change'}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
