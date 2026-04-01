/**
 * Displays Alternative Query Plans with cost comparison bars.
 * Uses log scale for bars when the cost range spans more than 10x.
 */
export default function AQPSection({ qep, aqps }) {
  if (!aqps || aqps.length === 0) return null;

  const qepCost = qep[0].Plan['Total Cost'];

  // Filter out AQPs with absurdly high costs (e.g. seqscan disabled on unindexed table)
  // for the bar chart, but keep them in the table
  const allCosts = [qepCost, ...aqps.map(a => a.total_cost)];
  const maxCost = Math.max(...allCosts);
  const useLog = maxCost / Math.min(...allCosts) > 10;

  function barWidth(cost) {
    if (useLog) {
      const logMin = Math.log10(Math.max(1, Math.min(...allCosts)));
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

  return (
    <div className="aqp-section">
      <h2>Alternative Query Plans (AQPs)</h2>

      <table className="aqp-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Disabled Operators</th>
            <th>Total Cost</th>
            <th>vs QEP</th>
          </tr>
        </thead>
        <tbody>
          <tr className="qep-row">
            <td>QEP</td>
            <td><em>None (original plan)</em></td>
            <td>{qepCost.toFixed(2)}</td>
            <td>--</td>
          </tr>
          {aqps.map((aqp, i) => {
            const diff = ((aqp.total_cost - qepCost) / qepCost * 100).toFixed(1);
            return (
              <tr key={i}>
                <td>{i + 1}</td>
                <td>{aqp.disabled_operators.join(', ')}</td>
                <td>{aqp.total_cost.toFixed(2)}</td>
                <td className={aqp.total_cost > qepCost ? 'cost-higher' : 'cost-lower'}>
                  {aqp.total_cost > qepCost ? '+' : ''}{diff}%
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      {useLog && (
        <div className="aqp-scale-note">Bar widths use logarithmic scale due to large cost differences.</div>
      )}

      <div className="aqp-bars">
        <div className="aqp-bar-item">
          <div className="bar-label">QEP (Original)</div>
          <div className="bar-container">
            <div
              className="bar bar-qep"
              style={{ width: `${barWidth(qepCost)}%` }}
            >
              {formatCost(qepCost)}
            </div>
          </div>
        </div>
        {aqps.map((aqp, i) => {
          const ratio = (aqp.total_cost / qepCost).toFixed(1);
          return (
            <div key={i} className="aqp-bar-item">
              <div className="bar-label">{aqp.description}</div>
              <div className="bar-container">
                <div
                  className="bar bar-aqp"
                  style={{ width: `${barWidth(aqp.total_cost)}%` }}
                >
                  {formatCost(aqp.total_cost)} ({ratio}x)
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
