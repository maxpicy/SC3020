import { useMemo, useRef, useEffect } from 'react';
import { layoutTree, NODE_W, NODE_H, VIRTUAL_W, wrapText, formatExpr } from '../utils/treeLayout';
import { getNodeColor, NODE_BORDER_COLORS } from '../utils/colors';

export default function QEPTreeView({
  qep,
  annotations,
  tableRowCounts,
  hoveredIdx,
  setHoveredIdx,
  selectedIdx,
  setSelectedIdx,
  setPinnedPos,
}) {
  const svgRef = useRef(null);
  const containerRef = useRef(null);

  const nodeIdToAnnIdx = useMemo(() => {
    const map = {};
    // First-wins: generate_annotations emits the primary scan/join/sort annotation
    // BEFORE the HAVING/filter annotations that share the same plan_node.id. Hovering
    // a plan node in the tree should highlight the primary span, not the later filter.
    annotations.forEach((ann, idx) => {
      if (ann.plan_node && ann.plan_node.id !== undefined && !(ann.plan_node.id in map)) {
        map[ann.plan_node.id] = idx;
      }
    });
    return map;
  }, [annotations]);

  const { tree, totalWidth, totalHeight } = useMemo(() => {
    if (!qep || !qep[0]) return { tree: null, totalWidth: 0, totalHeight: 0 };
    return layoutTree(qep[0].Plan, tableRowCounts);
  }, [qep, tableRowCounts]);

  useEffect(() => {
    if (selectedIdx === null || !containerRef.current) return;
    const sqlSpan = document.getElementById(`sql-span-${selectedIdx}`);
    if (sqlSpan) sqlSpan.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }, [selectedIdx]);

  if (!tree) return null;

  function truncate(text, maxChars = 28) {
    if (!text) return '';
    const s = String(text);
    return s.length > maxChars ? s.slice(0, maxChars - 1) + '\u2026' : s;
  }

  function formatCond(cond) {
    if (!cond) return '';
    let s = cond.replace(/::\w+(\(\d+\))?/g, '');
    while (s.startsWith('(') && s.endsWith(')')) {
      const inner = s.slice(1, -1);
      let depth = 0, ok = true;
      for (const c of inner) {
        if (c === '(') depth++;
        if (c === ')') depth--;
        if (depth < 0) { ok = false; break; }
      }
      if (ok && depth === 0) s = inner; else break;
    }
    return s.trim();
  }

  function renderEdges(node) {
    const edges = [];
    for (const child of node.children) {
      const x1 = node.cx;
      const y1 = node.y + node.h;
      const x2 = child.cx;
      const y2 = child.y;
      const midY = (y1 + y2) / 2;
      edges.push(
        <path
          key={`edge-${node.id}-${child.id}`}
          d={`M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2}`}
          fill="none"
          stroke="#999"
          strokeWidth={2}
        />
      );
      edges.push(...renderEdges(child));
    }
    return edges;
  }

  function renderVirtualNode(node) {
    const nodeType = node.node['Node Type'] || '';
    const detail = node.node._detail || '';
    const fillColor = getNodeColor(nodeType);
    const borderColor = NODE_BORDER_COLORS[nodeType] || '#666';
    const lines = wrapText(detail, 32);
    const isFilter = node.node._virtualType === 'filter';
    const outputRows = node.node._outputRows;
    const preFilterRows = node.node._preFilterRows;
    let rowsY = node.y + 28 + lines.length * 14;

    return (
      <g key={`vnode-${node.id}`} className="tree-node">
        <rect
          x={node.x}
          y={node.y}
          width={node.w}
          height={node.h}
          rx={8}
          ry={8}
          fill={fillColor}
          stroke={borderColor}
          strokeWidth={1.5}
        />
        <text x={node.cx} y={node.y + 16} textAnchor="middle" className="node-type">
          {nodeType}
        </text>
        {lines.map((line, i) => (
          <text
            key={`vline-${i}`}
            x={node.cx}
            y={node.y + 30 + i * 14}
            textAnchor="middle"
            className="node-detail"
            fill="#444"
          >
            {line}
          </text>
        ))}
        {isFilter && preFilterRows != null && (
          <text x={node.cx} y={rowsY + 4} textAnchor="middle" className="node-cost">
            {preFilterRows.toLocaleString()} rows → {outputRows.toLocaleString()} rows
          </text>
        )}
        {isFilter && preFilterRows == null && outputRows != null && (
          <text x={node.cx} y={rowsY + 4} textAnchor="middle" className="node-cost">
            est. {outputRows.toLocaleString()} rows after filter
          </text>
        )}
      </g>
    );
  }

  function renderRealNode(node) {
    // Use _planId from the raw QEP (set by tagPlanNodeIds), not the positioned
    // layout id. The layout counter includes virtual Projection/Filter wrappers
    // and no longer matches the backend's plan_node.id.
    const annIdx = nodeIdToAnnIdx[node.node._planId];
    const isHovered = annIdx !== undefined && hoveredIdx === annIdx;
    const isSelected = annIdx !== undefined && selectedIdx === annIdx;
    const rawNodeType = node.node['Node Type'] || '';
    // Surface Postgres' "Partial Mode" (Partial / Finalize) on aggregate/sort
    // nodes that run under a parallel Gather. Otherwise the user sees two
    // identical "Aggregate" boxes and the QEP looks like it's double-grouping.
    const partialMode = node.node['Partial Mode'] || '';
    const displayNodeType =
      partialMode === 'Partial'  ? `Partial ${rawNodeType}` :
      partialMode === 'Finalize' ? `Finalize ${rawNodeType}` :
      rawNodeType;
    const fillColor = getNodeColor(rawNodeType);
    const strokeColor = isHovered || isSelected ? '#1565C0' : '#666';
    const strokeWidth = isHovered || isSelected ? 3 : 1;

    const relName = node.node['Relation Name'] || '';
    const alias = node.node['Alias'] || '';
    const cost = node.node['Total Cost'] ?? 0;
    const rows = node.node['Plan Rows'] ?? 0;
    const hasFilter = !!node.node['Filter'];
    const hashCond = node.node['Hash Cond'] || '';
    const mergeCond = node.node['Merge Cond'] || '';
    const joinFilter = node.node['Join Filter'] || '';
    const indexCond = node.node['Index Cond'] || '';
    const sortKey = node.node['Sort Key'] || null;
    const groupKey = node.node['Group Key'] || null;
    const joinCond = hashCond || mergeCond || joinFilter;
    const preFilterRows = relName && tableRowCounts && tableRowCounts[relName]
      ? tableRowCounts[relName] : null;

    const displayName = relName
      ? `${relName}${alias && alias !== relName ? ` (${alias})` : ''}`
      : '';

    let detailY = node.y + 68 + (hasFilter && preFilterRows != null ? 14 : 0);
    const detailLines = [];

    if (joinCond) {
      const condText = formatCond(joinCond);
      for (const line of wrapText('cond: ' + condText, 34)) {
        detailLines.push({ text: line, color: '#2E7D32' });
      }
    }
    if (indexCond) {
      const idxText = formatCond(indexCond);
      for (const line of wrapText('idx: ' + idxText, 34)) {
        detailLines.push({ text: line, color: '#1565C0' });
      }
    }
    if (sortKey) {
      const sortText = sortKey.map(formatExpr).join(', ');
      for (const line of wrapText('sort: ' + sortText, 34)) {
        detailLines.push({ text: line, color: '#E65100' });
      }
    }
    if (groupKey) {
      const groupText = groupKey.map(formatExpr).join(', ');
      for (const line of wrapText('group: ' + groupText, 34)) {
        detailLines.push({ text: line, color: '#6A1B9A' });
      }
    }

    return (
      <g
        key={`node-${node.id}`}
        id={annIdx !== undefined ? `tree-node-${annIdx}` : undefined}
        className={`tree-node ${isHovered ? 'hovered' : ''} ${isSelected ? 'selected' : ''}`}
        style={{ cursor: annIdx !== undefined ? 'pointer' : 'default' }}
        onMouseEnter={() => annIdx !== undefined && setHoveredIdx(annIdx)}
        onMouseLeave={() => setHoveredIdx(null)}
        onClick={() => {
          if (annIdx === undefined) return;
          const unpin = selectedIdx === annIdx;
          setSelectedIdx(unpin ? null : annIdx);
          if (!setPinnedPos) return;
          if (unpin) {
            setPinnedPos(null);
            return;
          }
          // Anchor the pinned tooltip on the corresponding SQL span (the tooltip
          // lives in the SQL view's container, so we measure relative to that).
          const sqlSpan = document.getElementById(`sql-span-${annIdx}`);
          const sqlContainer = document.querySelector('.sql-annotated-view');
          if (sqlSpan && sqlContainer) {
            const r = sqlSpan.getBoundingClientRect();
            const cr = sqlContainer.getBoundingClientRect();
            setPinnedPos({ x: r.left - cr.left, y: r.bottom - cr.top + 8 });
          }
        }}
      >
        <rect
          x={node.x}
          y={node.y}
          width={node.w}
          height={node.h}
          rx={8}
          ry={8}
          fill={fillColor}
          stroke={strokeColor}
          strokeWidth={strokeWidth}
        />
        <text x={node.cx} y={node.y + 18} textAnchor="middle" className="node-type">
          {displayNodeType}
        </text>
        {displayName && (
          <text x={node.cx} y={node.y + 34} textAnchor="middle" className="node-rel">
            {displayName}
          </text>
        )}
        <text x={node.cx} y={node.y + 50} textAnchor="middle" className="node-cost">
          cost: {cost.toFixed(1)} | rows: {rows.toLocaleString()}
        </text>
        {hasFilter && preFilterRows != null && (
          <text x={node.cx} y={node.y + 62} textAnchor="middle" className="node-rows">
            (scans {preFilterRows.toLocaleString()} rows, filter applied during scan)
          </text>
        )}

        {detailLines.map((dl, i) => (
          <text
            key={`detail-${i}`}
            x={node.x + 6}
            y={detailY + i * 14}
            className="node-detail"
            fill={dl.color}
          >
            {dl.text}
          </text>
        ))}
      </g>
    );
  }

  function renderNodes(node) {
    const elements = [];

    if (node.node._virtual) {
      elements.push(renderVirtualNode(node));
    } else {
      elements.push(renderRealNode(node));
    }

    for (const child of node.children) {
      elements.push(...renderNodes(child));
    }
    return elements;
  }

  return (
    <div className="qep-tree-view" ref={containerRef}>
      <h2>Query Execution Plan (QEP)</h2>
      <div className="qep-cost-summary">
        Total Cost: <strong>{qep[0].Plan['Total Cost'].toFixed(2)}</strong>
        {' | '}Estimated Rows: <strong>{qep[0].Plan['Plan Rows'].toLocaleString()}</strong>
      </div>
      <div className="tree-scroll-container">
        <svg
          ref={svgRef}
          width={totalWidth}
          height={totalHeight}
          viewBox={`0 0 ${totalWidth} ${totalHeight}`}
        >
          {renderEdges(tree)}
          {renderNodes(tree)}
        </svg>
      </div>
    </div>
  );
}
