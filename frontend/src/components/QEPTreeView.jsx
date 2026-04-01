import { useMemo, useRef, useEffect, useState } from 'react';
import { layoutTree, NODE_W, NODE_H } from '../utils/treeLayout';
import { getNodeColor } from '../utils/colors';

/**
 * SVG-based QEP tree visualization.
 * Nodes are color-coded and interactive (hover/click syncs with SQL view).
 */
export default function QEPTreeView({
  qep,
  annotations,
  hoveredIdx,
  setHoveredIdx,
  selectedIdx,
  setSelectedIdx,
}) {
  const svgRef = useRef(null);
  const containerRef = useRef(null);

  // Build node-id to annotation-index map
  const nodeIdToAnnIdx = useMemo(() => {
    const map = {};
    annotations.forEach((ann, idx) => {
      if (ann.plan_node && ann.plan_node.id !== undefined) {
        map[ann.plan_node.id] = idx;
      }
    });
    return map;
  }, [annotations]);

  // Layout the tree
  const { tree, totalWidth, totalHeight } = useMemo(() => {
    if (!qep || !qep[0]) return { tree: null, totalWidth: 0, totalHeight: 0 };
    return layoutTree(qep[0].Plan);
  }, [qep]);

  // Draw connection arrow from selected tree node to SQL span
  const [arrowPath, setArrowPath] = useState(null);

  useEffect(() => {
    if (selectedIdx === null || !containerRef.current) {
      setArrowPath(null);
      return;
    }

    const treeNode = document.getElementById(`tree-node-${selectedIdx}`);
    const sqlSpan = document.getElementById(`sql-span-${selectedIdx}`);
    if (!treeNode || !sqlSpan) {
      setArrowPath(null);
      return;
    }

    // Scroll the SQL span into view
    sqlSpan.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

    setArrowPath(null); // Arrows are handled via synchronized highlighting
  }, [selectedIdx]);

  if (!tree) return null;

  // Render edges recursively
  function renderEdges(node) {
    const edges = [];
    for (const child of node.children) {
      const x1 = node.cx;
      const y1 = node.y + NODE_H;
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

  // Render nodes recursively
  function renderNodes(node) {
    const annIdx = nodeIdToAnnIdx[node.id];
    const isHovered = annIdx !== undefined && hoveredIdx === annIdx;
    const isSelected = annIdx !== undefined && selectedIdx === annIdx;
    const fillColor = getNodeColor(node.node.hasOwnProperty('Node Type') ? node.node['Node Type'] : '');
    const strokeColor = isHovered || isSelected ? '#1565C0' : '#666';
    const strokeWidth = isHovered || isSelected ? 3 : 1;

    const nodeType = node.node['Node Type'] || '';
    const relName = node.node['Relation Name'] || '';
    const alias = node.node['Alias'] || '';
    const cost = node.node['Total Cost'] || 0;
    const rows = node.node['Plan Rows'] || 0;

    const displayName = relName
      ? `${relName}${alias && alias !== relName ? ` (${alias})` : ''}`
      : '';

    const nodes = [
      <g
        key={`node-${node.id}`}
        id={annIdx !== undefined ? `tree-node-${annIdx}` : undefined}
        className={`tree-node ${isHovered ? 'hovered' : ''} ${isSelected ? 'selected' : ''}`}
        style={{ cursor: annIdx !== undefined ? 'pointer' : 'default' }}
        onMouseEnter={() => annIdx !== undefined && setHoveredIdx(annIdx)}
        onMouseLeave={() => setHoveredIdx(null)}
        onClick={() => annIdx !== undefined && setSelectedIdx(selectedIdx === annIdx ? null : annIdx)}
      >
        <rect
          x={node.x}
          y={node.y}
          width={NODE_W}
          height={NODE_H}
          rx={8}
          ry={8}
          fill={fillColor}
          stroke={strokeColor}
          strokeWidth={strokeWidth}
        />
        <text x={node.cx} y={node.y + 18} textAnchor="middle" className="node-type">
          {nodeType}
        </text>
        {displayName && (
          <text x={node.cx} y={node.y + 34} textAnchor="middle" className="node-rel">
            {displayName}
          </text>
        )}
        <text x={node.cx} y={node.y + 50} textAnchor="middle" className="node-cost">
          cost: {cost.toFixed(1)}
        </text>
        <text x={node.cx} y={node.y + 64} textAnchor="middle" className="node-rows">
          rows: {rows.toLocaleString()}
        </text>
      </g>,
    ];

    for (const child of node.children) {
      nodes.push(...renderNodes(child));
    }
    return nodes;
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
