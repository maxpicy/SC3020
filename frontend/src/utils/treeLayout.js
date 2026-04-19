const NODE_W = 220;
const VIRTUAL_W = 220;
const NODE_H_BASE = 72;
const NODE_H_LINE = 14;
const H_GAP = 24;
const V_GAP = 36;

function nodeDetailLines(planNode) {
  let lines = 0;
  if (planNode['Filter'] && planNode['Relation Name']) lines += 1;
  const cond = planNode['Hash Cond'] || planNode['Merge Cond'] || planNode['Join Filter'] || '';
  if (cond) lines += wrapText('cond: ' + cleanCond(cond), 34).length;
  const idx = planNode['Index Cond'] || '';
  if (idx) lines += wrapText('idx: ' + cleanCond(idx), 34).length;
  const sortKey = planNode['Sort Key'];
  if (sortKey) lines += wrapText('sort: ' + sortKey.map(formatExpr).join(', '), 34).length;
  const groupKey = planNode['Group Key'];
  if (groupKey) lines += wrapText('group: ' + groupKey.map(formatExpr).join(', '), 34).length;
  return lines;
}

function getNodeHeight(planNode) {
  if (planNode._virtual) {
    const detail = planNode._detail || '';
    const lines = wrapText(detail, 32).length;
    const isFilter = planNode._virtualType === 'filter';
    const rowsLineCount = isFilter ? (planNode._preFilterRows ? 2 : 1) : 0;
    return 24 + Math.max(1, lines) * 14 + rowsLineCount * 14 + 8;
  }
  return NODE_H_BASE + nodeDetailLines(planNode) * NODE_H_LINE;
}

function getNodeWidth(planNode) {
  return planNode._virtual ? VIRTUAL_W : NODE_W;
}

function wrapText(text, maxChars) {
  if (!text || text.length <= maxChars) return [text || ''];
  const words = text.split(/(\s+|,\s*)/);
  const lines = [];
  let current = '';
  for (const word of words) {
    if (current.length + word.length > maxChars && current.length > 0) {
      lines.push(current.trimEnd());
      current = word.trimStart();
    } else {
      current += word;
    }
  }
  if (current.trim()) lines.push(current.trimEnd());
  return lines.length > 0 ? lines : [''];
}

// Mirrors formatOutputCols but for sort_key / group_key, which Postgres emits
// as expression strings rather than column lists.
function formatExpr(expr) {
  if (!expr) return '';
  const clean = String(expr).replace(/::\w+(\(\d+\))?/g, '').trim();
  // Split off trailing DESC/ASC so it survives summarisation.
  const direction = /\s+(DESC|ASC)\b\s*$/i.exec(clean);
  const body = direction ? clean.slice(0, direction.index).trim() : clean;
  const suffix = direction ? ' ' + direction[1].toUpperCase() : '';

  const simple = body.match(/^(\w+)\.(\w+)$/);
  if (simple) return simple[2] + suffix;

  const agg = body.match(/^\(?\s*(PARTIAL\s+|FINALIZE\s+)?(sum|avg|count|min|max|stddev|variance|array_agg|string_agg|bool_and|bool_or|every)\s*\(/i);
  if (agg) {
    const prefix = agg[1] ? agg[1].trim().toLowerCase() + ' ' : '';
    return prefix + agg[2].toLowerCase() + '(...)' + suffix;
  }

  const collapsed = body.replace(/\s+/g, ' ');
  const compact = collapsed.length <= 24 ? collapsed : collapsed.slice(0, 21) + '…';
  return compact + suffix;
}

function cleanCond(cond) {
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

function formatOutputCols(output) {
  if (!output || output.length === 0) return '';
  const cols = output.map(c => {
    const clean = c.replace(/::\w+(\(\d+\))?/g, '').trim();

    const simple = clean.match(/^(\w+)\.(\w+)$/);
    if (simple) return simple[2];

    // Aggregate with optional PARTIAL/FINALIZE parallel markers: render as
    // `sum(...)` so the tree shows the operation without the expression body.
    const agg = clean.match(/^\(?\s*(PARTIAL\s+|FINALIZE\s+)?(sum|avg|count|min|max|stddev|variance|array_agg|string_agg|bool_and|bool_or|every)\s*\(/i);
    if (agg) {
      const prefix = agg[1] ? agg[1].trim().toLowerCase() + ' ' : '';
      return prefix + agg[2].toLowerCase() + '(...)';
    }

    // Arithmetic / CASE / coalesce / etc: collapse whitespace and truncate so
    // unbalanced parens don't leak into the tree rendering.
    const collapsed = clean.replace(/\s+/g, ' ');
    if (collapsed.length <= 24) return collapsed;
    return collapsed.slice(0, 21) + '…';
  });
  return cols.join(', ');
}

function injectVirtualNodes(planNode) {
  const node = { ...planNode };
  const myOutput = node['Output'] || [];
  const children = node.Plans || [];
  const rowCounts = node._tableRowCounts || {};
  for (const child of children) {
    child._parentOutput = myOutput;
    child._tableRowCounts = rowCounts;
  }
  node.Plans = children.map(c => injectVirtualNodes(c));

  let result = node;

  if (node['Filter']) {
    const relName = node['Relation Name'] || '';
    const preFilterRows = relName && rowCounts[relName] ? rowCounts[relName] : null;
    const filterNode = {
      'Node Type': 'Filter',
      'Total Cost': 0,
      'Plan Rows': node['Plan Rows'] || 0,
      '_virtual': true,
      '_virtualType': 'filter',
      '_detail': cleanCond(node['Filter']),
      '_outputRows': node['Plan Rows'] || 0,
      '_preFilterRows': preFilterRows,
      'Plans': [result],
    };
    result = filterNode;
  }

  // Only show projection when columns are actually narrowed
  const output = node['Output'] || [];
  const nodeType = node['Node Type'] || '';
  const SKIP_PROJ = ['Hash', 'Materialize', 'Memoize', 'Gather', 'Gather Merge', 'Filter', 'Projection'];
  const SCAN_TYPES = ['Seq Scan', 'Index Scan', 'Index Only Scan', 'Bitmap Heap Scan'];

  if (output.length > 0 && !SKIP_PROJ.includes(nodeType)) {
    const realChildren = (node.Plans || []).filter(c => !c._virtual);
    const childOutputs = realChildren.flatMap(c => c['Output'] || []);

    const isScan = SCAN_TYPES.includes(nodeType);
    const reducesColumns = childOutputs.length > 0 && output.length < childOutputs.length;
    const isRoot = planNode._isRoot;

    // Don't wrap scans in a Projection. A Seq/Index Scan's Output is the raw
    // heap tuple, not a narrowing step. Real narrowing happens at the Hash Join
    // (or Hash) above and is already shown via the reducesColumns case. Rendering
    // a Projection above a scan was misleading students into thinking Postgres
    // wasn't pushing projection down.
    let showProjection = false;
    if (isRoot) {
      showProjection = true;
    } else if (reducesColumns && !isScan) {
      showProjection = true;
    }

    if (showProjection) {
      const projNode = {
        'Node Type': 'Projection',
        'Total Cost': 0,
        'Plan Rows': node['Plan Rows'] || 0,
        '_virtual': true,
        '_virtualType': 'projection',
        '_detail': formatOutputCols(output),
        'Plans': [result],
      };
      result = projNode;
    }
  }

  return result;
}

function computeWidths(planNode) {
  const children = planNode.Plans || [];
  const childLayouts = children.map(c => computeWidths(c));

  const nodeH = getNodeHeight(planNode);
  const nodeW = getNodeWidth(planNode);

  let subtreeWidth;
  if (childLayouts.length === 0) {
    subtreeWidth = nodeW;
  } else {
    subtreeWidth = childLayouts.reduce((sum, c) => sum + c.width, 0)
      + H_GAP * (childLayouts.length - 1);
    subtreeWidth = Math.max(subtreeWidth, nodeW);
  }

  return {
    node: planNode,
    width: subtreeWidth,
    nodeWidth: nodeW,
    height: nodeH,
    children: childLayouts,
  };
}

function assignPositions(layout, x, y, depth, counter) {
  const id = counter.value++;
  const nodeH = layout.height;
  const nodeW = layout.nodeWidth;
  const nodeX = x + layout.width / 2 - nodeW / 2;
  const nodeY = y;

  const positioned = {
    id,
    x: nodeX,
    y: nodeY,
    w: nodeW,
    h: nodeH,
    cx: x + layout.width / 2,
    cy: nodeY + nodeH / 2,
    node: layout.node,
    children: [],
  };

  let childX = x;
  for (const childLayout of layout.children) {
    const childPositioned = assignPositions(
      childLayout,
      childX,
      y + nodeH + V_GAP,
      depth + 1,
      counter
    );
    positioned.children.push(childPositioned);
    childX += childLayout.width + H_GAP;
  }

  return positioned;
}

// Tag raw plan nodes in the same pre-order the backend uses
// (preprocessing.walk_plan_tree). Hovering a QEP node must map to the right
// annotation even when virtual Projection/Filter wrappers shift the layout counter.
function tagPlanNodeIds(planNode, counter) {
  planNode._planId = counter.value++;
  for (const child of (planNode.Plans || [])) {
    tagPlanNodeIds(child, counter);
  }
}

export function layoutTree(planRoot, tableRowCounts) {
  if (!planRoot) return { tree: null, totalWidth: 0, totalHeight: 0 };

  planRoot._isRoot = true;
  planRoot._tableRowCounts = tableRowCounts || {};
  tagPlanNodeIds(planRoot, { value: 0 });
  const enriched = injectVirtualNodes(planRoot);
  const widthLayout = computeWidths(enriched);
  const counter = { value: 0 };
  const tree = assignPositions(widthLayout, 0, 0, 0, counter);

  function getMaxBounds(node) {
    let maxX = node.x + node.w;
    let maxY = node.y + node.h;
    for (const child of node.children) {
      const bounds = getMaxBounds(child);
      maxX = Math.max(maxX, bounds.maxX);
      maxY = Math.max(maxY, bounds.maxY);
    }
    return { maxX, maxY };
  }

  const bounds = getMaxBounds(tree);
  return {
    tree,
    totalWidth: bounds.maxX + 20,
    totalHeight: bounds.maxY + 20,
  };
}

export { NODE_W, NODE_H_BASE as NODE_H, VIRTUAL_W, wrapText, formatExpr };
