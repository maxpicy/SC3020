/**
 * Recursive tree layout algorithm for QEP visualization.
 * Computes (x, y) positions for each node in a top-down tree.
 */

const NODE_W = 180;
const NODE_H = 72;
const H_GAP = 24;
const V_GAP = 48;

/**
 * Recursively compute widths bottom-up.
 */
function computeWidths(planNode) {
  const children = planNode.Plans || [];
  const childLayouts = children.map(c => computeWidths(c));

  let subtreeWidth;
  if (childLayouts.length === 0) {
    subtreeWidth = NODE_W;
  } else {
    subtreeWidth = childLayouts.reduce((sum, c) => sum + c.width, 0)
      + H_GAP * (childLayouts.length - 1);
    subtreeWidth = Math.max(subtreeWidth, NODE_W);
  }

  return {
    node: planNode,
    width: subtreeWidth,
    children: childLayouts,
  };
}

/**
 * Assign x positions top-down.
 */
function assignPositions(layout, x, y, depth, counter) {
  const id = counter.value++;
  const nodeX = x + layout.width / 2 - NODE_W / 2;
  const nodeY = y;

  const positioned = {
    id,
    x: nodeX,
    y: nodeY,
    w: NODE_W,
    h: NODE_H,
    cx: x + layout.width / 2,  // center x
    cy: nodeY + NODE_H / 2,    // center y
    node: layout.node,
    children: [],
  };

  let childX = x;
  for (const childLayout of layout.children) {
    const childPositioned = assignPositions(
      childLayout,
      childX,
      y + NODE_H + V_GAP,
      depth + 1,
      counter
    );
    positioned.children.push(childPositioned);
    childX += childLayout.width + H_GAP;
  }

  return positioned;
}

/**
 * Main entry: layout a QEP plan tree for SVG rendering.
 * @param {Object} planRoot - The raw QEP plan JSON (result.qep[0].Plan)
 * @returns {{ tree, totalWidth, totalHeight }} positioned tree and dimensions
 */
export function layoutTree(planRoot) {
  if (!planRoot) return { tree: null, totalWidth: 0, totalHeight: 0 };

  const widthLayout = computeWidths(planRoot);
  const counter = { value: 0 };
  const tree = assignPositions(widthLayout, 0, 0, 0, counter);

  // Compute total dimensions
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

export { NODE_W, NODE_H };
