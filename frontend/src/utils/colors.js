export const COMPONENT_COLORS = {
  scan:      { bg: '#BBDEFB', border: '#1976D2', label: 'Scan' },
  join:      { bg: '#C8E6C9', border: '#388E3C', label: 'Join' },
  sort:      { bg: '#FFE0B2', border: '#F57C00', label: 'Sort' },
  aggregate: { bg: '#E1BEE7', border: '#7B1FA2', label: 'Aggregate' },
  groupby:   { bg: '#D1C4E9', border: '#512DA8', label: 'Group By' },
  limit:     { bg: '#FFECB3', border: '#FFA000', label: 'Limit' },
  distinct:  { bg: '#B2EBF2', border: '#0097A7', label: 'Distinct' },
  having:    { bg: '#F8BBD0', border: '#C2185B', label: 'Having' },
  filter:    { bg: '#FFCCBC', border: '#E64A19', label: 'Filter' },
  subquery:  { bg: '#DCEDC8', border: '#689F38', label: 'Subquery' },
  optimizer_introduced: { bg: '#F5F5F5', border: '#9E9E9E', label: 'Optimizer' },
};

export const NODE_COLORS = {
  'Seq Scan':         '#BBDEFB',
  'Index Scan':       '#90CAF9',
  'Index Only Scan':  '#90CAF9',
  'Bitmap Heap Scan': '#64B5F6',
  'Hash Join':        '#C8E6C9',
  'Merge Join':       '#A5D6A7',
  'Nested Loop':      '#81C784',
  'Sort':             '#FFE0B2',
  'HashAggregate':    '#E1BEE7',
  'GroupAggregate':   '#CE93D8',
  'Aggregate':        '#CE93D8',
  'Limit':            '#FFECB3',
  'Unique':           '#B2EBF2',
  'Hash':             '#E0E0E0',
  'Materialize':      '#E0E0E0',
  'Memoize':          '#E0E0E0',
  'Gather':           '#F5F5F5',
  'Gather Merge':     '#F5F5F5',
  'Filter':           '#FFCCBC',
  'Projection':       '#DCEDC8',
};

export const NODE_BORDER_COLORS = {
  'Filter':     '#E64A19',
  'Projection': '#689F38',
};

export function getNodeColor(nodeType) {
  return NODE_COLORS[nodeType] || '#E0E0E0';
}

export function getComponentColor(type) {
  return COMPONENT_COLORS[type] || COMPONENT_COLORS.optimizer_introduced;
}
