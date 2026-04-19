import re
from dataclasses import dataclass, field
from preprocessing import (
    SQLComponent, PlanNode, AQPResult,
    get_qep, get_targeted_aqps, get_table_indexes,
    walk_plan_tree, get_tables_in_subtree, normalize_condition,
    conditions_match, find_corresponding_node, parse_query,
    OPERATOR_TO_OPTION, SKIP_NODE_TYPES, JOIN_NODE_TYPES,
    SCAN_NODE_TYPES, AGGREGATE_NODE_TYPES, JOIN_OPTIONS,
)


@dataclass
class Annotation:
    component: SQLComponent
    plan_node: PlanNode
    how: str
    why: str
    qep_cost: float = 0.0
    alternative_costs: dict = field(default_factory=dict)


def generate_how(node):
    nt = node.node_type

    if nt == "Seq Scan":
        text = f"Table '{node.relation_name}' is accessed using sequential scan (reads all rows)."
        if node.filter_cond:
            text += f" Rows are filtered by: {node.filter_cond}."
        return text

    elif nt == "Index Scan":
        text = f"Table '{node.relation_name}' is accessed via index scan on '{node.index_name}'"
        if node.index_cond:
            text += f" with condition {node.index_cond}"
        text += "."
        if node.filter_cond:
            text += f" Additional filter applied: {node.filter_cond}."
        return text

    elif nt == "Index Only Scan":
        text = f"Table '{node.relation_name}' is accessed via index-only scan on '{node.index_name}' (no heap access needed)."
        if node.index_cond:
            text += f" Condition: {node.index_cond}."
        return text

    elif nt == "Bitmap Heap Scan":
        text = f"Table '{node.relation_name}' is accessed via bitmap scan."
        recheck = node.raw.get("Recheck Cond", "")
        if recheck:
            text += f" Recheck condition: {recheck}."
        if node.filter_cond:
            text += f" Additional filter: {node.filter_cond}."
        return text

    elif nt == "Hash Join":
        cond = node.join_cond or "unknown condition"
        jtype = node.join_type or "Inner"
        text = f"This join is implemented using hash join ({jtype} Join)."
        text += f" The inner relation is hashed, then the outer relation probes the hash table on condition {cond}."
        return text

    elif nt == "Merge Join":
        cond = node.join_cond or "unknown condition"
        jtype = node.join_type or "Inner"
        text = f"This join is implemented using merge join ({jtype} Join)."
        text += f" Both inputs are sorted on the join key, then merged in a single pass on condition {cond}."
        return text

    elif nt == "Nested Loop":
        jtype = node.join_type or "Inner"
        text = f"This join is implemented using nested loop ({jtype} Join)."
        text += " For each row from the outer relation, the inner relation is scanned."
        if node.join_cond:
            text += f" Join condition: {node.join_cond}."
        elif node.children:
            for child in node.children:
                if child.index_cond:
                    text += f" Inner lookup uses index condition: {child.index_cond}."
                    break
        return text

    elif nt == "Sort":
        keys = ", ".join(node.sort_key) if node.sort_key else "unknown"
        text = f"Results are sorted by {keys}."
        if node.parent_node_type == "Merge Join":
            text += " This sort is required to prepare input for merge join."
        elif node.parent_node_type in ("GroupAggregate", "Unique"):
            text += " This sort is required for sort-based aggregation/deduplication."
        return text

    elif nt == "HashAggregate":
        keys = ", ".join(node.group_key) if node.group_key else "all rows"
        text = f"Aggregation uses hash-based strategy, grouping by {keys}."
        if node.filter_cond:
            text += f" HAVING filter applied: {node.filter_cond}."
        return text

    elif nt == "GroupAggregate":
        keys = ", ".join(node.group_key) if node.group_key else "all rows"
        text = f"Aggregation uses sort-based strategy, grouping by {keys}. Input must be pre-sorted on the group key."
        if node.filter_cond:
            text += f" HAVING filter applied: {node.filter_cond}."
        return text

    elif nt == "Aggregate":
        strategy = node.raw.get("Strategy", "Plain")
        if strategy == "Plain":
            text = "A single aggregate value is computed across all qualifying rows."
        else:
            text = f"Aggregation is performed using {strategy} strategy."
        if node.filter_cond:
            text += f" HAVING filter applied: {node.filter_cond}."
        return text

    elif nt == "Limit":
        rows = node.plan_rows
        text = f"Output is limited to {rows} rows."
        return text

    elif nt == "Unique":
        text = "Duplicate rows are removed (DISTINCT) by scanning sorted input and eliminating adjacent duplicates."
        return text

    elif nt == "Append":
        text = "Results from multiple sub-plans are concatenated (UNION ALL)."
        return text

    elif nt == "SetOp":
        strategy = node.raw.get("Strategy", "")
        cmd = node.raw.get("Command", "")
        text = f"Set operation ({cmd}) is performed using {strategy} strategy."
        return text

    elif nt == "Subquery Scan":
        text = f"Subquery result is scanned (alias: {node.alias or 'unknown'})."
        return text

    elif nt == "CTE Scan":
        cte_name = node.raw.get("CTE Name", node.alias or "unknown")
        text = f"CTE '{cte_name}' is scanned."
        return text

    else:
        text = f"Operation: {nt}."
        if node.filter_cond:
            text += f" Filter: {node.filter_cond}."
        return text


def generate_why(node, aqps, alias_map, table_indexes=None):
    nt = node.node_type

    option = OPERATOR_TO_OPTION.get(nt)
    if option is None:
        if nt == "Sort" and node.parent_node_type in ("Merge Join", "GroupAggregate", "Unique"):
            return f"This sort is required by the parent {node.parent_node_type} operator."
        return ""

    relevant_aqps = [a for a in aqps if option in a.disabled_operators]
    if not relevant_aqps:
        return "No alternative plans were generated for comparison."

    parts = []
    alternatives_found = {}

    for aqp in relevant_aqps:
        corr_node = find_corresponding_node(node, aqp.nodes, alias_map)

        if corr_node and corr_node.node_type != nt:
            if node.total_cost > 0:
                cost_ratio = corr_node.total_cost / node.total_cost
            else:
                cost_ratio = float('inf')
            if (corr_node.node_type not in alternatives_found or
                    corr_node.total_cost < alternatives_found[corr_node.node_type][0]):
                alternatives_found[corr_node.node_type] = (corr_node.total_cost, cost_ratio)
        elif corr_node and corr_node.node_type == nt:
            qep_total = node.total_cost
            aqp_total = aqp.total_cost
            if qep_total > 0:
                ratio = aqp_total / qep_total
                if ratio > 1.01:
                    parts.append(
                        f"Even with {nt} discouraged, the planner still uses it "
                        f"(no viable alternative). Discouraging it increases estimated cost by {ratio:.1f}x."
                    )

    if alternatives_found:
        for alt_type, (alt_cost, ratio) in sorted(alternatives_found.items(), key=lambda x: x[1][0]):
            parts.append(
                f"{alt_type} would cost {alt_cost:.2f} ({ratio:.1f}x the cost of {nt} at {node.total_cost:.2f})."
            )

    if nt == "Seq Scan" and table_indexes is not None:
        table = node.relation_name
        if table and table not in table_indexes:
            parts.append(f"No secondary index exists on table '{table}', so sequential scan is the only option.")
        elif table and table in table_indexes:
            idx_names = [name for name, _ in table_indexes[table]]
            parts.append(
                f"Table '{table}' has indexes ({', '.join(idx_names)}), "
                f"but the planner estimated sequential scan to be cheaper for this query."
            )

    if not parts:
        return "This operator was chosen as the most cost-effective option."

    return " ".join(parts)


def match_node_to_component(node, components, alias_map):
    nt = node.node_type

    if nt in SCAN_NODE_TYPES:
        target_table = (node.relation_name or "").lower()
        for comp in components:
            if comp.component_type == "scan":
                if target_table in comp.tables:
                    return comp

    # Strict (fully-qualified) pass runs across ALL candidates before loose
    # (bare-column) fallback, so a self-join with distinguishable prefixed forms
    # wins over a loose bare-form collision.
    elif nt in JOIN_NODE_TYPES:
        if node.join_cond:
            for strict in (True, False):
                for comp in components:
                    if comp.component_type == "join" and comp.conditions:
                        for sql_cond in comp.conditions:
                            if conditions_match(node.join_cond, sql_cond, alias_map, strict=strict):
                                return comp
                # Implicit joins get classified as filters during parsing
                for comp in components:
                    if comp.component_type == "filter" and comp.conditions:
                        for sql_cond in comp.conditions:
                            if conditions_match(node.join_cond, sql_cond, alias_map, strict=strict):
                                comp.component_type = "join"
                                return comp

        node_tables = get_tables_in_subtree(node)
        best_match = None
        best_overlap = 0
        for comp in components:
            if comp.component_type == "join":
                comp_tables = set(comp.tables)
                overlap = len(node_tables & comp_tables)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_match = comp
        if best_match:
            return best_match

    elif nt == "Sort":
        # Skip sorts introduced by optimizer for Merge Join / GroupAggregate / Unique
        if node.parent_node_type not in ("Merge Join", "GroupAggregate", "Unique"):
            for comp in components:
                if comp.component_type == "sort":
                    return comp

    elif nt in AGGREGATE_NODE_TYPES:
        if node.group_key:
            for comp in components:
                if comp.component_type == "groupby":
                    return comp
        for comp in components:
            if comp.component_type == "aggregate":
                return comp

    elif nt == "Limit":
        for comp in components:
            if comp.component_type == "limit":
                return comp

    elif nt == "Unique":
        for comp in components:
            if comp.component_type == "distinct":
                return comp

    return None


def generate_annotations(query):
    components, alias_map = parse_query(query)

    qep = get_qep(query)
    qep_root = qep[0]["Plan"]
    qep_nodes = walk_plan_tree(qep_root)

    aqps = get_targeted_aqps(query, qep_nodes)

    try:
        table_indexes = get_table_indexes()
    except Exception:
        table_indexes = {}

    annotations = []
    matched_components = set()

    for node in qep_nodes:
        if node.node_type in SKIP_NODE_TYPES:
            continue

        component = match_node_to_component(node, components, alias_map)

        if component is None:
            component = _create_synthetic_component(node, query)

        comp_key = (component.component_type, component.sql_text, component.start_pos)
        if comp_key in matched_components:
            continue
        matched_components.add(comp_key)

        how = generate_how(node)
        why = generate_why(node, aqps, alias_map, table_indexes)

        alt_costs = _get_alternative_costs(node, aqps, alias_map)

        annotations.append(Annotation(
            component=component,
            plan_node=node,
            how=how,
            why=why,
            qep_cost=node.total_cost,
            alternative_costs=alt_costs,
        ))

    for node in qep_nodes:
        if node.node_type in AGGREGATE_NODE_TYPES and node.filter_cond:
            for comp in components:
                if comp.component_type == "having":
                    comp_key = ("having", comp.sql_text, comp.start_pos)
                    if comp_key not in matched_components:
                        matched_components.add(comp_key)
                        annotations.append(Annotation(
                            component=comp,
                            plan_node=node,
                            how=f"The HAVING condition is applied as a post-aggregation filter: {node.filter_cond}.",
                            why="HAVING filters are always applied after aggregation is complete.",
                            qep_cost=node.total_cost,
                        ))

    for node in qep_nodes:
        if node.node_type in SCAN_NODE_TYPES and node.filter_cond:
            for comp in components:
                if comp.component_type == "filter":
                    comp_key = ("filter", comp.sql_text, comp.start_pos)
                    if comp_key not in matched_components:
                        if comp.conditions:
                            for sql_cond in comp.conditions:
                                if _filter_condition_matches(node.filter_cond, sql_cond,
                                                            node.relation_name, alias_map):
                                    matched_components.add(comp_key)
                                    annotations.append(Annotation(
                                        component=comp,
                                        plan_node=node,
                                        how=f"This filter is applied during scan of '{node.relation_name}'. "
                                            f"Rows not satisfying this condition are discarded early.",
                                        why="The filter is pushed down to the scan operator to reduce "
                                            "the number of rows before any joins.",
                                        qep_cost=node.total_cost,
                                    ))
                                    break

    return annotations, qep, aqps


def _filter_condition_matches(plan_cond, sql_cond, relation_name, alias_map):
    if conditions_match(plan_cond, sql_cond, alias_map):
        return True

    pn = normalize_condition(plan_cond)
    sn = normalize_condition(sql_cond)

    if not pn or not sn:
        return False

    pn_resolved = _resolve_aliases_in_cond(pn, alias_map)
    sn_resolved = _resolve_aliases_in_cond(sn, alias_map)

    # Strip table prefix so "orders.o_orderdate" matches bare "o_orderdate"
    pn_bare = pn_resolved
    if relation_name:
        pn_bare = pn_bare.replace(relation_name.lower() + '.', '')

    # SQL uses DATE '...' but plan emits '...'::date
    sn_clean = sn_resolved.replace("date '", "'").replace("date'", "'")
    pn_clean = pn_bare.replace("date '", "'").replace("date'", "'")

    if sn_clean in pn_clean:
        return True

    sn_bare = sn_clean
    for alias, table in alias_map.items():
        sn_bare = sn_bare.replace(alias + '.', '').replace(table + '.', '')
    pn_bare2 = pn_clean
    for alias, table in alias_map.items():
        pn_bare2 = pn_bare2.replace(alias + '.', '').replace(table + '.', '')

    if sn_bare in pn_bare2:
        return True

    # Postgres emits heavily-parenthesized filters like
    # `((c_nationkey = 1) OR (c_nationkey = 2))` but SQL has no inner parens.
    # Drop all parens and collapse whitespace so they line up.
    def _drop_parens(s):
        return re.sub(r'\s+', ' ', s.replace('(', ' ').replace(')', ' ')).strip()

    pn_noparen = _drop_parens(pn_bare2)
    sn_noparen = _drop_parens(sn_bare)
    if sn_noparen == pn_noparen or sn_noparen in pn_noparen:
        return True

    col_op_match = re.match(r'(\w+)\s*(>=|<=|>|<|=|!=|<>|like|between)\s*', sn_bare, re.IGNORECASE)
    if col_op_match:
        col = col_op_match.group(1)
        op = col_op_match.group(2)
        val_part = sn_bare[col_op_match.end():].strip().strip("'\"")
        if col in pn_bare2 and op in pn_bare2 and val_part in pn_bare2:
            return True

    return False


def _resolve_aliases_in_cond(condition, alias_map):
    result = condition
    for alias, table in sorted(alias_map.items(), key=lambda x: -len(x[0])):
        result = re.sub(r'\b' + re.escape(alias) + r'\.', table + '.', result)
    return result


def _create_synthetic_component(node, query):
    nt = node.node_type

    if nt == "Sort" and node.parent_node_type in ("Merge Join", "GroupAggregate", "Unique"):
        desc = f"Sort (introduced by optimizer for {node.parent_node_type})"
    elif nt in SCAN_NODE_TYPES and node.relation_name:
        desc = f"{node.relation_name}"
    else:
        desc = nt

    return SQLComponent(
        component_type="optimizer_introduced",
        sql_text=desc,
        start_pos=0,
        end_pos=0,
    )


def _get_alternative_costs(node, aqps, alias_map):
    costs = {}
    option = OPERATOR_TO_OPTION.get(node.node_type)
    if not option:
        return costs

    for aqp in aqps:
        if option in aqp.disabled_operators:
            corr = find_corresponding_node(node, aqp.nodes, alias_map)
            if corr and corr.node_type != node.node_type:
                if corr.node_type not in costs or corr.total_cost < costs[corr.node_type]:
                    costs[corr.node_type] = corr.total_cost

    return costs
