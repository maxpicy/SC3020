import psycopg2
import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Function, Comparison
from sqlparse.tokens import Keyword, DML, Punctuation, Wildcard
from dataclasses import dataclass, field


DB_CONFIG = {
    "dbname": "TPC-H",
    "user": "claude",
    "password": "pass",
    "host": "localhost",
    "port": 5432,
}

OPERATOR_TO_OPTION = {
    "Hash Join":        "enable_hashjoin",
    "Merge Join":       "enable_mergejoin",
    "Nested Loop":      "enable_nestloop",
    "Seq Scan":         "enable_seqscan",
    "Index Scan":       "enable_indexscan",
    "Index Only Scan":  "enable_indexonlyscan",
    "Bitmap Heap Scan": "enable_bitmapscan",
    "Sort":             "enable_sort",
    "Memoize":          "enable_memoize",
    "HashAggregate":    "enable_hashagg",
    "Materialize":      "enable_material",
}

# Auxiliary nodes - skip when generating annotations
SKIP_NODE_TYPES = {"Hash", "Materialize", "Memoize", "Gather", "Gather Merge"}

JOIN_NODE_TYPES = {"Hash Join", "Merge Join", "Nested Loop"}
SCAN_NODE_TYPES = {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan"}
AGGREGATE_NODE_TYPES = {"Aggregate", "HashAggregate", "GroupAggregate"}

# Used for generating pairwise AQPs to compare all three join types
JOIN_OPTIONS = ["enable_hashjoin", "enable_mergejoin", "enable_nestloop"]


@dataclass
class SQLComponent:
    component_type: str         # scan, join, sort, aggregate, groupby, limit, subquery, having, distinct
    sql_text: str
    start_pos: int
    end_pos: int
    tables: list = field(default_factory=list)
    aliases: dict = field(default_factory=dict)
    columns: list = field(default_factory=list)
    conditions: list = field(default_factory=list)


@dataclass
class PlanNode:
    node_type: str
    id: int = 0              # For frontend node mapping
    relation_name: str = None
    alias: str = None
    total_cost: float = 0.0
    startup_cost: float = 0.0
    plan_rows: int = 0
    join_type: str = None
    join_cond: str = None
    filter_cond: str = None
    sort_key: list = None
    group_key: list = None
    index_name: str = None
    index_cond: str = None
    parent_node_type: str = None
    children: list = field(default_factory=list)
    depth: int = 0
    raw: dict = field(default_factory=dict)


@dataclass
class AQPResult:
    disabled_operators: list
    plan: list
    total_cost: float
    nodes: list            # list[PlanNode]
    description: str


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_qep(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"EXPLAIN (FORMAT JSON, VERBOSE TRUE) {query}")
    qep = cur.fetchone()[0]
    cur.close()
    conn.close()
    return qep


def get_aqp(query, disabled_operators):
    conn = get_connection()
    cur = conn.cursor()
    for op in disabled_operators:
        cur.execute(f"SET {op} = off;")
    cur.execute(f"EXPLAIN (FORMAT JSON, VERBOSE TRUE) {query}")
    aqp = cur.fetchone()[0]
    cur.close()
    conn.close()
    return aqp


def get_all_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables


def get_table_indexes():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname;
    """)
    result = {}
    for tablename, indexname, indexdef in cur.fetchall():
        if tablename not in result:
            result[tablename] = []
        result[tablename].append((indexname, indexdef))
    cur.close()
    conn.close()
    return result


def get_table_row_counts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT relname, reltuples::bigint
        FROM pg_class
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
          AND relkind = 'r';
    """)
    result = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return result


def walk_plan_tree(plan_dict, parent_type=None, depth=0, _counter=None):
    if _counter is None:
        _counter = [0]
    # Remap Aggregate based on Strategy for consistent matching with AGGREGATE_NODE_TYPES
    raw_node_type = plan_dict.get("Node Type", "Unknown")
    if raw_node_type == "Aggregate":
        strategy = plan_dict.get("Strategy", "")
        if strategy == "Hashed":
            raw_node_type = "HashAggregate"
        elif strategy == "Sorted":
            raw_node_type = "GroupAggregate"
        # else: keep as "Aggregate" (Plain strategy)

    node_id = _counter[0]
    _counter[0] += 1

    node = PlanNode(
        node_type=raw_node_type,
        id=node_id,
        relation_name=plan_dict.get("Relation Name"),
        alias=plan_dict.get("Alias"),
        total_cost=plan_dict.get("Total Cost", 0.0),
        startup_cost=plan_dict.get("Startup Cost", 0.0),
        plan_rows=plan_dict.get("Plan Rows", 0),
        join_type=plan_dict.get("Join Type"),
        filter_cond=plan_dict.get("Filter"),
        sort_key=plan_dict.get("Sort Key"),
        group_key=plan_dict.get("Group Key"),
        index_name=plan_dict.get("Index Name"),
        index_cond=plan_dict.get("Index Cond"),
        parent_node_type=parent_type,
        depth=depth,
        raw=plan_dict,
    )

    if "Hash Cond" in plan_dict:
        node.join_cond = plan_dict["Hash Cond"]
    elif "Merge Cond" in plan_dict:
        node.join_cond = plan_dict["Merge Cond"]
    elif "Join Filter" in plan_dict:
        node.join_cond = plan_dict["Join Filter"]

    # Nested Loop may have join condition in inner child's Index Cond
    if node.node_type == "Nested Loop" and node.join_cond is None:
        children = plan_dict.get("Plans", [])
        if len(children) >= 2:
            inner = children[1]
            if "Index Cond" in inner:
                node.join_cond = inner["Index Cond"]

    nodes = [node]
    for child_dict in plan_dict.get("Plans", []):
        child_nodes = walk_plan_tree(child_dict, parent_type=node.node_type, depth=depth + 1, _counter=_counter)
        node.children.extend([child_nodes[0]] if child_nodes else [])
        nodes.extend(child_nodes)

    return nodes


def get_tables_in_subtree(node):
    tables = set()
    if node.relation_name:
        tables.add(node.relation_name.lower())
    for child in node.children:
        tables |= get_tables_in_subtree(child)
    return tables


def normalize_condition(cond):
    if not cond:
        return ""
    s = cond.strip()
    while s.startswith("(") and s.endswith(")"):
        # Verify the outer parens actually match before stripping
        depth = 0
        matched = True
        for i, c in enumerate(s):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            if depth == 0 and i < len(s) - 1:
                matched = False
                break
        if matched:
            s = s[1:-1].strip()
        else:
            break
    # Remove PostgreSQL type casts (::text, ::integer, etc.)
    s = re.sub(r'::\w+(\(\d+\))?', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = s.lower()
    return s


def conditions_match(plan_cond, sql_cond, alias_map):
    pn = normalize_condition(plan_cond)
    sn = normalize_condition(sql_cond)

    if not pn or not sn:
        return False

    if pn == sn:
        return True

    pn_resolved = _resolve_aliases(pn, alias_map)
    sn_resolved = _resolve_aliases(sn, alias_map)
    if pn_resolved == sn_resolved:
        return True

    # Commutative match for equality conditions
    for cond_str in [pn_resolved, pn]:
        if "=" in cond_str and "<" not in cond_str and ">" not in cond_str and "!" not in cond_str:
            parts = cond_str.split("=", 1)
            if len(parts) == 2:
                flipped = parts[1].strip() + " = " + parts[0].strip()
                if flipped == sn_resolved or flipped == sn:
                    return True

    return False


def _resolve_aliases(condition, alias_map):
    result = condition
    # Sort by length descending to avoid partial replacements
    for alias, table in sorted(alias_map.items(), key=lambda x: -len(x[0])):
            result = re.sub(
            r'\b' + re.escape(alias) + r'\.',
            table + '.',
            result
        )
    return result


def find_corresponding_node(qep_node, aqp_nodes, alias_map):
    qep_tables = get_tables_in_subtree(qep_node)
    if not qep_tables:
        return None

    for aqp_node in aqp_nodes:
        if aqp_node.node_type in SKIP_NODE_TYPES:
            continue
        aqp_tables = get_tables_in_subtree(aqp_node)
        if qep_tables == aqp_tables:
            # Scan nodes also require same relation_name
            if qep_node.node_type in SCAN_NODE_TYPES and aqp_node.node_type in SCAN_NODE_TYPES:
                if qep_node.relation_name and aqp_node.relation_name:
                    if qep_node.relation_name.lower() == aqp_node.relation_name.lower():
                        return aqp_node
            elif qep_node.node_type in JOIN_NODE_TYPES and aqp_node.node_type in JOIN_NODE_TYPES:
                return aqp_node
            elif (qep_node.node_type in AGGREGATE_NODE_TYPES and
                  aqp_node.node_type in AGGREGATE_NODE_TYPES):
                return aqp_node
            elif qep_node.node_type == "Sort" and aqp_node.node_type == "Sort":
                return aqp_node
            elif qep_node.node_type not in SCAN_NODE_TYPES | JOIN_NODE_TYPES | AGGREGATE_NODE_TYPES:
                return aqp_node

    return None


def get_targeted_aqps(query, qep_nodes):
    seen_types = set()
    for node in qep_nodes:
        if node.node_type in OPERATOR_TO_OPTION:
            seen_types.add(node.node_type)

    aqps = []
    cache = {}

    for node_type in seen_types:
        option = OPERATOR_TO_OPTION[node_type]
        key = frozenset([option])
        if key not in cache:
            try:
                plan = get_aqp(query, [option])
                nodes = walk_plan_tree(plan[0]["Plan"])
                aqp = AQPResult(
                    disabled_operators=[option],
                    plan=plan,
                    total_cost=plan[0]["Plan"]["Total Cost"],
                    nodes=nodes,
                    description=f"{node_type} disabled",
                )
                cache[key] = aqp
                aqps.append(aqp)
            except Exception:
                pass

    # Disable pairs of join methods to force the third alternative
    qep_join_types = seen_types & JOIN_NODE_TYPES
    if qep_join_types:
        for i, opt1 in enumerate(JOIN_OPTIONS):
            for opt2 in JOIN_OPTIONS[i + 1:]:
                key = frozenset([opt1, opt2])
                if key not in cache:
                    try:
                        plan = get_aqp(query, [opt1, opt2])
                        nodes = walk_plan_tree(plan[0]["Plan"])
                        aqp = AQPResult(
                            disabled_operators=[opt1, opt2],
                            plan=plan,
                            total_cost=plan[0]["Plan"]["Total Cost"],
                            nodes=nodes,
                            description=f"{opt1} and {opt2} disabled",
                        )
                        cache[key] = aqp
                        aqps.append(aqp)
                    except Exception:
                        pass

    return aqps


def parse_query(query):
    components = []
    alias_map = {}

    original = query
    parsed = sqlparse.parse(query)
    if not parsed:
        return components, alias_map

    stmt = parsed[0]

    clauses = _split_clauses(stmt, original)

    if "FROM" in clauses:
        from_text, from_start = clauses["FROM"]
        table_components = _parse_from_clause(from_text, from_start, original)
        for comp in table_components:
            alias_map.update(comp.aliases)
            components.append(comp)

    # Table name itself serves as an alias when no explicit alias is given
    for comp in components:
        if comp.component_type == "scan":
            for tbl in comp.tables:
                if tbl not in alias_map.values():
                    alias_map[tbl] = tbl

    if "WHERE" in clauses:
        where_text, where_start = clauses["WHERE"]
        where_components = _parse_where_clause(where_text, where_start, original, alias_map)
        components.extend(where_components)

    # ON conditions from explicit JOINs are already handled in _parse_from_clause

    if "SELECT" in clauses:
        select_text, select_start = clauses["SELECT"]
        select_components = _parse_select_clause(select_text, select_start, original)
        components.extend(select_components)

    if "GROUP BY" in clauses:
        gb_text, gb_start = clauses["GROUP BY"]
        components.append(SQLComponent(
            component_type="groupby",
            sql_text=gb_text.strip(),
            start_pos=gb_start,
            end_pos=gb_start + len(gb_text),
            columns=_extract_columns(gb_text),
        ))

    if "ORDER BY" in clauses:
        ob_text, ob_start = clauses["ORDER BY"]
        components.append(SQLComponent(
            component_type="sort",
            sql_text=ob_text.strip(),
            start_pos=ob_start,
            end_pos=ob_start + len(ob_text),
            columns=_extract_columns(ob_text),
        ))

    if "HAVING" in clauses:
        hv_text, hv_start = clauses["HAVING"]
        components.append(SQLComponent(
            component_type="having",
            sql_text=hv_text.strip(),
            start_pos=hv_start,
            end_pos=hv_start + len(hv_text),
            conditions=[hv_text.strip()],
        ))

    if "LIMIT" in clauses:
        lm_text, lm_start = clauses["LIMIT"]
        components.append(SQLComponent(
            component_type="limit",
            sql_text=lm_text.strip(),
            start_pos=lm_start,
            end_pos=lm_start + len(lm_text),
        ))

    return components, alias_map


def _split_clauses(stmt, original):
    clauses = {}
    original_upper = original.upper()

    clause_keywords = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"
    ]

    # Only match top-level keywords (not inside subqueries)
    keyword_positions = []
    depth = 0
    i = 0
    while i < len(original_upper):
        if original[i] == '(':
            depth += 1
            i += 1
            continue
        elif original[i] == ')':
            depth -= 1
            i += 1
            continue

        if depth == 0:
            for kw in clause_keywords:
                if original_upper[i:i + len(kw)] == kw:
                    before_ok = (i == 0 or not original_upper[i - 1].isalpha())
                    after_pos = i + len(kw)
                    after_ok = (after_pos >= len(original_upper) or
                                not original_upper[after_pos].isalpha())
                    if before_ok and after_ok:
                        keyword_positions.append((i, kw))
                        i = after_pos
                        break
            else:
                i += 1
        else:
            i += 1

    for idx, (pos, kw) in enumerate(keyword_positions):
        content_start = pos + len(kw)
        if idx + 1 < len(keyword_positions):
            content_end = keyword_positions[idx + 1][0]
        else:
            content_end = len(original)
        content = original[content_start:content_end].rstrip(';').strip()
        clauses[kw] = (content, content_start)

    return clauses


def _parse_from_clause(from_text, from_start, original):
    components = []

    text = from_text.strip()
    if not text:
        return components

    parsed = sqlparse.parse("SELECT * FROM " + text)
    if not parsed:
        return components

    stmt = parsed[0]

    from_tokens = []
    found_from = False
    for token in stmt.tokens:
        if found_from:
            from_tokens.append(token)
        if token.ttype is Keyword and token.normalized == 'FROM':
            found_from = True

    if not from_tokens:
        return components

    tables_and_joins = _extract_tables_from_tokens(from_tokens)

    for item in tables_and_joins:
        if item["type"] == "table":
            table_name = item["table"].lower()
            alias = item.get("alias", table_name).lower()

            search_text = item["raw_text"]
            pos = _find_position(original, search_text, from_start)

            comp = SQLComponent(
                component_type="scan",
                sql_text=search_text,
                start_pos=pos,
                end_pos=pos + len(search_text),
                tables=[table_name],
                aliases={alias: table_name},
            )
            components.append(comp)

        elif item["type"] == "join":
            cond_text = item.get("condition", "")
            if cond_text:
                pos = _find_position(original, cond_text, from_start)
                comp = SQLComponent(
                    component_type="join",
                    sql_text=cond_text,
                    start_pos=pos,
                    end_pos=pos + len(cond_text),
                    tables=item.get("tables", []),
                    conditions=[cond_text],
                )
                components.append(comp)

    return components


def _extract_tables_from_tokens(tokens):
    results = []

    for token in tokens:
        if token.ttype is Punctuation:
            continue

        if token.is_whitespace:
            continue

        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                table_info = _parse_identifier(identifier)
                if table_info:
                    results.append(table_info)

        elif isinstance(token, Identifier):
            table_info = _parse_identifier(token)
            if table_info:
                results.append(table_info)

        # JOIN keyword - table and ON follow in subsequent tokens
        elif token.ttype is Keyword and 'JOIN' in token.normalized:
            pass

        elif isinstance(token, Comparison):
            results.append({
                "type": "join",
                "condition": str(token).strip(),
                "tables": [],
            })

    return results


def _parse_identifier(identifier):
    for token in identifier.tokens:
        if isinstance(token, Parenthesis):
            inner = str(token)
            if inner.strip().upper().startswith("(SELECT"):
                alias = identifier.get_alias() or ""
                return {
                    "type": "table",
                    "table": f"subquery_{alias}",
                    "alias": alias.lower() if alias else "",
                    "raw_text": str(identifier).strip(),
                }

    real_name = identifier.get_real_name()
    if not real_name:
        return None

    alias = identifier.get_alias() or real_name
    return {
        "type": "table",
        "table": real_name.lower(),
        "alias": alias.lower(),
        "raw_text": str(identifier).strip(),
    }


def _parse_where_clause(where_text, where_start, original, alias_map):
    components = []
    if not where_text.strip():
        return components

    conditions = _split_conditions(where_text)

    for cond_text in conditions:
        cond_text = cond_text.strip()
        if not cond_text:
            continue

        if _contains_subquery(cond_text):
            pos = _find_position(original, cond_text, where_start)
            components.append(SQLComponent(
                component_type="subquery",
                sql_text=cond_text,
                start_pos=pos,
                end_pos=pos + len(cond_text),
                conditions=[cond_text],
            ))
            continue

        tables_referenced = _get_tables_in_condition(cond_text, alias_map)

        pos = _find_position(original, cond_text, where_start)

        if len(tables_referenced) >= 2:
            # References 2+ tables, so this is a join condition
            components.append(SQLComponent(
                component_type="join",
                sql_text=cond_text,
                start_pos=pos,
                end_pos=pos + len(cond_text),
                tables=list(tables_referenced),
                conditions=[cond_text],
                columns=_extract_columns(cond_text),
            ))
        else:
            # Filter on 0 or 1 table, tracked for scan node annotation
            components.append(SQLComponent(
                component_type="filter",
                sql_text=cond_text,
                start_pos=pos,
                end_pos=pos + len(cond_text),
                tables=list(tables_referenced),
                conditions=[cond_text],
                columns=_extract_columns(cond_text),
            ))

    return components


def _split_conditions(text):
    conditions = []
    depth = 0
    current = []
    tokens = re.split(r'(\bAND\b|\bOR\b)', text, flags=re.IGNORECASE)

    i = 0
    while i < len(tokens):
        token = tokens[i]
        for ch in token:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1

        if depth == 0 and token.strip().upper() in ('AND', 'OR'):
            conditions.append(''.join(current).strip())
            current = []
        else:
            current.append(token)
        i += 1

    if current:
        conditions.append(''.join(current).strip())

    return [c for c in conditions if c]


def _contains_subquery(text):
    upper = text.upper()
    return bool(re.search(r'\(\s*SELECT\b', upper))


def _get_tables_in_condition(condition, alias_map):
    tables = set()
    cond_lower = condition.lower()

    refs = re.findall(r'\b(\w+)\.\w+', cond_lower)
    for ref in refs:
        if ref in alias_map:
            tables.add(alias_map[ref])
        elif ref in alias_map.values():
            tables.add(ref)

    # No table.column found - try matching bare column prefixes (TPC-H convention)
    if not tables:
        bare_cols = re.findall(r'\b([a-zA-Z]\w*)\b', cond_lower)
        sql_keywords = {
            'and', 'or', 'not', 'in', 'is', 'null', 'like', 'between',
            'exists', 'true', 'false', 'date', 'interval', 'case', 'when',
            'then', 'else', 'end', 'as', 'select', 'from', 'where', 'asc',
            'desc', 'any', 'all', 'some',
        }
        bare_cols = [c for c in bare_cols if c not in sql_keywords]
        for col in bare_cols:
            for alias, table in alias_map.items():
                if col.startswith(alias + '_') or col.startswith(alias + '.'):
                    tables.add(table)
                    break
                # First letter of table name as prefix (e.g. o_custkey -> orders)
                if len(table) > 0 and col.startswith(table[0] + '_'):
                    prefix = table[0] + '_'
                    matching_tables = [t for a, t in alias_map.items()
                                       if t.startswith(table[0])]
                    if len(matching_tables) == 1:
                        tables.add(table)
                        break
                if len(table) > 1 and col.startswith(table[:2] + '_'):
                    tables.add(table)
                    break

    return tables


def _extract_columns(text):
    return re.findall(r'\b(\w+\.\w+)\b', text)


def _parse_select_clause(select_text, select_start, original):
    components = []
    text = select_text.strip()

    upper = text.upper()
    if upper.startswith("DISTINCT"):
        pos = _find_position(original, "DISTINCT", select_start)
        components.append(SQLComponent(
            component_type="distinct",
            sql_text="DISTINCT",
            start_pos=pos,
            end_pos=pos + len("DISTINCT"),
        ))

    agg_pattern = r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\('
    for match in re.finditer(agg_pattern, text, re.IGNORECASE):
        func_start = match.start()
        depth = 0
        func_end = func_start
        for i in range(match.end() - 1, len(text)):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    func_end = i + 1
                    break

        func_text = text[func_start:func_end]
        pos = _find_position(original, func_text, select_start)
        components.append(SQLComponent(
            component_type="aggregate",
            sql_text=func_text,
            start_pos=pos,
            end_pos=pos + len(func_text),
        ))

    return components


def _find_position(original, search_text, start_from=0):
    if not search_text:
        return start_from

    pos = original.find(search_text, start_from)
    if pos >= 0:
        return pos

    original_lower = original.lower()
    search_lower = search_text.lower()
    pos = original_lower.find(search_lower, start_from)
    if pos >= 0:
        return pos

    search_collapsed = re.sub(r'\s+', ' ', search_text).strip()
    original_collapsed_map = []
    collapsed = []
    for i, ch in enumerate(original):
        if ch in (' ', '\t', '\n', '\r'):
            if collapsed and collapsed[-1] != ' ':
                collapsed.append(' ')
                original_collapsed_map.append(i)
        else:
            collapsed.append(ch)
            original_collapsed_map.append(i)
    collapsed_str = ''.join(collapsed)

    pos = collapsed_str.lower().find(search_collapsed.lower())
    if pos >= 0 and pos < len(original_collapsed_map):
        return original_collapsed_map[pos]

    return start_from
