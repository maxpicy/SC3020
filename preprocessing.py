"""
preprocessing.py
Handles database connection, SQL parsing, QEP/AQP retrieval, and plan tree walking
for the SQL query annotation tool.
"""

import psycopg2
import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Function, Comparison
from sqlparse.tokens import Keyword, DML, Punctuation, Wildcard
from dataclasses import dataclass, field


# ============================================================
# Database connection settings
# ============================================================

DB_CONFIG = {
    "dbname": "TPC-H",
    "user": "claude",
    "password": "pass",
    "host": "localhost",
    "port": 5432,
}

# Maps QEP node types to PostgreSQL planner options
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

# Node types that are auxiliary (skip when generating annotations)
SKIP_NODE_TYPES = {"Hash", "Materialize", "Memoize", "Gather", "Gather Merge"}

# Node types considered as join operators
JOIN_NODE_TYPES = {"Hash Join", "Merge Join", "Nested Loop"}

# Node types considered as scan operators
SCAN_NODE_TYPES = {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan"}

# Node types considered as aggregate operators
AGGREGATE_NODE_TYPES = {"Aggregate", "HashAggregate", "GroupAggregate"}

# All planner join options for generating pairwise AQPs
JOIN_OPTIONS = ["enable_hashjoin", "enable_mergejoin", "enable_nestloop"]


# ============================================================
# Data Structures
# ============================================================

@dataclass
class SQLComponent:
    """Represents a parsed piece of the SQL query that can receive an annotation."""
    component_type: str         # "scan", "join", "sort", "aggregate", "groupby", "limit", "subquery", "having", "distinct"
    sql_text: str               # The literal SQL fragment
    start_pos: int              # Character offset in original query
    end_pos: int                # End offset
    tables: list = field(default_factory=list)       # Table names involved (lowercase)
    aliases: dict = field(default_factory=dict)       # {alias: table_name}
    columns: list = field(default_factory=list)       # Column references involved
    conditions: list = field(default_factory=list)    # Conditions as strings


@dataclass
class PlanNode:
    """Represents a single node from the QEP/AQP tree."""
    node_type: str
    id: int = 0              # Unique ID assigned during tree walk (for frontend mapping)
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
    """Stores an Alternative Query Plan and its metadata."""
    disabled_operators: list
    plan: list
    total_cost: float
    nodes: list            # list[PlanNode]
    description: str


# ============================================================
# Database Connection Functions
# ============================================================

def get_connection():
    """Establish and return a connection to the PostgreSQL database."""
    return psycopg2.connect(**DB_CONFIG)


def get_qep(query):
    """
    Retrieve the Query Execution Plan (QEP) for a given SQL query.
    Uses VERBOSE to get additional detail for mapping.
    Returns the plan as a Python list (parsed from JSON).
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"EXPLAIN (FORMAT JSON, VERBOSE TRUE) {query}")
    qep = cur.fetchone()[0]
    cur.close()
    conn.close()
    return qep


def get_aqp(query, disabled_operators):
    """
    Retrieve an Alternative Query Plan (AQP) by disabling specific planner options.
    Returns the plan as a Python list (parsed from JSON).
    """
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
    """Return a list of all user tables in the public schema."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables


def get_table_indexes():
    """
    Return dict of {table_name: [(index_name, indexdef)]}
    for all indexes in the public schema.
    """
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
    """Return dict of {table_name: estimated_row_count} from pg_class."""
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


# ============================================================
# QEP/AQP Tree Walking
# ============================================================

def walk_plan_tree(plan_dict, parent_type=None, depth=0, _counter=None):
    """
    Recursively walk the JSON plan tree and return a flat list of PlanNode objects
    in depth-first order. Each node gets a unique incrementing id.
    """
    if _counter is None:
        _counter = [0]
    # Remap Aggregate node type based on Strategy for consistent matching
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

    # Extract join condition from the appropriate field
    if "Hash Cond" in plan_dict:
        node.join_cond = plan_dict["Hash Cond"]
    elif "Merge Cond" in plan_dict:
        node.join_cond = plan_dict["Merge Cond"]
    elif "Join Filter" in plan_dict:
        node.join_cond = plan_dict["Join Filter"]

    # For Nested Loop without explicit join condition, try to get it from inner child's Index Cond
    if node.node_type == "Nested Loop" and node.join_cond is None:
        children = plan_dict.get("Plans", [])
        if len(children) >= 2:
            inner = children[1]
            if "Index Cond" in inner:
                node.join_cond = inner["Index Cond"]

    # Recurse into children
    nodes = [node]
    for child_dict in plan_dict.get("Plans", []):
        child_nodes = walk_plan_tree(child_dict, parent_type=node.node_type, depth=depth + 1, _counter=_counter)
        node.children.extend([child_nodes[0]] if child_nodes else [])
        nodes.extend(child_nodes)

    return nodes


def get_tables_in_subtree(node):
    """Collect all relation_name values from a node's subtree."""
    tables = set()
    if node.relation_name:
        tables.add(node.relation_name.lower())
    for child in node.children:
        tables |= get_tables_in_subtree(child)
    return tables


def normalize_condition(cond):
    """
    Normalize a plan condition for comparison with SQL conditions.
    Strips outer parentheses, type casts, collapses whitespace, lowercases.
    """
    if not cond:
        return ""
    s = cond.strip()
    # Remove outer parentheses
    while s.startswith("(") and s.endswith(")"):
        # Check if the outer parens actually match
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
    # Remove PostgreSQL type casts like ::text, ::integer, ::numeric, etc.
    s = re.sub(r'::\w+(\(\d+\))?', '', s)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    # Lowercase
    s = s.lower()
    return s


def conditions_match(plan_cond, sql_cond, alias_map):
    """
    Check if a plan condition matches a SQL condition, accounting for
    aliases, type casts, and commutative equality.
    """
    pn = normalize_condition(plan_cond)
    sn = normalize_condition(sql_cond)

    if not pn or not sn:
        return False

    # Direct match
    if pn == sn:
        return True

    # Resolve aliases in both
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
    """Replace aliases with table names in a condition string."""
    result = condition
    # Sort by length descending to avoid partial replacements
    for alias, table in sorted(alias_map.items(), key=lambda x: -len(x[0])):
        # Replace alias.column with table.column
        result = re.sub(
            r'\b' + re.escape(alias) + r'\.',
            table + '.',
            result
        )
    return result


def find_corresponding_node(qep_node, aqp_nodes, alias_map):
    """
    Find the AQP node that corresponds to a QEP node by matching
    the set of tables in their subtrees.
    """
    qep_tables = get_tables_in_subtree(qep_node)
    if not qep_tables:
        return None

    for aqp_node in aqp_nodes:
        if aqp_node.node_type in SKIP_NODE_TYPES:
            continue
        aqp_tables = get_tables_in_subtree(aqp_node)
        if qep_tables == aqp_tables:
            # For scan nodes, also require same relation_name
            if qep_node.node_type in SCAN_NODE_TYPES and aqp_node.node_type in SCAN_NODE_TYPES:
                if qep_node.relation_name and aqp_node.relation_name:
                    if qep_node.relation_name.lower() == aqp_node.relation_name.lower():
                        return aqp_node
            # For join nodes, same table set is sufficient
            elif qep_node.node_type in JOIN_NODE_TYPES and aqp_node.node_type in JOIN_NODE_TYPES:
                return aqp_node
            # For aggregate/sort nodes
            elif (qep_node.node_type in AGGREGATE_NODE_TYPES and
                  aqp_node.node_type in AGGREGATE_NODE_TYPES):
                return aqp_node
            elif qep_node.node_type == "Sort" and aqp_node.node_type == "Sort":
                return aqp_node
            # Generic fallback: if they cover the same tables and are the same category
            elif qep_node.node_type not in SCAN_NODE_TYPES | JOIN_NODE_TYPES | AGGREGATE_NODE_TYPES:
                return aqp_node

    return None


# ============================================================
# AQP Generation Strategy
# ============================================================

def get_targeted_aqps(query, qep_nodes):
    """
    Generate representative AQPs by disabling operators found in the QEP.
    Returns a list of AQPResult objects.
    """
    # Collect unique operator types from QEP
    seen_types = set()
    for node in qep_nodes:
        if node.node_type in OPERATOR_TO_OPTION:
            seen_types.add(node.node_type)

    aqps = []
    cache = {}  # frozenset(disabled) -> AQPResult

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

    # For join nodes, also generate pairwise AQPs to compare all three join types
    qep_join_types = seen_types & JOIN_NODE_TYPES
    if qep_join_types:
        # Disable pairs of join methods to force the third
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


# ============================================================
# SQL Parsing with sqlparse
# ============================================================

def parse_query(query):
    """
    Parse an SQL query into a list of SQLComponent objects and an alias map.
    Returns (components: list[SQLComponent], alias_map: dict[str, str])
    """
    components = []
    alias_map = {}  # alias (lowercase) -> table_name (lowercase)

    # Normalize for analysis but keep original for position tracking
    original = query
    parsed = sqlparse.parse(query)
    if not parsed:
        return components, alias_map

    stmt = parsed[0]

    # Split into clauses
    clauses = _split_clauses(stmt, original)

    # Parse FROM clause to get tables and aliases
    if "FROM" in clauses:
        from_text, from_start = clauses["FROM"]
        table_components = _parse_from_clause(from_text, from_start, original)
        for comp in table_components:
            alias_map.update(comp.aliases)
            components.append(comp)

    # Also build alias_map from tables that don't have explicit aliases
    # (table name itself serves as the alias)
    for comp in components:
        if comp.component_type == "scan":
            for tbl in comp.tables:
                if tbl not in alias_map.values():
                    alias_map[tbl] = tbl

    # Parse WHERE clause for join and filter conditions
    if "WHERE" in clauses:
        where_text, where_start = clauses["WHERE"]
        where_components = _parse_where_clause(where_text, where_start, original, alias_map)
        components.extend(where_components)

    # Parse ON conditions from explicit JOINs (already handled in FROM parsing)
    # These are stored in the join components from _parse_from_clause

    # Parse SELECT clause for aggregations and DISTINCT
    if "SELECT" in clauses:
        select_text, select_start = clauses["SELECT"]
        select_components = _parse_select_clause(select_text, select_start, original)
        components.extend(select_components)

    # Parse GROUP BY
    if "GROUP BY" in clauses:
        gb_text, gb_start = clauses["GROUP BY"]
        components.append(SQLComponent(
            component_type="groupby",
            sql_text=gb_text.strip(),
            start_pos=gb_start,
            end_pos=gb_start + len(gb_text),
            columns=_extract_columns(gb_text),
        ))

    # Parse ORDER BY
    if "ORDER BY" in clauses:
        ob_text, ob_start = clauses["ORDER BY"]
        components.append(SQLComponent(
            component_type="sort",
            sql_text=ob_text.strip(),
            start_pos=ob_start,
            end_pos=ob_start + len(ob_text),
            columns=_extract_columns(ob_text),
        ))

    # Parse HAVING
    if "HAVING" in clauses:
        hv_text, hv_start = clauses["HAVING"]
        components.append(SQLComponent(
            component_type="having",
            sql_text=hv_text.strip(),
            start_pos=hv_start,
            end_pos=hv_start + len(hv_text),
            conditions=[hv_text.strip()],
        ))

    # Parse LIMIT
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
    """
    Split a parsed SQL statement into clause regions.
    Returns dict of {clause_name: (clause_text, start_position_in_original)}.
    """
    clauses = {}
    original_upper = original.upper()

    # Define clause keywords in order of precedence
    clause_keywords = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"
    ]

    # Find positions of top-level clause keywords (not inside subqueries)
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
                    # Make sure it's a word boundary
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

    # Extract text between consecutive keywords
    for idx, (pos, kw) in enumerate(keyword_positions):
        # Content starts after the keyword
        content_start = pos + len(kw)
        # Content ends at the next keyword or end of string
        if idx + 1 < len(keyword_positions):
            content_end = keyword_positions[idx + 1][0]
        else:
            content_end = len(original)
        # Strip trailing semicolons
        content = original[content_start:content_end].rstrip(';').strip()
        clauses[kw] = (content, content_start)

    return clauses


def _parse_from_clause(from_text, from_start, original):
    """
    Parse the FROM clause to extract table references and JOIN conditions.
    Returns a list of SQLComponent objects (scan and join components).
    """
    components = []

    # Normalize the FROM text for pattern matching
    text = from_text.strip()
    if not text:
        return components

    # Use sqlparse to parse the FROM clause tokens
    parsed = sqlparse.parse("SELECT * FROM " + text)
    if not parsed:
        return components

    stmt = parsed[0]

    # Find the FROM token and get what follows
    from_tokens = []
    found_from = False
    for token in stmt.tokens:
        if found_from:
            from_tokens.append(token)
        if token.ttype is Keyword and token.normalized == 'FROM':
            found_from = True

    if not from_tokens:
        return components

    # Extract table identifiers and JOIN keywords
    tables_and_joins = _extract_tables_from_tokens(from_tokens)

    for item in tables_and_joins:
        if item["type"] == "table":
            table_name = item["table"].lower()
            alias = item.get("alias", table_name).lower()

            # Find position in original query
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
            # ON condition from explicit JOIN
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
    """
    Extract table references and join conditions from FROM clause tokens.
    Returns a list of dicts with type='table' or type='join'.
    """
    results = []

    for token in tokens:
        if token.ttype is Punctuation:
            continue

        # Skip whitespace
        if token.is_whitespace:
            continue

        # Handle IdentifierList (comma-separated tables)
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                table_info = _parse_identifier(identifier)
                if table_info:
                    results.append(table_info)

        # Handle single Identifier
        elif isinstance(token, Identifier):
            table_info = _parse_identifier(token)
            if table_info:
                results.append(table_info)

        # Handle JOIN keyword followed by table and ON condition
        elif token.ttype is Keyword and 'JOIN' in token.normalized:
            # The join keyword itself is noted; table and ON follow in subsequent tokens
            pass

        # Handle Comparison in ON clause — this gets complex, so we use a different approach
        elif isinstance(token, Comparison):
            results.append({
                "type": "join",
                "condition": str(token).strip(),
                "tables": [],
            })

    return results


def _parse_identifier(identifier):
    """Parse a single sqlparse Identifier to extract table name and alias."""
    # Check if this is a subquery
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
    """
    Parse the WHERE clause into join conditions and filter conditions.
    Returns a list of SQLComponent objects.
    """
    components = []
    if not where_text.strip():
        return components

    # Split conditions on AND/OR at depth 0
    conditions = _split_conditions(where_text)

    for cond_text in conditions:
        cond_text = cond_text.strip()
        if not cond_text:
            continue

        # Check if this is a subquery condition
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

        # Classify as join or filter condition
        tables_referenced = _get_tables_in_condition(cond_text, alias_map)

        pos = _find_position(original, cond_text, where_start)

        if len(tables_referenced) >= 2:
            # Join condition - references 2+ tables
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
            # Filter condition - references 0 or 1 table
            # Filters are attached to scan nodes, not standalone components
            # We still track them for potential annotation
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
    """Split a WHERE clause text on AND/OR at parenthesis depth 0."""
    conditions = []
    depth = 0
    current = []
    tokens = re.split(r'(\bAND\b|\bOR\b)', text, flags=re.IGNORECASE)

    i = 0
    while i < len(tokens):
        token = tokens[i]
        # Count parentheses in this token
        for ch in token:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1

        if depth == 0 and token.strip().upper() in ('AND', 'OR'):
            # This is a top-level AND/OR
            conditions.append(''.join(current).strip())
            current = []
        else:
            current.append(token)
        i += 1

    if current:
        conditions.append(''.join(current).strip())

    return [c for c in conditions if c]


def _contains_subquery(text):
    """Check if text contains a subquery (SELECT inside parentheses)."""
    upper = text.upper()
    return bool(re.search(r'\(\s*SELECT\b', upper))


def _get_tables_in_condition(condition, alias_map):
    """
    Determine which tables are referenced in a condition using the alias map.
    Returns a set of table names.
    """
    tables = set()
    cond_lower = condition.lower()

    # Look for alias.column or table.column patterns
    # Pattern: word followed by dot followed by word
    refs = re.findall(r'\b(\w+)\.\w+', cond_lower)
    for ref in refs:
        if ref in alias_map:
            tables.add(alias_map[ref])
        elif ref in alias_map.values():
            tables.add(ref)

    # If no table.column patterns found, try matching bare column names
    # against known table/alias column prefixes (e.g. c_custkey -> customer)
    if not tables:
        bare_cols = re.findall(r'\b([a-zA-Z]\w*)\b', cond_lower)
        # Exclude SQL keywords and literals
        sql_keywords = {
            'and', 'or', 'not', 'in', 'is', 'null', 'like', 'between',
            'exists', 'true', 'false', 'date', 'interval', 'case', 'when',
            'then', 'else', 'end', 'as', 'select', 'from', 'where', 'asc',
            'desc', 'any', 'all', 'some',
        }
        bare_cols = [c for c in bare_cols if c not in sql_keywords]
        for col in bare_cols:
            for alias, table in alias_map.items():
                # Match by column prefix: e.g. column "o_custkey" matches alias "o" or table "orders"
                # Common patterns: <alias>_<col>, <alias>.<col>, or <table_initial>_<col>
                if col.startswith(alias + '_') or col.startswith(alias + '.'):
                    tables.add(table)
                    break
                # Also try first letter of table name as prefix (TPC-H convention)
                if len(table) > 0 and col.startswith(table[0] + '_'):
                    # Verify this prefix uniquely identifies the table
                    prefix = table[0] + '_'
                    matching_tables = [t for a, t in alias_map.items()
                                       if t.startswith(table[0])]
                    if len(matching_tables) == 1:
                        tables.add(table)
                        break
                # Also try first two letters
                if len(table) > 1 and col.startswith(table[:2] + '_'):
                    tables.add(table)
                    break

    return tables


def _extract_columns(text):
    """Extract column references (including table.column) from text."""
    return re.findall(r'\b(\w+\.\w+)\b', text)


def _parse_select_clause(select_text, select_start, original):
    """Parse SELECT clause for DISTINCT and aggregation functions."""
    components = []
    text = select_text.strip()

    # Check for DISTINCT
    upper = text.upper()
    if upper.startswith("DISTINCT"):
        pos = _find_position(original, "DISTINCT", select_start)
        components.append(SQLComponent(
            component_type="distinct",
            sql_text="DISTINCT",
            start_pos=pos,
            end_pos=pos + len("DISTINCT"),
        ))

    # Check for aggregate functions
    agg_pattern = r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\('
    for match in re.finditer(agg_pattern, text, re.IGNORECASE):
        # Find the full function call including closing paren
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
    """
    Find the position of search_text in the original query string.
    Uses case-insensitive search and handles whitespace variations.
    """
    if not search_text:
        return start_from

    # Try exact match first
    pos = original.find(search_text, start_from)
    if pos >= 0:
        return pos

    # Try case-insensitive
    original_lower = original.lower()
    search_lower = search_text.lower()
    pos = original_lower.find(search_lower, start_from)
    if pos >= 0:
        return pos

    # Try with collapsed whitespace
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
