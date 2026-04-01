"""
interface.py
FastAPI application serving the SQL Query Annotation Tool.
Provides API endpoints for query analysis and serves the React frontend.
"""

import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional

from annotation import generate_annotations, Annotation
from preprocessing import (
    SQLComponent, PlanNode, AQPResult,
    get_all_tables, get_table_indexes,
)


# ============================================================
# Pydantic Models (for JSON serialization)
# ============================================================

class SQLComponentModel(BaseModel):
    component_type: str
    sql_text: str
    start_pos: int
    end_pos: int
    tables: list = []
    aliases: dict = {}
    columns: list = []
    conditions: list = []


class PlanNodeModel(BaseModel):
    id: int = 0
    node_type: str
    relation_name: Optional[str] = None
    alias: Optional[str] = None
    total_cost: float = 0.0
    startup_cost: float = 0.0
    plan_rows: int = 0
    join_type: Optional[str] = None
    join_cond: Optional[str] = None
    filter_cond: Optional[str] = None
    sort_key: Optional[list] = None
    group_key: Optional[list] = None
    index_name: Optional[str] = None
    index_cond: Optional[str] = None
    parent_node_type: Optional[str] = None
    children: list = []  # Will be list[PlanNodeModel] after update_forward_refs
    depth: int = 0


# Enable recursive self-reference
PlanNodeModel.model_rebuild()


class AQPResultModel(BaseModel):
    disabled_operators: list
    total_cost: float
    description: str
    nodes: list[PlanNodeModel] = []


class AnnotationModel(BaseModel):
    component: SQLComponentModel
    plan_node: PlanNodeModel
    how: str
    why: str
    qep_cost: float = 0.0
    alternative_costs: dict = {}


class AnalyzeRequest(BaseModel):
    query: str


class AnalyzeResponse(BaseModel):
    annotations: list[AnnotationModel]
    qep: list  # Raw JSON plan
    aqps: list[AQPResultModel]
    original_query: str


class TablesResponse(BaseModel):
    tables: list[str]


# ============================================================
# Dataclass -> Pydantic Converters
# ============================================================

def convert_plan_node(node: PlanNode) -> PlanNodeModel:
    """Convert a PlanNode dataclass to a Pydantic model (recursive)."""
    return PlanNodeModel(
        id=node.id,
        node_type=node.node_type,
        relation_name=node.relation_name,
        alias=node.alias,
        total_cost=node.total_cost,
        startup_cost=node.startup_cost,
        plan_rows=node.plan_rows,
        join_type=node.join_type,
        join_cond=node.join_cond,
        filter_cond=node.filter_cond,
        sort_key=node.sort_key,
        group_key=node.group_key,
        index_name=node.index_name,
        index_cond=node.index_cond,
        parent_node_type=node.parent_node_type,
        children=[convert_plan_node(c) for c in node.children],
        depth=node.depth,
    )


def convert_component(comp: SQLComponent) -> SQLComponentModel:
    """Convert an SQLComponent dataclass to a Pydantic model."""
    return SQLComponentModel(
        component_type=comp.component_type,
        sql_text=comp.sql_text,
        start_pos=comp.start_pos,
        end_pos=comp.end_pos,
        tables=comp.tables,
        aliases=comp.aliases,
        columns=comp.columns,
        conditions=comp.conditions,
    )


def convert_annotation(ann: Annotation) -> AnnotationModel:
    """Convert an Annotation dataclass to a Pydantic model."""
    return AnnotationModel(
        component=convert_component(ann.component),
        plan_node=convert_plan_node(ann.plan_node),
        how=ann.how,
        why=ann.why,
        qep_cost=ann.qep_cost,
        alternative_costs=ann.alternative_costs,
    )


def convert_aqp(aqp: AQPResult) -> AQPResultModel:
    """Convert an AQPResult dataclass to a Pydantic model."""
    # Only convert top-level nodes (not the full flat list, just the tree root)
    root_nodes = [n for n in aqp.nodes if n.depth == 0]
    return AQPResultModel(
        disabled_operators=aqp.disabled_operators,
        total_cost=aqp.total_cost,
        description=aqp.description,
        nodes=[convert_plan_node(n) for n in root_nodes],
    )


# ============================================================
# FastAPI Application
# ============================================================

app = FastAPI(title="SC3020 SQL Query Annotation Tool")

# CORS for development (Vite dev server on port 5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/analyze", response_model=AnalyzeResponse)
def analyze_query(request: AnalyzeRequest):
    """Analyze an SQL query and return annotations, QEP, and AQPs."""
    query = request.query.strip().rstrip(";")
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    try:
        annotations, qep, aqps = generate_annotations(query)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return AnalyzeResponse(
        annotations=[convert_annotation(a) for a in annotations],
        qep=qep,
        aqps=[convert_aqp(a) for a in aqps],
        original_query=query,
    )


@app.get("/api/tables", response_model=TablesResponse)
def list_tables():
    """List all tables in the database."""
    try:
        tables = get_all_tables()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return TablesResponse(tables=tables)


# Serve React frontend static files (production build)
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend", "dist")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIR, "assets")), name="assets")

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        """Serve the React SPA for all non-API routes."""
        file_path = os.path.join(FRONTEND_DIR, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


def launch_gui():
    """Launch the web-based GUI via uvicorn."""
    print("=" * 60)
    print("SC3020 SQL Query Annotation Tool")
    print("Starting web server at http://127.0.0.1:8000")
    print("=" * 60)
    uvicorn.run(app, host="127.0.0.1", port=8000)
