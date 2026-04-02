# SC3020 Project 2 — SQL Query Annotation Tool

A tool that analyzes SQL queries by comparing PostgreSQL Query Execution Plans (QEP) with Alternative Query Plans (AQP), then annotates the query explaining **how** each component is executed and **why** certain operators were chosen.

## Prerequisites

- Python 3.11+
- PostgreSQL 16
- Node.js 18+ (for building the frontend)

## Setup

### 1. Install Python dependencies

```bash
pip install psycopg2-binary sqlparse fastapi uvicorn
```

### 2. Set up PostgreSQL with TPC-H data

Ensure PostgreSQL is running and a database named `TPC-H` exists with the TPC-H schema loaded (region, nation, part, supplier, partsupp, customer, orders, lineitem).

Update the connection settings in `preprocessing.py` if needed:

```python
DB_CONFIG = {
    "dbname": "TPC-H",
    "user": "your_username",
    "host": "localhost",
    "port": 5432,
}
```

### 3. Build the frontend

```bash
cd frontend
npm install
npm run build
cd ..
```

## Usage

### Web GUI (default)

```bash
python project.py
```

Opens the web interface at **http://127.0.0.1:8000**.

### CLI mode

```bash
python project.py --cli
```

Enter SQL queries interactively; annotations are printed to the terminal.

## Project Structure

```
project.py          Main entry point (launches web server or CLI)
interface.py        FastAPI app, API endpoints, static file serving
annotation.py       Annotation generation (HOW/WHY), node-to-SQL mapping
preprocessing.py    DB connection, SQL parsing, QEP/AQP tree walking
frontend/           React app (Vite)
```
