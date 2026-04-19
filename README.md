# SC3020 Project 2 , SQL Query Annotation Tool

A tool that analyzes SQL queries by comparing PostgreSQL Query Execution Plans (QEP) with Alternative Query Plans (AQP), then annotates the query explaining **how** each component is executed and **why** certain operators were chosen.

## Running the tool

There are 2 ways to run the tool:
- **Docker:** run the tool in a Docker container
- **From source:** run the tool from source

## Docker

Prerequisites: Docker. You also need a running populated PostgreSQL database , this tool does not create one.

```bash
docker build -t sc3020 .
docker run --rm -p 8000:8000 sc3020
```

Open **http://localhost:8000** and fill in the **Database Connection** panel at the top:

- **Host:**
  - Mac/Windows: `host.docker.internal`
  - Linux: re-run with `--network=host` and use `localhost`, or add `--add-host=host.docker.internal:host-gateway`
- **Port / Database / User / Password:** whatever matches your Postgres.

Click **Connect** , on success, the SQL query panel appears and you can start analyzing queries.

## Run from source

Prerequisites: Python 3.11+, Node.js 18+, PostgreSQL 16.

```bash
pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
python project.py
```

Then open http://127.0.0.1:8000 and enter your database connection in the UI.

### CLI mode

```bash
python project.py --cli
```

Enter SQL queries interactively; annotations are printed to the terminal. CLI mode uses the fallback defaults in `preprocessing.py` , edit them if your DB differs.

## Project Structure

```
project.py          Main entry point (launches web server or CLI)
interface.py        FastAPI app, API endpoints, static file serving
annotation.py       Annotation generation (HOW/WHY), node-to-SQL mapping
preprocessing.py    DB connection, SQL parsing, QEP/AQP tree walking
frontend/           React app (Vite)
Dockerfile          Multi-stage build (Node, Python)
requirements.txt    Pinned Python deps
sample_queries.sql  Sample AI Generated queries for testing
```
