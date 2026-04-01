"""
project.py
Main entry point for the SQL Query Annotation Tool.
SC3020 Database System Principles - Project 2

Run this file to launch the application:
    python project.py          # Launches web GUI (FastAPI + React)
    python project.py --cli    # Launches CLI mode
"""

import sys


def cli_mode():
    """CLI mode for testing the annotation engine without the GUI."""
    from annotation import generate_annotations

    print("=" * 60)
    print("SC3020 SQL Query Annotation Tool (CLI Mode)")
    print("=" * 60)
    print("Enter an SQL query (or 'quit' to exit):")
    print()

    while True:
        try:
            query = input("SQL> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not query or query.lower() == "quit":
            break

        # Allow multi-line input (end with semicolon)
        while not query.endswith(";"):
            try:
                line = input("...> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            query += " " + line

        # Remove trailing semicolon for EXPLAIN
        query = query.rstrip(";")

        try:
            annotations, qep, aqps = generate_annotations(query)

            print(f"\n{'=' * 60}")
            print(f"QEP Total Cost: {qep[0]['Plan']['Total Cost']:.2f}")
            print(f"AQPs Generated: {len(aqps)}")
            print(f"Annotations: {len(annotations)}")
            print(f"{'=' * 60}")

            for i, ann in enumerate(annotations, 1):
                comp = ann.component
                print(f"\n--- Annotation {i}: [{comp.component_type.upper()}] ---")
                print(f"  SQL: {comp.sql_text}")
                print(f"  Plan Node: {ann.plan_node.node_type} (cost: {ann.qep_cost:.2f})")
                print(f"  HOW: {ann.how}")
                if ann.why:
                    print(f"  WHY: {ann.why}")
                if ann.alternative_costs:
                    print(f"  Alternatives: {ann.alternative_costs}")

            print(f"\n{'=' * 60}\n")

        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
            print()


def main():
    if "--cli" in sys.argv:
        cli_mode()
    else:
        from interface import launch_gui
        launch_gui()


if __name__ == "__main__":
    main()
