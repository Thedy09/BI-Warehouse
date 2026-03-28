from __future__ import annotations

import argparse
import csv
from pathlib import Path

from google.cloud import bigquery


PROJECT_ROOT = Path(__file__).resolve().parents[1]
QUERIES_DIR = PROJECT_ROOT / "queries"


def render_query(path: Path, project: str, dataset: str) -> str:
    raw_sql = path.read_text(encoding="utf-8")
    return (
        raw_sql.replace("{{ target_project }}", project)
        .replace("{{ target_dataset }}", dataset)
        .strip()
    )


def export_query(client: bigquery.Client, sql_path: Path, project: str, dataset: str, output_dir: Path) -> Path:
    query = render_query(sql_path, project, dataset)
    job = client.query(query)
    result = job.result()

    destination = output_dir / f"{sql_path.stem.replace('export_', '')}.csv"
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([field.name for field in result.schema])
        for row in result:
            writer.writerow(list(row.values()))

    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description="Export selected dbt model outputs from BigQuery to CSV.")
    parser.add_argument("--project", required=True, help="Target GCP project that contains the dbt-built dataset.")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset that contains dbt model outputs.")
    parser.add_argument("--output-dir", default="artifacts", help="Local directory for CSV exports.")
    parser.add_argument("--location", default="US", help="BigQuery location.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    client = bigquery.Client(project=args.project, location=args.location)
    export_files = sorted(QUERIES_DIR.glob("export_*.sql"))

    if not export_files:
        raise SystemExit("No export SQL files were found in the queries directory.")

    for sql_path in export_files:
        destination = export_query(client, sql_path, args.project, args.dataset, output_dir)
        print(f"exported {destination}")


if __name__ == "__main__":
    main()
