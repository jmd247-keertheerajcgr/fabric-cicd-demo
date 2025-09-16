
import os
import msal
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
import csv
from pathlib import Path

raw_env = os.getenv("ENVIRONMENT", "dev").lower()

if raw_env.endswith("dev"):
    ENVIRONMENT = "dev"
elif raw_env.endswith("prod"):
    ENVIRONMENT = "prod"
else:
    ENVIRONMENT = raw_env
    
REPO_DIR = "."
ITEM_TYPES = ["Lakehouse", "Notebook", "Environment", "Warehouse", "DataPipeline", "SemanticModel", "Report"]

def get_workspace_id_by_env(env: str, csv_path: str) -> str:
    env = env.strip().lower()
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("Environment", "").strip().lower() == env:
                ws = row.get("Workspace ID", "").strip()
                if not ws:
                    raise ValueError(f"Workspace ID missing for env={env} in {csv_path}")
                return ws
    raise ValueError(f"No row for env={env} in {csv_path}")

def main():
    here = Path(__file__).resolve().parent
    csv_file = here / "workspace_data.csv"
    WORKSPACE_ID = get_workspace_id_by_env(ENVIRONMENT, csv_file)

    ws = FabricWorkspace(
        workspace_id=WORKSPACE_ID,
        environment=ENVIRONMENT,
        repository_directory=REPO_DIR,
        item_type_in_scope=ITEM_TYPES
    )

    publish_all_items(ws)
    # unpublish_all_orphan_items(ws)

if __name__ == "__main__":

    main()