"""
Reset Checkpoints Script
------------------------
This helper script deletes all checkpoint JSON files
for CRM, ERP, Salesforce, and SAP in one command.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHECKPOINT_DIR = PROJECT_ROOT / "storage" / "checkpoints"

def reset_checkpoints():
    if not CHECKPOINT_DIR.exists():
        print(f"Checkpoint directory does not exist: {CHECKPOINT_DIR}")
        return

    deleted = []
    for file in CHECKPOINT_DIR.glob("*_checkpoint.json"):
        try:
            os.remove(file)
            deleted.append(file.name)
        except Exception as e:
            print(f"Failed to delete {file}: {e}")

    if deleted:
        print("Deleted checkpoint files:")
        for f in deleted:
            print(f"  - {f}")
    else:
        print("No checkpoint files found to delete.")

if __name__ == "__main__":
    reset_checkpoints()
