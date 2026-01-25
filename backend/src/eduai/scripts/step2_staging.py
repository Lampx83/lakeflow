"""
Step 2 – Processing
200_staging → 300_processed
"""

from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

from eduai.runtime.config import runtime_config
from eduai.pipelines.processing.pipeline import run_processed_pipeline
from eduai.config import paths
from eduai.common.raw_finder import find_raw_file


# ======================================================
# BOOTSTRAP RUNTIME CONFIG (BẮT BUỘC)
# ======================================================

data_base = os.getenv("EDUAI_DATA_BASE_PATH")
if not data_base:
    raise RuntimeError(
        "EDUAI_DATA_BASE_PATH is not set. "
        "Example: export EDUAI_DATA_BASE_PATH=/path/to/data_lake"
    )

base_path = Path(data_base).expanduser().resolve()
runtime_config.set_data_base_path(base_path)

print(f"[BOOT] DATA_BASE_PATH = {base_path}")


# ======================================================
# MAIN
# ======================================================

def main():
    print("=== RUN 300_PROCESSED PIPELINE ===")

    staging_root = paths.staging_path()
    raw_root = paths.raw_path()
    processed_root = paths.processed_path()

    print(f"[DEBUG] STAGING_PATH   = {staging_root}")
    print(f"[DEBUG] RAW_PATH       = {raw_root}")
    print(f"[DEBUG] PROCESSED_PATH = {processed_root}")

    if not staging_root.exists():
        raise RuntimeError(f"STAGING_PATH does not exist: {staging_root}")

    processed_count = 0

    staging_dirs = list(staging_root.iterdir())
    print(f"[DEBUG] Found {len(staging_dirs)} staging dirs")

    for staging_dir in staging_dirs:
        if not staging_dir.is_dir():
            continue

        file_hash = staging_dir.name
        raw_file = find_raw_file(file_hash, raw_root)

        if raw_file is None:
            print(f"[SKIP] Raw file not found for {file_hash}")
            continue

        try:
            run_processed_pipeline(
                file_hash=file_hash,
                raw_file_path=raw_file,
                staging_dir=staging_dir,
                processed_root=processed_root,
                force=True,
            )
            processed_count += 1

        except Exception as exc:
            print(f"[ERROR] Failed processing {file_hash}: {exc}")

    print("=================================")
    print(f"=== DONE. Processed files: {processed_count} ===")
    print("=================================")


if __name__ == "__main__":
    main()
