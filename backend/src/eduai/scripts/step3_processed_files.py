"""
Step 3 – Embeddings
300_processed → 400_embeddings
"""

from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

from eduai.runtime.config import runtime_config
from eduai.pipelines.embedding.pipeline import run_embedding_pipeline
from eduai.config import paths


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
    print("=== RUN 400_EMBEDDINGS PIPELINE ===")

    processed_root = paths.processed_path()
    embeddings_root = paths.embeddings_path()

    print(f"[DEBUG] PROCESSED_PATH  = {processed_root}")
    print(f"[DEBUG] EMBEDDINGS_PATH = {embeddings_root}")

    if not processed_root.exists():
        raise RuntimeError(f"PROCESSED_PATH does not exist: {processed_root}")

    embedded = skipped = failed = 0

    processed_dirs = list(processed_root.iterdir())
    print(f"[DEBUG] Found {len(processed_dirs)} processed dirs")

    for processed_dir in processed_dirs:
        if not processed_dir.is_dir():
            continue

        file_hash = processed_dir.name
        print(f"[400] Processing: {file_hash}")

        try:
            result = run_embedding_pipeline(
                file_hash=file_hash,
                processed_dir=processed_dir,
                embeddings_root=embeddings_root,
                force=False,
            )

            if result == "SKIPPED":
                skipped += 1
                print(f"[400][SKIP] Already embedded: {file_hash}")
            else:
                embedded += 1
                print(f"[400][OK] Embedded: {file_hash}")

        except Exception as exc:
            failed += 1
            print(f"[400][ERROR] {file_hash}: {exc}")

    print("=================================")
    print(f"Embedded files : {embedded}")
    print(f"Skipped        : {skipped}")
    print(f"Failed         : {failed}")
    print("=================================")


if __name__ == "__main__":
    main()
