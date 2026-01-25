"""
Step 4 – Qdrant Ingest
400_embeddings → Qdrant

SOURCE OF TRUTH:
- Vectors + meta : 400_embeddings
- Text chunks    : 300_processed
"""

from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

import numpy as np
from qdrant_client import QdrantClient

from eduai.runtime.config import runtime_config
from eduai.config import paths
from eduai.vectorstore.qdrant_ingest import (
    ingest_file_embeddings,
    ensure_collection,
)


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
# QDRANT CONFIG
# ======================================================

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))


# ======================================================
# MAIN
# ======================================================

def main():
    print("=== RUN QDRANT INGEST (400 → Qdrant) ===")

    embeddings_root = paths.embeddings_path()
    processed_root = paths.processed_path()

    print(f"[DEBUG] EMBEDDINGS_PATH = {embeddings_root}")
    print(f"[DEBUG] PROCESSED_PATH  = {processed_root}")

    if not embeddings_root.exists():
        raise RuntimeError(
            f"EMBEDDINGS_PATH does not exist: {embeddings_root}"
        )

    if not processed_root.exists():
        raise RuntimeError(
            f"PROCESSED_PATH does not exist: {processed_root}"
        )

    # -------------------------
    # Connect to Qdrant
    # -------------------------
    try:
        client = QdrantClient(
            host=QDRANT_HOST,
            port=QDRANT_PORT,
        )
        client.get_collections()  # ping
    except Exception as exc:
        raise RuntimeError(
            f"Cannot connect to Qdrant at {QDRANT_HOST}:{QDRANT_PORT}"
        ) from exc

    ingested = skipped = failed = 0

    emb_dirs = list(embeddings_root.iterdir())
    print(f"[DEBUG] Found {len(emb_dirs)} embedding dirs")

    # -------------------------
    # Iterate over embeddings
    # -------------------------
    for emb_dir in emb_dirs:
        if not emb_dir.is_dir():
            continue

        file_hash = emb_dir.name
        embeddings_file = emb_dir / "embedding.npy"

        print(f"\n[QDRANT] Processing {file_hash}")

        # ---------- Skip: no embedding ----------
        if not embeddings_file.exists():
            print(f"[QDRANT][SKIP] No embedding.npy for {file_hash}")
            skipped += 1
            continue

        try:
            # ---------- Load vectors ----------
            vectors = np.load(embeddings_file)
            if vectors.ndim != 2:
                raise RuntimeError(
                    f"Invalid embedding shape for {file_hash}"
                )

            # ---------- Ensure collection ----------
            ensure_collection(
                client=client,
                vector_dim=vectors.shape[1],
            )

            # ---------- Ingest ----------
            count = ingest_file_embeddings(
                client=client,
                file_hash=file_hash,
                embeddings_dir=emb_dir,
                processed_root=processed_root,
            )

            print(
                f"[QDRANT][OK] {file_hash}: "
                f"{count} vectors ingested"
            )
            ingested += 1

        except Exception as exc:
            failed += 1
            print(
                f"[QDRANT][FAIL] {file_hash}: {exc}"
            )

    # -------------------------
    # Summary
    # -------------------------
    print("\n=================================")
    print("QDRANT INGEST SUMMARY")
    print(f"Ingested : {ingested}")
    print(f"Skipped  : {skipped}")
    print(f"Failed   : {failed}")
    print("=================================")


if __name__ == "__main__":
    main()
