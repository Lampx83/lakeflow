"""
Step 0 – Inbox Ingestion
000_inbox → 100_raw
"""

from pathlib import Path
import os

from eduai.runtime.config import runtime_config
from eduai.catalog.db import get_connection, init_db
from eduai.pipelines.ingesting.pipeline import run_ingestion
from eduai.config import paths

from dotenv import load_dotenv
load_dotenv()

# ======================================================
# BOOTSTRAP RUNTIME CONFIG (BẮT BUỘC)
# ======================================================

data_base = os.getenv("EDUAI_DATA_BASE_PATH")
if not data_base:
    raise RuntimeError(
        "EDUAI_DATA_BASE_PATH is not set. "
        "Example: export EDUAI_DATA_BASE_PATH=/path/to/data_lake"
    )

data_base_path = Path(data_base).expanduser().resolve()
runtime_config.set_data_base_path(data_base_path)

print(f"[BOOT] DATA_BASE_PATH = {data_base_path}")


# ======================================================
# INIT CATALOG DB
# ======================================================

conn = get_connection(paths.catalog_db_path())
init_db(conn)


# ======================================================
# RUN INGESTION
# ======================================================

print("=== RUN INGESTION (000_inbox → 100_raw) ===")

before = conn.execute(
    "SELECT COUNT(*) FROM raw_objects"
).fetchone()[0]

run_ingestion(
    inbox_root=paths.inbox_path(),
    raw_root=paths.raw_path(),
    conn=conn,
)

after = conn.execute(
    "SELECT COUNT(*) FROM raw_objects"
).fetchone()[0]

print("\n📦 INGESTION SUMMARY")
print(f"Files before : {before}")
print(f"Files after  : {after}")
print(f"New ingested : {after - before}")
