"""
Step 1 – PDF Staging
100_raw → 200_staging
"""

from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

from eduai.runtime.config import runtime_config
from eduai.pipelines.staging.pipeline import run_pdf_staging
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
# HELPERS
# ======================================================

def extract_file_hash(pdf_path: Path) -> str:
    return pdf_path.stem


def already_staged(file_hash: str) -> bool:
    return (paths.staging_path() / file_hash / "validation.json").exists()


# ======================================================
# MAIN
# ======================================================

def main():
    print("=== RUN PDF STAGING (200_staging) ===")

    raw_root = paths.raw_path()
    print(f"[DEBUG] RAW_PATH = {raw_root}")

    if not raw_root.exists():
        raise RuntimeError(f"RAW_PATH does not exist: {raw_root}")

    processed = skipped = failed = 0

    pdf_files = list(raw_root.rglob("*.pdf"))
    print(f"[DEBUG] Found {len(pdf_files)} PDF files")

    for pdf_path in pdf_files:
        file_hash = extract_file_hash(pdf_path)

        if already_staged(file_hash):
            print(f"[STAGING][SKIP] Already staged: {file_hash}")
            skipped += 1
            continue

        print(f"[STAGING][PDF] Processing: {pdf_path}")

        try:
            run_pdf_staging(
                file_hash=file_hash,
                raw_pdf_path=pdf_path,
                staging_root=paths.staging_path(),
            )
            processed += 1

        except Exception as exc:
            failed += 1
            print(
                f"[STAGING][ERROR] Failed processing {pdf_path}\n"
                f"                Reason: {exc}"
            )

    print("=================================")
    print(f"PDF processed : {processed}")
    print(f"PDF skipped   : {skipped}")
    print(f"PDF failed    : {failed}")
    print("=================================")


if __name__ == "__main__":
    main()
