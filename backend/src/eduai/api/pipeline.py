from fastapi import APIRouter, HTTPException
import subprocess
import sys
import os
from pathlib import Path

router = APIRouter()

from pathlib import Path

SCRIPTS_DIR = (
    Path(__file__).resolve()
    .parents[1]      # eduai/
    / "scripts"
)

ALLOWED = {
    "step0": "step0_inbox.py",
    "step1": "step1_raw.py",
    "step2": "step2_staging.py",
    "step3": "step3_processed_files.py",
    "step4": "step3_processed_qdrant.py",
}

@router.post("/run/{step}")
def run_step(step: str):
    if step not in ALLOWED:
        raise HTTPException(status_code=400, detail="Invalid step")

    script_path = SCRIPTS_DIR / ALLOWED[step]
    if not script_path.exists():
        raise HTTPException(status_code=404, detail=f"Script not found: {script_path}")

    env = os.environ.copy()
    # backend Dockerfile đã đặt PYTHONPATH=/app/src, nhưng set lại cho chắc
    env["PYTHONPATH"] = env.get("PYTHONPATH", "/app/src")

    try:
        p = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            env=env,
            cwd=Path(__file__).resolve().parents[3],
            timeout=60 * 60,  # 1h tuỳ nhu cầu
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Pipeline step timed out")

    return {
        "step": step,
        "script": ALLOWED[step],
        "returncode": p.returncode,
        "stdout": p.stdout,
        "stderr": p.stderr,
    }
