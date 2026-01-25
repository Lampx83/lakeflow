import os
from pathlib import Path

API_BASE = os.getenv("API_BASE_URL", "http://eduai-backend:8011")
EDUAI_MODE = os.getenv("EDUAI_MODE", "DEV")

# =========================
# DATA ROOT (CRITICAL)
# =========================
DATA_ROOT = Path(
    os.getenv(
        "EDUAI_DATA_BASE_PATH",
        "/data",   # default cho Docker
    )
).expanduser().resolve()
