import os
import socket
from pathlib import Path

def _resolve_api_base() -> str:
    base = os.getenv("API_BASE_URL", "http://localhost:8011")
    # Khi chạy dev trên host, "eduai-backend" không resolve → dùng localhost
    if "eduai-backend" in base:
        try:
            socket.gethostbyname("eduai-backend")
        except socket.gaierror:
            base = "http://localhost:8011"
    return base

API_BASE = _resolve_api_base()
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
