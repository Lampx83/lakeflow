import requests
from config.settings import API_BASE

def run_pipeline_step(step: str) -> dict:
    resp = requests.post(
        f"{API_BASE}/pipeline/run/{step}",
        timeout=3600,
    )
    resp.raise_for_status()
    return resp.json()
