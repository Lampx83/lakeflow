import requests
from config.settings import API_BASE

def login(username: str, password: str) -> str | None:
    resp = requests.post(
        f"{API_BASE}/auth/login",
        json={"username": username, "password": password},
        timeout=10,
    )
    if resp.status_code == 200:
        return resp.json()["access_token"]
    return None


def semantic_search(query: str, top_k: int, token: str):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(
        f"{API_BASE}/search/semantic",
        json={"query": query, "top_k": top_k},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()
