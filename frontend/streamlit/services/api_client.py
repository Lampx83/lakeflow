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


def semantic_search(
    query: str,
    top_k: int,
    token: str,
    *,
    collection_name: str | None = None,
    score_threshold: float | None = None,
):
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"query": query, "top_k": top_k}
    if collection_name:
        payload["collection_name"] = collection_name
    if score_threshold is not None:
        payload["score_threshold"] = score_threshold
    resp = requests.post(
        f"{API_BASE}/search/semantic",
        json=payload,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def qa(
    question: str,
    top_k: int,
    temperature: float,
    token: str,
    *,
    collection_name: str | None = None,
    score_threshold: float | None = None,
):
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "question": question,
        "top_k": top_k,
        "temperature": temperature,
    }
    if collection_name:
        payload["collection_name"] = collection_name
    if score_threshold is not None:
        payload["score_threshold"] = score_threshold
    resp = requests.post(
        f"{API_BASE}/search/qa",
        json=payload,
        headers=headers,
        timeout=90,  # Q&A: embedding + search + LLM
    )
    resp.raise_for_status()
    return resp.json()
