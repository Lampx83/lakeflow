from fastapi import APIRouter
import requests

from eduai.api.schemas.search import (
    SemanticSearchRequest,
    SemanticSearchResponse,
)
from eduai.api.deps import get_embedding_model
from eduai.vectorstore.constants import COLLECTION_NAME
from eduai.core.config import QDRANT_URL

router = APIRouter(
    prefix="/search",
    tags=["Search"],
)


@router.post(
    "/semantic",
    response_model=SemanticSearchResponse,
)
def semantic_search(req: SemanticSearchRequest):
    """
    Semantic search dùng Qdrant REST API (requests)
    """

    # --------------------------------------------------
    # 1. Embed query
    # --------------------------------------------------
    model = get_embedding_model()

    query_vector = model.encode(
        req.query,
        normalize_embeddings=True,
    ).tolist()

    # --------------------------------------------------
    # 2. Call Qdrant REST API
    # --------------------------------------------------
    url = f"{QDRANT_URL}/collections/{COLLECTION_NAME}/points/search"

    payload = {
        "vector": query_vector,
        "limit": req.top_k,
        "with_payload": True,
        "with_vector": False,
    }

    try:
        resp = requests.post(
            url,
            json=payload,
            timeout=10,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Qdrant search failed: {exc}")

    data = resp.json()

    # --------------------------------------------------
    # 3. Parse response
    # --------------------------------------------------
    points = data.get("result", [])

    results = []
    for p in points:
        payload = p.get("payload", {}) or {}

        results.append({
            "score": float(p.get("score", 0.0)),
            "file_hash": payload.get("file_hash"),
            "chunk_id": payload.get("chunk_id"),
            "section_id": payload.get("section_id"),
            "text": payload.get("text"),
            "token_estimate": payload.get("token_estimate"),
        })

    return {
        "query": req.query,
        "results": results,
    }
