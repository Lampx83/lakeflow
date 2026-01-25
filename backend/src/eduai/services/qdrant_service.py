from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from eduai.core.config import QDRANT_URL, QDRANT_API_KEY


# =====================================================
# CLIENT (singleton-ish)
# =====================================================

_client: Optional[QdrantClient] = None


def get_client() -> QdrantClient:
    global _client
    if _client is None:
        _client = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_API_KEY,
        )
    return _client


# =====================================================
# COLLECTIONS
# =====================================================

def list_collections() -> List[Dict[str, Any]]:
    """
    Danh sách collections trong Qdrant
    """
    client = get_client()
    resp = client.get_collections()

    return [
        {
            "name": c.name,
        }
        for c in resp.collections
    ]


def get_collection_detail(name: str) -> Dict[str, Any]:
    client = get_client()
    info = client.get_collection(name)

    vectors = {}
    params = info.config.params.vectors

    # Trường hợp 1: single vector
    if hasattr(params, "size"):
        vectors = {
            "default": {
                "size": params.size,
                "distance": str(params.distance),
            }
        }

    # Trường hợp 2: named vectors
    elif isinstance(params, dict):
        for k, v in params.items():
            vectors[k] = {
                "size": v.size,
                "distance": str(v.distance),
            }

    return {
        "name": name,
        "status": info.status,
        "vectors": vectors,   # ✅ JSON-safe
        "points_count": info.points_count,
        "indexed_vectors_count": info.indexed_vectors_count,
        "segments_count": info.segments_count,
    }




# =====================================================
# POINTS – BROWSE
# =====================================================

def list_points(
    collection: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Duyệt points theo offset/limit (debug, inspector)
    """
    client = get_client()

    points, _ = client.scroll(
        collection_name=collection,
        limit=limit,
        offset=offset,
        with_payload=True,
        with_vectors=False,  # inspector: không cần vector
    )

    return [_serialize_point(p) for p in points]


# =====================================================
# POINTS – FILTER
# =====================================================

def filter_points(
    collection: str,
    *,
    file_hash: Optional[str] = None,
    section_id: Optional[str] = None,
    chunk_id: Optional[int] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Filter points theo metadata payload
    """
    must: List[qmodels.FieldCondition] = []

    if file_hash:
        must.append(
            qmodels.FieldCondition(
                key="file_hash",
                match=qmodels.MatchValue(value=file_hash),
            )
        )

    if section_id:
        must.append(
            qmodels.FieldCondition(
                key="section_id",
                match=qmodels.MatchValue(value=section_id),
            )
        )

    if chunk_id is not None:
        must.append(
            qmodels.FieldCondition(
                key="chunk_id",
                match=qmodels.MatchValue(value=chunk_id),
            )
        )

    flt = qmodels.Filter(must=must) if must else None

    client = get_client()

    points, _ = client.scroll(
        collection_name=collection,
        scroll_filter=flt,
        limit=limit,
        with_payload=True,
        with_vectors=False,
    )

    return [_serialize_point(p) for p in points]


# =====================================================
# INTERNAL
# =====================================================

def _serialize_point(p) -> Dict[str, Any]:
    """
    Chuẩn hoá point để trả về API / UI
    """
    return {
        "id": p.id,
        "score": getattr(p, "score", None),
        "payload": p.payload or {},
    }
