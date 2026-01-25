# frontend/streamlit/services/qdrant_service.py

import requests
from typing import List, Dict, Any, Optional

from config.settings import API_BASE


# =====================================================
# INTERNAL
# =====================================================

def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# =====================================================
# COLLECTIONS
# =====================================================

def list_collections(token: str):
    resp = requests.get(
        f"{API_BASE}/qdrant/collections",
        headers=_headers(token),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["collections"]




def get_collection_detail(
    collection: str,
    token: str,
) -> Dict[str, Any]:
    resp = requests.get(
        f"{API_BASE}/qdrant/collections/{collection}",
        headers=_headers(token),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# =====================================================
# POINTS – BROWSE
# =====================================================

def list_points(
    collection: str,
    token: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    resp = requests.get(
        f"{API_BASE}/qdrant/collections/{collection}/points",
        params={
            "limit": limit,
            "offset": offset,
        },
        headers=_headers(token),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["points"]


# =====================================================
# POINTS – FILTER
# =====================================================

def filter_points(
    collection: str,
    token: str,
    *,
    file_hash: Optional[str] = None,
    section_id: Optional[str] = None,
    chunk_id: Optional[int] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    payload = {
        "file_hash": file_hash,
        "section_id": section_id,
        "chunk_id": chunk_id,
        "limit": limit,
    }

    resp = requests.post(
        f"{API_BASE}/qdrant/collections/{collection}/filter",
        json=payload,
        headers=_headers(token),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["points"]
