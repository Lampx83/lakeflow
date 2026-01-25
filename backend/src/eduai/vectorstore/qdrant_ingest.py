from pathlib import Path
from typing import List, Dict, Any
import uuid

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct,
    VectorParams,
    Distance,
)

from eduai.common.jsonio import read_json
from eduai.vectorstore.constants import COLLECTION_NAME


# =====================================================
# COLLECTION MANAGEMENT
# =====================================================

def ensure_collection(
    client: QdrantClient,
    vector_dim: int,
) -> None:
    """
    Ensure Qdrant collection exists.
    If already exists → do nothing.
    """

    collections = client.get_collections().collections
    if any(c.name == COLLECTION_NAME for c in collections):
        return

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=vector_dim,
            distance=Distance.COSINE,
        ),
    )


# =====================================================
# INGEST EMBEDDINGS (FINAL, CORRECT VERSION)
# =====================================================

def ingest_file_embeddings(
    client: QdrantClient,
    file_hash: str,
    embeddings_dir: Path,
    processed_root: Path,
) -> int:
    """
    Ingest embeddings of one file into Qdrant.

    Source of truth:
    - Vectors + meta: 400_embeddings/<file_hash>
    - Text chunks   : 300_processed/<file_hash>/chunks.json

    Returns
    -------
    int
        Number of vectors ingested
    """

    # --------------------------------------------------
    # Paths
    # --------------------------------------------------

    embeddings_file = embeddings_dir / "embedding.npy"
    meta_file = embeddings_dir / "chunks_meta.json"
    processed_chunks_file = (
        processed_root / file_hash / "chunks.json"
    )

    if not embeddings_file.exists():
        raise FileNotFoundError(
            f"Missing embedding.npy for {file_hash}"
        )

    if not meta_file.exists():
        raise FileNotFoundError(
            f"Missing chunks_meta.json for {file_hash}"
        )

    if not processed_chunks_file.exists():
        raise FileNotFoundError(
            f"Missing 300_processed chunks.json for {file_hash}"
        )

    # --------------------------------------------------
    # Load data
    # --------------------------------------------------

    vectors = np.load(embeddings_file)

    chunks_meta: List[Dict[str, Any]] = read_json(meta_file)
    chunks: List[Dict[str, Any]] = read_json(processed_chunks_file)

    if len(vectors) != len(chunks_meta):
        raise RuntimeError(
            f"Vector/meta count mismatch for {file_hash}: "
            f"{len(vectors)} vectors vs {len(chunks_meta)} meta"
        )

    # --------------------------------------------------
    # Build chunk_id → text map (SOURCE OF TRUTH)
    # --------------------------------------------------

    chunk_text_map = {
        c["chunk_id"]: c.get("text", "")
        for c in chunks
    }

    # --------------------------------------------------
    # Build Qdrant points
    # --------------------------------------------------

    points: List[PointStruct] = []

    for vec, meta in zip(vectors, chunks_meta):
        chunk_id = meta["chunk_id"]

        # Deterministic UUID (safe for re-run)
        point_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{file_hash}:{chunk_id}"
            )
        )

        payload = {
            "file_hash": file_hash,
            "chunk_id": chunk_id,
            "section_id": meta.get("section_id"),
            "token_estimate": meta.get("token_estimate"),
            "text": chunk_text_map.get(chunk_id),  # 🔑 CRITICAL
            "source": "EDUAI",
        }

        points.append(
            PointStruct(
                id=point_id,
                vector=vec.tolist(),
                payload=payload,
            )
        )

    # --------------------------------------------------
    # Upsert to Qdrant
    # --------------------------------------------------

    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points,
    )

    return len(points)
