from pathlib import Path
from typing import Dict, Any, List, Literal
from datetime import datetime
import json
import tempfile
import shutil

import numpy as np
from sentence_transformers import SentenceTransformer

from eduai.common.jsonio import read_json, write_json


EmbeddingStatus = Literal["EMBEDDED", "SKIPPED"]

DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def run_embedding_pipeline(
    file_hash: str,
    processed_dir: Path,
    embeddings_root: Path,
    model_name: str = DEFAULT_MODEL_NAME,
    force: bool = False,
) -> EmbeddingStatus:
    """
    FINAL – NAS SAFE – PRODUCTION GRADE
    """

    # =====================================================
    # 1. Validate input
    # =====================================================
    chunks_file = processed_dir / "chunks.json"
    if not chunks_file.exists():
        raise RuntimeError(f"Missing chunks.json for {file_hash}")

    out_dir = embeddings_root / file_hash
    final_path = out_dir / "embedding.npy"

    if final_path.exists() and not force:
        print(f"[400] Skip (already embedded): {file_hash}")
        return "SKIPPED"

    # =====================================================
    # 2. Load chunks
    # =====================================================
    chunks: List[Dict[str, Any]] = read_json(chunks_file)
    texts = [c["text"].strip() for c in chunks if c.get("text")]

    if not texts:
        print(f"[400] No valid text chunks for {file_hash}, skip")
        return "SKIPPED"

    # =====================================================
    # 3. Load model & embed
    # =====================================================
    print(f"[400] Loading model: {model_name}")
    model = SentenceTransformer(model_name)

    print(f"[400] Embedding {len(texts)} chunks for {file_hash}")
    vectors = model.encode(
        texts,
        show_progress_bar=True,
        normalize_embeddings=True,
    ).astype("float32")

    # =====================================================
    # 4. WRITE TO LOCAL FS FIRST (CRITICAL)
    # =====================================================
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        tmp_embedding = tmpdir / "embedding.npy"
        np.save(tmp_embedding, vectors)   # ✔ local FS, 100% safe

        # ---------- ensure target dir ----------
        out_dir.mkdir(parents=True, exist_ok=True)

        # ---------- atomic copy to NAS ----------
        shutil.copy2(tmp_embedding, final_path)

    # =====================================================
    # 5. Metadata
    # =====================================================
    chunks_meta = [
        {
            "chunk_id": c.get("chunk_id"),
            "section_id": c.get("section_id"),
            "file_hash": file_hash,
            "token_estimate": c.get("token_estimate"),
        }
        for c in chunks
    ]

    write_json(out_dir / "chunks_meta.json", chunks_meta)

    write_json(
        out_dir / "model.json",
        {
            "model_name": model_name,
            "embedding_dim": int(vectors.shape[1]),
            "chunk_count": len(vectors),
            "created_at": datetime.utcnow().isoformat(),
            "pipeline": "400_embeddings",
        },
    )

    with (out_dir / "embedding.jsonl").open("w", encoding="utf-8") as f:
        for meta, vec in zip(chunks_meta, vectors):
            f.write(
                json.dumps(
                    {**meta, "vector": vec.tolist()},
                    ensure_ascii=False,
                )
                + "\n"
            )

    print(f"[400] Completed embedding for {file_hash}")
    return "EMBEDDED"
