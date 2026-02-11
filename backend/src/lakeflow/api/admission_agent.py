"""
Trợ lý (Agent) Admission – API tương thích Research Agent: /metadata, /data, /ask.
Sử dụng Qwen3 8b, dữ liệu từ collection "Admission" trong Qdrant.
"""

import time
from typing import Any, Optional

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from lakeflow.api.deps import get_embedding_model
from lakeflow.core.config import get_qdrant_url, LLM_BASE_URL, LLM_MODEL, QDRANT_API_KEY
from lakeflow.services.qdrant_service import get_client

ADMISSION_COLLECTION = "Admission"
ADMISSION_AGENT_LLM_MODEL = "qwen3:8b"

router = APIRouter(
    prefix="/admission_agent/v1",
    tags=["admission-agent"],
)


# ---------------------------------------------------------------------------
# Schemas (tương thích Research agent)
# ---------------------------------------------------------------------------

class AskRequest(BaseModel):
    session_id: Optional[str] = None
    model_id: Optional[str] = None
    user: Optional[str] = None
    prompt: str = Field(..., description="Câu hỏi của người dùng")
    context: Optional[dict] = None


# ---------------------------------------------------------------------------
# GET /metadata
# ---------------------------------------------------------------------------

@router.get("/metadata")
def get_metadata() -> dict:
    """
    Metadata của Trợ lý Admission (tương thích Research agent).
    """
    return {
        "name": "Admission",
        "description": "Trả lời câu hỏi về tuyển sinh, quy chế tuyển sinh và tài liệu liên quan. Dữ liệu lấy từ collection Admission trong Qdrant.",
        "version": "1.0.0",
        "developer": "LakeFlow",
        "capabilities": ["admission", "tuyen sinh", "quy che", "tai lieu"],
        "supported_models": [
            {
                "model_id": "qwen3:8b",
                "name": "Qwen3 8B",
                "description": "Mô hình Ollama cho hỏi đáp dựa trên tài liệu Admission",
                "accepted_file_types": [],
            },
        ],
        "sample_prompts": [
            "Điều kiện tuyển sinh đại học chính quy là gì?",
            "Thời gian nộp hồ sơ tuyển sinh năm nay?",
            "Các ngành đào tạo và chỉ tiêu tuyển sinh?",
        ],
        "provided_data_types": [
            {"type": "qdrant_collection", "description": "Collection Admission trong Qdrant"},
        ],
        "contact": "",
        "status": "active",
    }


# ---------------------------------------------------------------------------
# GET /data – danh sách nguồn dữ liệu (từ collection Admission)
# ---------------------------------------------------------------------------

def _collect_sources_from_collection(collection: str, limit: int = 500) -> list[dict]:
    """Scroll collection, thu thập các nguồn (source) duy nhất từ payload."""
    sources_seen: set[str] = set()
    items: list[dict] = []
    try:
        client = get_client()
        points, _ = client.scroll(
            collection_name=collection,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
        for p in points:
            payload = p.payload or {}
            source = payload.get("source") or payload.get("title") or payload.get("file_hash") or ""
            if isinstance(source, str) and source.strip() and source not in sources_seen:
                sources_seen.add(source)
                items.append({
                    "id": source,
                    "name": source,
                    "description": f"Nguồn: {source}",
                })
    except Exception:
        pass
    if not items:
        items = [
            {
                "id": "admission",
                "name": "Collection Admission",
                "description": "Dữ liệu từ Qdrant collection Admission (tài liệu tuyển sinh).",
            },
        ]
    return items


@router.get("/data")
def get_data(type: Optional[str] = None) -> dict:
    """
    Trả về danh sách nguồn dữ liệu (từ collection Admission trong Qdrant).
    Tương thích Research agent GET /data.
    """
    items = _collect_sources_from_collection(ADMISSION_COLLECTION)
    return {
        "status": "success",
        "data_type": type or "sources",
        "items": items,
    }


# ---------------------------------------------------------------------------
# POST /ask – RAG: embed câu hỏi → search Admission → LLM (Qwen3 8b)
# ---------------------------------------------------------------------------

def _search_admission_collection(query_vector: list[float], top_k: int = 5) -> list[dict]:
    """Tìm kiếm vector trong collection Admission."""
    base = get_qdrant_url(None)
    url = f"{base}/collections/{ADMISSION_COLLECTION}/points/search"
    body = {
        "vector": query_vector,
        "limit": top_k,
        "with_payload": True,
        "with_vector": False,
    }
    headers = {"Content-Type": "application/json"}
    if QDRANT_API_KEY:
        headers["api-key"] = QDRANT_API_KEY
    resp = requests.post(url, json=body, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("result", [])


def _text_from_payload(payload: dict) -> str:
    """Lấy nội dung text từ payload (tương thích LakeFlow chunk payload)."""
    if not payload:
        return ""
    return (payload.get("text") or payload.get("content") or "").strip()


@router.post("/ask")
def ask(req: AskRequest) -> dict:
    """
    Hỏi đáp RAG: embed câu hỏi → tìm trong collection Admission → tổng hợp bằng Qwen3 8b.
    Trả về format tương thích Research agent (session_id, status, content_markdown, meta).
    """
    t0 = time.time()
    prompt = (req.prompt or "").strip()
    if not prompt:
        return {
            "session_id": req.session_id,
            "status": "error",
            "error_code": "INVALID_REQUEST",
            "error_message": "Thiếu nội dung câu hỏi (prompt).",
        }

    try:
        # 1. Embed câu hỏi
        model = get_embedding_model()
        query_vector = model.encode(
            prompt,
            normalize_embeddings=True,
        ).tolist()

        # 2. Search collection Admission
        points = _search_admission_collection(query_vector, top_k=5)
        sources: list[str] = []
        context_parts: list[str] = []
        for i, p in enumerate(points):
            payload = p.get("payload") or {}
            text = _text_from_payload(payload)
            if text:
                context_parts.append(f"[Đoạn {i + 1}]\n{text}")
                src = payload.get("source") or payload.get("title") or f"Kết quả {i + 1}"
                sources.append(str(src) if not isinstance(src, str) else src)

        context_text = (
            "\n\n---\n\n".join(context_parts)
            if context_parts
            else "Không tìm thấy đoạn tài liệu nào phù hợp với câu hỏi trong cơ sở dữ liệu."
        )

        # 3. LLM (Qwen3 8b qua Ollama)
        system_prompt = """Bạn là trợ lý tra cứu thông tin tuyển sinh và tài liệu liên quan. Nhiệm vụ của bạn:
- Trả lời CHỈ dựa trên các đoạn ngữ liệu được cung cấp bên dưới.
- Nếu thông tin không có trong ngữ liệu, hãy nói rõ "Trong cơ sở dữ liệu hiện tại không có thông tin về..." và gợi ý liên hệ phòng ban tuyển sinh.
- Trích dẫn rõ ràng, có thể đánh số hoặc gạch đầu dòng.
- Trả lời bằng tiếng Việt."""

        user_message = f"""Dựa trên các đoạn trích sau từ tài liệu, hãy trả lời câu hỏi của người dùng.

=== NGỮ LIỆU ===
{context_text}

=== CÂU HỎI ===
{prompt}"""

        chat_url = f"{LLM_BASE_URL.rstrip('/')}/v1/chat/completions"
        llm_payload = {
            "model": ADMISSION_AGENT_LLM_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 1500,
        }

        llm_resp = requests.post(
            chat_url,
            json=llm_payload,
            headers={"Content-Type": "application/json"},
            timeout=90,
        )
        llm_resp.raise_for_status()
        llm_data = llm_resp.json()
        choice = llm_data.get("choices") or []
        content_markdown = ""
        if choice:
            msg = choice[0].get("message") or {}
            content_markdown = (msg.get("content") or "").strip()
        if not content_markdown:
            content_markdown = "*(Không tạo được câu trả lời.)*"

        usage = llm_data.get("usage") or {}
        tokens_used = usage.get("total_tokens") or 0
        response_time_ms = int((time.time() - t0) * 1000)

        return {
            "session_id": req.session_id,
            "status": "success",
            "content_markdown": content_markdown,
            "meta": {
                "model": ADMISSION_AGENT_LLM_MODEL,
                "response_time_ms": response_time_ms,
                "tokens_used": tokens_used,
                "sources": sources if sources else None,
                "points_count": len(points),
            },
        }

    except requests.RequestException as e:
        response_time_ms = int((time.time() - t0) * 1000)
        return {
            "session_id": req.session_id,
            "status": "error",
            "error_message": str(e),
            "meta": {"response_time_ms": response_time_ms},
        }
    except Exception as e:
        response_time_ms = int((time.time() - t0) * 1000)
        return {
            "session_id": req.session_id,
            "status": "error",
            "error_message": str(e),
            "meta": {"response_time_ms": response_time_ms},
        }
