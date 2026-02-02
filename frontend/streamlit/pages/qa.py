# streamlit/pages/qa.py

import pandas as pd
import streamlit as st

from services.api_client import qa
from services.qdrant_service import list_collections
from state.session import require_login


def render():
    if not require_login():
        return

    st.header("🤖 Hỏi đáp với AI")
    st.caption(
        "**Demo RAG:** Tính năng này dùng để demo RAG (Retrieval-Augmented Generation). "
        "Hệ thống (1) tìm các đoạn tài liệu liên quan (semantic search), (2) gửi làm context cho AI, "
        "(3) AI **chỉ được trả lời dựa trên context được cung cấp** — không dùng kiến thức bên ngoài. "
        "Nếu context không đủ, AI sẽ nói rõ không có thông tin. Trả lời bằng tiếng Việt."
    )

    token = st.session_state.token

    # --------------------------------------------------
    # PARAMS
    # --------------------------------------------------
    try:
        collections_resp = list_collections(token)
        collections = [c["name"] for c in collections_resp] if collections_resp else ["eduai_chunks"]
    except Exception:
        collections = ["eduai_chunks"]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        collection_name = st.selectbox(
            "📦 Collection",
            collections,
            help="Collection Qdrant chứa embeddings để tìm context.",
        )

    with col2:
        top_k = st.slider(
            "Số context (Top K)",
            min_value=1,
            max_value=20,
            value=5,
            help="Số đoạn tài liệu tối đa gửi làm context cho LLM.",
        )

    with col3:
        temperature = st.slider(
            "Temperature",
            min_value=0.0,
            max_value=2.0,
            value=0.7,
            step=0.1,
            help="0 = chính xác, 2 = sáng tạo hơn.",
        )

    with col4:
        use_threshold = st.checkbox("Ngưỡng điểm context", value=False)
        score_threshold = None
        if use_threshold:
            score_threshold = st.slider(
                "Score tối thiểu",
                min_value=0.0,
                max_value=1.0,
                value=0.5,
                step=0.05,
                key="qa_score_threshold",
            )

    question = st.text_area(
        "Câu hỏi của bạn",
        placeholder="Ví dụ: Quy định về kinh tế quốc dân? Điều kiện tuyển sinh? Chính sách học phí?",
        height=120,
        key="qa_question",
    )

    if st.button("🔍 Hỏi AI", type="primary"):
        if not question.strip():
            st.warning("Vui lòng nhập câu hỏi")
            return

        with st.spinner("Đang tìm context và gọi LLM..."):
            try:
                data = qa(
                    question=question.strip(),
                    top_k=top_k,
                    temperature=temperature,
                    token=token,
                    collection_name=collection_name or None,
                    score_threshold=score_threshold,
                )
            except Exception as exc:
                st.error(f"Lỗi khi gọi API: {exc}")
                return

        # ---------- Question (echo) ----------
        st.subheader("❓ Câu hỏi")
        st.write(question.strip())

        # ---------- Answer ----------
        st.subheader("💡 Câu trả lời")
        answer = data.get("answer") or "Không có câu trả lời."
        model_used = data.get("model_used")

        if model_used:
            st.caption(f"Model: **{model_used}**")

        st.markdown(answer)

        st.download_button(
            "⬇️ Tải câu trả lời (TXT)",
            data=answer,
            file_name="qa_answer.txt",
            mime="text/plain",
            key="qa_download_answer",
        )

        # ---------- Contexts summary ----------
        contexts = data.get("contexts", [])
        st.subheader("📚 Context đã dùng để trả lời")
        st.caption(
            "Các đoạn tài liệu được tìm bằng semantic search và gửi cho LLM. "
            "Score = độ tương đồng với câu hỏi (0–1)."
        )

        if not contexts:
            st.info("Không có context nào được sử dụng")
        else:
            scores = [c["score"] for c in contexts]
            st.metric("Số context", len(contexts))
            st.caption(f"Score trung bình: {sum(scores) / len(scores):.4f} | Min: {min(scores):.4f} | Max: {max(scores):.4f}")

            # ---------- Table ----------
            st.markdown("**Bảng context**")
            rows = []
            for idx, ctx in enumerate(contexts, start=1):
                text = ctx.get("text") or ""
                text_preview = (text[:80] + "…") if len(text) > 80 else text
                rows.append({
                    "#": idx,
                    "score": round(ctx["score"], 4),
                    "file_hash": ctx.get("file_hash"),
                    "chunk_id": ctx.get("chunk_id"),
                    "section_id": ctx.get("section_id"),
                    "text": text_preview,
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            # ---------- Detail per context ----------
            st.markdown("**Chi tiết từng context**")
            for idx, ctx in enumerate(contexts, start=1):
                title = (
                    f"[{idx}] score = {ctx['score']:.4f} | "
                    f"file_hash = {ctx.get('file_hash') or '—'} | "
                    f"chunk_id = {ctx.get('chunk_id')}"
                )
                with st.expander(title, expanded=(idx <= 2)):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.write("**Metadata**")
                        st.write(f"- file_hash: `{ctx.get('file_hash') or '—'}`")
                        st.write(f"- chunk_id: `{ctx.get('chunk_id')}`")
                        st.write(f"- section_id: `{ctx.get('section_id') or '—'}`")
                        st.write(f"- token_estimate: `{ctx.get('token_estimate') or '—'}`")
                        st.write(f"- source: `{ctx.get('source') or '—'}`")
                        if ctx.get("id"):
                            st.write(f"- point id: `{ctx.get('id')}`")
                    with c2:
                        text = ctx.get("text") or "(trống)"
                        st.write("**Nội dung**")
                        st.text_area(
                            "Nội dung",
                            value=text,
                            height=180,
                            key=f"qa_ctx_text_{idx}_{ctx.get('id', idx)}",
                            disabled=True,
                            label_visibility="collapsed",
                        )
                        st.download_button(
                            "⬇️ Tải nội dung context",
                            data=text,
                            file_name=f"context_{ctx.get('file_hash', '')}_{ctx.get('chunk_id', idx)}.txt",
                            mime="text/plain",
                            key=f"qa_ctx_dl_{idx}_{ctx.get('id', idx)}",
                        )

        # ---------- Raw response ----------
        with st.expander("📦 Raw API Response", expanded=False):
            st.json(data)

    else:
        st.info("Nhập câu hỏi và bấm **Hỏi AI** để bắt đầu.")
