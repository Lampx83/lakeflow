import streamlit as st
from services.api_client import semantic_search
from state.session import require_login


def render():
    if not require_login():
        return

    st.header("🔎 Semantic Search")

    query = st.text_area(
        "Query (ngôn ngữ tự nhiên)",
        placeholder="Ví dụ: Kinh tế quốc dân",
        height=80,
    )

    top_k = st.slider(
        "Top K",
        min_value=1,
        max_value=20,
        value=5,
    )

    if st.button("Search"):
        if not query.strip():
            st.warning("Query không được để trống")
            return

        try:
            data = semantic_search(
                query=query,
                top_k=top_k,
                token=st.session_state.token,
            )
        except Exception as exc:
            st.error(f"Lỗi khi gọi API: {exc}")
            return

        # ---------- Raw response ----------
        st.subheader("📦 Raw API Response")
        st.json(data)

        # ---------- Render results ----------
        st.subheader("📄 Results")

        results = data.get("results", [])
        if not results:
            st.info("Không có kết quả phù hợp")
            return

        for idx, r in enumerate(results, start=1):
            title = (
                f"[{idx}] score={r['score']:.4f} | "
                f"file={r['file_hash']} | "
                f"chunk={r['chunk_id']}"
            )

            with st.expander(title):
                st.write(r["text"])
                st.caption(
                    f"section={r.get('section_id')} | "
                    f"token_estimate={r.get('token_estimate')}"
                )
