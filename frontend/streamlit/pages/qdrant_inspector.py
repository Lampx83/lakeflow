# streamlit/pages/qdrant_inspector.py

import pandas as pd
import streamlit as st

from state.session import require_login
from services.qdrant_service import (
    list_collections,
    get_collection_detail,
    list_points,
    filter_points,
)

# =====================================================
# CONFIG
# =====================================================

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200

# =====================================================
# UI
# =====================================================

def render():
    if not require_login():
        return

    st.header("🧠 Qdrant Inspector")
    st.caption("Trình duyệt embeddings (read-only) – phục vụ debug & kiểm tra dữ liệu")

    token = st.session_state.token

    # -------------------------------------------------
    # LOAD COLLECTIONS
    # -------------------------------------------------
    try:
        collections = list_collections(token)
    except Exception as exc:
        st.error(f"Không lấy được danh sách collections: {exc}")
        return

    if not collections:
        st.info("Qdrant chưa có collection nào")
        return

    # -------------------------------------------------
    # SELECT COLLECTION
    # -------------------------------------------------
    col = st.selectbox(
        "📦 Collection",
        collections,
        format_func=lambda c: c["name"],
    )

    col_name = col["name"]

    # -------------------------------------------------
    # COLLECTION DETAIL (SOURCE OF TRUTH)
    # -------------------------------------------------
    try:
        detail = get_collection_detail(col_name, token)
    except Exception as exc:
        st.error(f"Lỗi khi lấy collection detail: {exc}")
        return

    st.subheader("📊 Collection Overview")

    points_count = detail.get("points_count", 0)

    vectors = detail.get("vectors", {})
    vector_size = "—"
    distance = "—"

    if isinstance(vectors, dict) and vectors:
        first = next(iter(vectors.values()))
        vector_size = first.get("size", "—")
        distance = first.get("distance", "—")

    c1, c2, c3 = st.columns(3)
    c1.metric("Points", points_count)
    c2.metric("Vector size", vector_size)
    c3.metric("Distance", distance)

    # -------------------------------------------------
    # COLLECTION DETAIL
    # -------------------------------------------------
    try:
        detail = get_collection_detail(col_name, token)
    except Exception as exc:
        st.error(f"Lỗi khi lấy collection detail: {exc}")
        return

    st.subheader("🧱 Payload Schema")
    st.json(detail.get("payload_schema", {}))

    # =================================================
    # FILTER
    # =================================================
    st.divider()
    st.subheader("🔍 Filter points (payload)")

    f1, f2, f3 = st.columns(3)

    with f1:
        file_hash = st.text_input("file_hash")

    with f2:
        section_id = st.text_input("section_id")

    with f3:
        chunk_id = st.number_input(
            "chunk_id",
            min_value=0,
            step=1,
            value=0,
        )

    use_filter = st.checkbox("Áp dụng filter")

    # =================================================
    # PAGINATION
    # =================================================
    st.divider()
    st.subheader("📄 Browse points")

    p1, p2 = st.columns(2)

    with p1:
        limit = st.slider(
            "Số point / trang",
            min_value=10,
            max_value=MAX_PAGE_SIZE,
            value=DEFAULT_PAGE_SIZE,
            step=10,
        )

    with p2:
        offset = st.number_input(
            "Offset",
            min_value=0,
            step=limit,
            value=0,
        )

    # -------------------------------------------------
    # LOAD POINTS
    # -------------------------------------------------
    try:
        if use_filter:
            points = filter_points(
                collection=col_name,
                token=token,
                file_hash=file_hash or None,
                section_id=section_id or None,
                chunk_id=chunk_id if chunk_id > 0 else None,
                limit=limit,
            )
        else:
            points = list_points(
                collection=col_name,
                token=token,
                limit=limit,
                offset=offset,
            )

    except Exception as exc:
        st.error(f"Lỗi khi load points: {exc}")
        return

    if not points:
        st.info("Không có point nào phù hợp")
        return

    # =================================================
    # TABLE VIEW
    # =================================================
    rows = []
    for p in points:
        payload = p.get("payload", {})

        rows.append({
            "id": p.get("id"),
            "file_hash": payload.get("file_hash"),
            "chunk_id": payload.get("chunk_id"),
            "section_id": payload.get("section_id"),
            "vector_dim": p.get("vector_size"),
        })

    df = pd.DataFrame(rows)

    st.dataframe(df, use_container_width=True)

    # =================================================
    # DETAIL VIEW
    # =================================================
    st.subheader("🔎 Chi tiết point")

    point_ids = [p["id"] for p in points]

    selected_id = st.selectbox(
        "Chọn point",
        point_ids,
    )

    selected_point = next(
        p for p in points if p["id"] == selected_id
    )

    with st.expander("📌 Payload"):
        st.json(selected_point.get("payload", {}))

    with st.expander("🧠 Vector info"):
        st.write(f"Vector dimension: {selected_point.get('vector_size')}")

    st.caption("⚠️ Vector raw không được hiển thị để đảm bảo hiệu năng & an toàn")
