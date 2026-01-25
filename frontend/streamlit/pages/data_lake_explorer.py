# frontend/streamlit/pages/data_lake_explorer.py

from pathlib import Path
import json

import pandas as pd
import streamlit as st

from config.settings import DATA_ROOT
from state.session import require_login
from utils.fs_utils import list_dir
from utils.sqlite_viewer import (
    connect_readonly,
    list_tables,
    get_table_schema,
    preview_table,
)

# =====================================================
# DATA ZONES
# =====================================================

ZONES = {
    "000_inbox": DATA_ROOT / "000_inbox",
    "100_raw": DATA_ROOT / "100_raw",
    "200_staging": DATA_ROOT / "200_staging",
    "300_processed": DATA_ROOT / "300_processed",
    "400_embeddings": DATA_ROOT / "400_embeddings",
    "500_catalog": DATA_ROOT / "500_catalog",
}

MAX_PREVIEW_SIZE = 2 * 1024 * 1024  # 2MB


# =====================================================
# UI HELPERS
# =====================================================

def render_directory_tree(root: Path) -> None:
    """
    Hiển thị cây thư mục 1 cấp (folders + files).
    """
    dirs, files = list_dir(root)

    for d in dirs:
        with st.expander(f"📁 {d.name}"):
            sub_dirs, sub_files = list_dir(d)

            for sd in sub_dirs:
                st.markdown(f"📁 `{sd.name}`")

            for sf in sub_files:
                size_kb = sf.stat().st_size / 1024
                st.markdown(f"📄 `{sf.name}` ({size_kb:.1f} KB)")


def render_file_preview(zone_path: Path) -> None:
    """
    Preview các file nhỏ (JSON, TXT).
    """
    preview_files = [
        p for p in zone_path.rglob("*")
        if p.is_file() and p.stat().st_size <= MAX_PREVIEW_SIZE
    ]

    if not preview_files:
        return

    file_map = {
        str(p.relative_to(zone_path)): p
        for p in preview_files
    }

    st.divider()
    file_key = st.selectbox(
        "📄 Chọn file để xem nội dung",
        list(file_map.keys()),
    )

    file_path = file_map[file_key]
    st.markdown(f"### 📄 {file_key}")

    if file_path.suffix == ".json":
        with file_path.open("r", encoding="utf-8") as f:
            st.json(json.load(f))

    elif file_path.suffix == ".txt":
        st.code(
            file_path.read_text(encoding="utf-8"),
            language="text",
        )

    else:
        st.info("Không hỗ trợ preview định dạng này")


def render_sqlite_viewer(zone_path: Path) -> None:
    """
    SQLite viewer (chỉ cho 500_catalog).
    """
    sqlite_files = [
        p for p in zone_path.iterdir()
        if p.is_file() and p.suffix in {".sqlite", ".db"}
    ]

    if not sqlite_files:
        st.info("Không tìm thấy SQLite database trong 500_catalog")
        return

    st.divider()
    st.subheader("📊 SQLite Database Viewer")
    st.caption("Chế độ chỉ đọc – phục vụ kiểm tra catalog & ingest log")

    db_file = st.selectbox(
        "🗄️ Chọn database",
        sqlite_files,
        format_func=lambda p: p.name,
    )

    try:
        conn = connect_readonly(db_file)
    except Exception as exc:
        st.error(f"Không mở được database: {exc}")
        return

    try:
        tables = list_tables(conn)
    except Exception as exc:
        st.error(f"Lỗi đọc metadata database: {exc}")
        return

    if not tables:
        st.warning("Database không có bảng nào")
        return

    table = st.selectbox("📋 Chọn bảng", tables)

    # ---------- Schema ----------
    st.markdown("### 🧱 Schema")
    schema_df = get_table_schema(conn, table)
    st.dataframe(schema_df, use_container_width=True)

    # ---------- Data preview ----------
    st.markdown("### 👁️ Preview dữ liệu")

    limit = st.slider(
        "Số dòng hiển thị",
        min_value=10,
        max_value=500,
        value=100,
        step=10,
    )

    try:
        data_df = preview_table(conn, table, limit)
        st.dataframe(data_df, use_container_width=True)
    except Exception as exc:
        st.error(f"Lỗi đọc dữ liệu bảng: {exc}")


# =====================================================
# MAIN PAGE
# =====================================================

def render():
    if not require_login():
        return

    st.header("🗂️ Data Lake Explorer")

    # --------------------------------------------------
    # SELECT ZONE
    # --------------------------------------------------
    zone_name = st.selectbox(
        "📂 Chọn data zone",
        list(ZONES.keys()),
    )

    zone_path = ZONES[zone_name]

    if not zone_path.exists():
        st.warning(f"Zone chưa tồn tại: {zone_path}")
        return

    st.subheader(f"📁 {zone_name}")

    # --------------------------------------------------
    # DIRECTORY TREE
    # --------------------------------------------------
    render_directory_tree(zone_path)

    # --------------------------------------------------
    # FILE PREVIEW
    # --------------------------------------------------
    render_file_preview(zone_path)

    # --------------------------------------------------
    # SQLITE VIEWER (ONLY 500_catalog)
    # --------------------------------------------------
    if zone_name == "500_catalog":
        render_sqlite_viewer(zone_path)
