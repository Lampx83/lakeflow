import streamlit as st
import requests
import os
import sys
import subprocess
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================

API_BASE = os.getenv("API_BASE_URL", "http://api:8011")

# Đường dẫn scripts (mount từ backend)
SCRIPTS_DIR = Path("/app/backend/src/eduai/scripts")
PYTHON_BIN = sys.executable

EDUAI_MODE = os.getenv("EDUAI_MODE", "DEV")  # DEV | PROD

st.set_page_config(
    page_title="EDUAI Backend Control UI",
    layout="wide",
)

st.title("EDUAI – Backend Control & Test UI")
st.caption("Dùng cho test, debug và vận hành pipeline nội bộ")

# =====================================================
# SESSION STATE
# =====================================================

if "token" not in st.session_state:
    st.session_state.token = None

# =====================================================
# HELPER: RUN SCRIPT
# =====================================================

def run_step(step: str):
    try:
        with st.spinner(f"Đang chạy pipeline {step}..."):
            resp = requests.post(
                f"{API_BASE}/pipeline/run/{step}",
                timeout=3600,
            )

        if resp.status_code != 200:
            st.error(f"❌ Run failed: {resp.text}")
            return

        data = resp.json()

        # ---------- Status ----------
        if data.get("returncode") == 0:
            st.success("✅ Completed")
        else:
            st.error(f"❌ Failed (code={data.get('returncode')})")

        # ---------- STDOUT ----------
        stdout = data.get("stdout", "")

        if "INGESTION SUMMARY" in stdout:
            with st.expander("📦 Ingestion summary", expanded=True):
                st.code(stdout, language="text")
        else:
            st.code(stdout)

        # ---------- STDERR ----------
        stderr = data.get("stderr", "")
        if stderr:
            with st.expander("⚠️ Error log (stderr)", expanded=True):
                st.code(stderr, language="text")

    except Exception as exc:
        st.error(str(exc))


# =====================================================
# 1️⃣ LOGIN
# =====================================================

st.header("1️⃣ Login")

with st.form("login_form"):
    col1, col2 = st.columns(2)
    with col1:
        username = st.text_input("Username", value="admin")
    with col2:
        password = st.text_input(
            "Password", type="password", value="admin123"
        )

    login_btn = st.form_submit_button("Login")

if login_btn:
    try:
        resp = requests.post(
            f"{API_BASE}/auth/login",
            json={
                "username": username,
                "password": password,
            },
            timeout=10,
        )

        if resp.status_code == 200:
            st.session_state.token = resp.json()["access_token"]
            st.success("Login successful")
        else:
            st.error(f"Login failed: {resp.text}")

    except Exception as exc:
        st.error(str(exc))

if st.session_state.token:
    st.markdown("**JWT Token:**")
    st.code(st.session_state.token, language="text")

# =====================================================
# 2️⃣ SEMANTIC SEARCH
# =====================================================

st.header("2️⃣ Semantic Search")

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

search_btn = st.button("Search")

if search_btn:
    if not st.session_state.token:
        st.warning("Vui lòng login trước")
    elif not query.strip():
        st.warning("Query không được để trống")
    else:
        try:
            headers = {
                "Authorization": f"Bearer {st.session_state.token}"
            }

            payload = {
                "query": query,
                "top_k": top_k,
            }

            resp = requests.post(
                f"{API_BASE}/search/semantic",
                json=payload,
                headers=headers,
                timeout=30,
            )

            if resp.status_code != 200:
                st.error(f"Search failed: {resp.text}")
            else:
                data = resp.json()

                st.subheader("📦 Raw API Response")
                st.json(data)

                st.subheader("📄 Results")
                results = data.get("results", [])

                if not results:
                    st.info("No results")
                else:
                    for idx, r in enumerate(results, 1):
                        title = (
                            f"[{idx}] "
                            f"score={r['score']:.4f} | "
                            f"file={r['file_hash']} | "
                            f"chunk={r['chunk_id']}"
                        )
                        with st.expander(title):
                            st.write(r["text"])
                            st.caption(
                                f"section={r.get('section_id')} | "
                                f"token_estimate={r.get('token_estimate')}"
                            )

        except Exception as exc:
            st.error(str(exc))

# =====================================================
# 3️⃣ PIPELINE RUNNER (DEV ONLY)
# =====================================================

if EDUAI_MODE == "DEV":
    st.markdown("---")
    st.header("3️⃣ Pipeline Runner (000 → 400)")

    st.warning(
        "⚠️ Chỉ dùng cho DEV / nội bộ. "
        "Không bật ở môi trường production."
    )

    PIPELINE_STEPS = [
        ("000 – Inbox Ingestion", "step0"),
        ("200 – File Staging", "step1"),
        ("300 – Data Processing", "step2"),
        ("400 – Embedding Generation", "step3"),
        ("401 – Qdrant Indexing", "step4"),
    ]

    for label, step in PIPELINE_STEPS:
        if st.button(f"▶ Run {label}"):
            run_step(step)

    st.markdown("### 🚀 Full Pipeline")

    if st.button("Run ALL (000 → Qdrant)"):
        for label, step in PIPELINE_STEPS:
            st.subheader(label)
            run_step(step)

# =====================================================
# FOOTER
# =====================================================

st.markdown("---")
st.caption(
    "EDUAI Streamlit Control UI – "
    "Test, debug & vận hành pipeline nội bộ. "
    "Không phải frontend sản phẩm."
)

st.markdown("---")
st.header("4️⃣ Data Lake Explorer")
from pathlib import Path
import json

DATA_ROOT = Path("/data")

ZONES = {
    "000_inbox": DATA_ROOT / "000_inbox",
    "100_raw": DATA_ROOT / "100_raw",
    "200_staging": DATA_ROOT / "200_staging",
    "300_processed": DATA_ROOT / "300_processed",
    "400_embeddings": DATA_ROOT / "400_embeddings",
    "500_catalog": DATA_ROOT / "500_catalog",
}
zone_name = st.selectbox(
    "📂 Chọn data zone",
    list(ZONES.keys()),
)

zone_path = ZONES[zone_name]

if not zone_path.exists():
    st.warning(f"Zone chưa tồn tại: {zone_path}")
    st.stop()

def list_dir(path: Path):
    dirs = []
    files = []

    for p in sorted(path.iterdir()):
        if p.is_dir():
            dirs.append(p)
        elif p.is_file():
            files.append(p)

    return dirs, files

dirs, files = list_dir(zone_path)

st.subheader(f"📁 {zone_name}")

# ---------- Folders ----------
for d in dirs:
    with st.expander(f"📁 {d.name}"):
        sub_dirs, sub_files = list_dir(d)

        for sd in sub_dirs:
            st.markdown(f"📁 `{sd.name}`")

        for sf in sub_files:
            size_kb = sf.stat().st_size / 1024
            st.markdown(f"📄 `{sf.name}` ({size_kb:.1f} KB)")
all_files = [
    p for p in zone_path.rglob("*")
    if p.is_file() and p.stat().st_size < 2 * 1024 * 1024
]

file_map = {
    str(p.relative_to(zone_path)): p
    for p in all_files
}

if file_map:
    file_key = st.selectbox(
        "📄 Chọn file để xem",
        list(file_map.keys()),
    )

    file_path = file_map[file_key]

    st.markdown(f"### 📄 {file_key}")

    if file_path.suffix in {".json"}:
        with file_path.open("r", encoding="utf-8") as f:
            st.json(json.load(f))

    elif file_path.suffix in {".txt"}:
        st.code(
            file_path.read_text(encoding="utf-8"),
            language="text",
        )
    else:
        st.info("Không hỗ trợ preview định dạng này")
