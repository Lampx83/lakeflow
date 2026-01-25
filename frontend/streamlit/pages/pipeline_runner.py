import streamlit as st
from services.pipeline_service import run_pipeline_step
from config.settings import EDUAI_MODE
from state.session import require_login

def render():
    if not require_login():
        return

    if EDUAI_MODE != "DEV":
        st.info("Pipeline Runner chỉ khả dụng ở DEV mode")
        return

    st.header("🚀 Pipeline Runner")

    steps = [
        ("000 – Inbox Ingestion", "step0"),
        ("100 – File Staging", "step1"),
        ("200 – Processing", "step2"),
        ("300 – Embedding", "step3"),
        ("400 – Qdrant Indexing", "step4"),
    ]

    for label, step in steps:
        if st.button(label):
            result = run_pipeline_step(step)
            st.code(result.get("stdout", ""))
