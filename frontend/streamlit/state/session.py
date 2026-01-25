import streamlit as st

from state.token_store import load_token


def init_session():
    if "token" not in st.session_state:
        st.session_state.token = load_token()
def is_logged_in() -> bool:
    return bool(st.session_state.get("token"))

def require_login() -> bool:
    """
    Dùng trong page cần auth.
    Trả False nếu chưa login (và hiển thị warning).
    """
    if not is_logged_in():
        st.warning("🔒 Vui lòng đăng nhập để sử dụng chức năng này")
        return False
    return True
