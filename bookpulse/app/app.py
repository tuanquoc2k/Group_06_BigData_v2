"""Streamlit entry point for the BookPulse reader demo."""
import streamlit as st

st.set_page_config(page_title="BookPulse · Khám phá và gợi ý", page_icon="📚", layout="wide")
st.navigation([
    st.Page("bookstore.py", title="Khám phá & gợi ý", icon="📚", default=True),
    st.Page("setting.py", title="Điều khiển demo", icon="⚙️", url_path="setting"),
]).run()
