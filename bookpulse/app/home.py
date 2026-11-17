"""Compatibility landing page; the current Streamlit entry point is app/app.py."""
import streamlit as st

st.set_page_config(page_title="BookPulse", page_icon="📚")
st.title("BookPulse đã chuyển sang trang demo mới")
st.info("Khởi động ứng dụng bằng `streamlit run app/app.py` để mở trang Khám phá & gợi ý và Điều khiển demo.")
