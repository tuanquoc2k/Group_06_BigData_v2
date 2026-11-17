"""Demo controls for streaming, event replay, audits, and batch retraining."""
import json
import os
import urllib.error
import urllib.request

import streamlit as st

from common.fake import FAKE_CSV, write_fake

CONTROL_URL = os.getenv("CONTROL_URL", "http://spark:8600")
TASK_LABELS = {
    "reset": "Xóa dữ liệu streaming thử nghiệm",
    "late": "Gửi sự kiện mô phỏng",
    "kafka_audit": "Đối chiếu Kafka và MongoDB",
    "nosql_evidence": "Kiểm chứng MongoDB",
    "retrain": "Train lại mô hình batch",
}


def call(path, payload=None, timeout=5):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        CONTROL_URL + path, data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def launch(name, **params):
    try:
        code, reply = call(f"/run/{name}", params)
    except OSError as exc:
        st.error(f"Không gọi được server điều khiển ({type(exc).__name__}).")
        return
    if code != 200:
        st.warning(reply.get("error", f"HTTP {code}"))
        return
    if name != "start_stream":
        st.session_state["active_control_task"] = name
        st.session_state.pop("last_control_task", None)


@st.fragment(run_every=5)
def streaming_controls():
    try:
        status = call("/status", timeout=2)[1]
    except OSError:
        st.error("Chưa kết nối được server điều khiển trong container spark.")
        return

    running = status.get("stream_running", False)
    left, start_col, stop_col = st.columns([4, 1.2, 1.2])
    if running:
        left.success("Streaming đang hoạt động")
    else:
        left.warning("Streaming đang tạm dừng")
    if start_col.button("Bật job", type="primary", disabled=running, use_container_width=True):
        launch("start_stream")
        st.rerun(scope="fragment")
    if stop_col.button("Dừng job", disabled=not running, use_container_width=True):
        try:
            call("/stop_stream", {})
        except OSError as exc:
            st.error(f"Không dừng được streaming ({type(exc).__name__}).")
        st.rerun(scope="fragment")
    task = status.get("tasks", {}).get("start_stream")
    if task and task.get("log"):
        with st.expander("Log streaming", expanded=False):
            st.code("\n".join(task["log"].splitlines()[-20:]))


@st.fragment(run_every=2)
def monitor_control_task(name):
    try:
        status = call("/status")[1]
        task = status.get("tasks", {}).get(name)
        if not task:
            st.caption("Đang chờ tác vụ bắt đầu…")
            return
        log = task.get("log", "")
        if task.get("running"):
            st.info(f"{TASK_LABELS[name]} · đang chạy {task.get('elapsed', 0)} giây")
            st.code("\n".join(log.splitlines()[-14:]) or "Đang khởi động…")
            return
        try:
            log = call(f"/log/{name}")[1].get("log", log)
        except OSError:
            pass
        st.session_state["last_control_task"] = {
            "name": name, "returncode": task.get("returncode"), "log": log,
        }
        st.session_state.pop("active_control_task", None)
        st.rerun()
    except OSError as exc:
        st.warning(f"Mất kết nối server điều khiển ({type(exc).__name__}); sẽ thử lại.")


st.title("⚙️ Điều khiển demo")
st.caption("Điều khiển luồng dữ liệu và kiểm tra các kết quả của project.")

st.subheader("1 · Streaming")
streaming_controls()

busy_task = st.session_state.get("active_control_task")
if busy_task:
    monitor_control_task(busy_task)
elif st.session_state.get("last_control_task"):
    last = st.session_state["last_control_task"]
    with st.expander(
        f"Kết quả gần nhất · {TASK_LABELS.get(last['name'], last['name'])}",
        expanded=last.get("returncode") not in (0, None),
    ):
        if last.get("returncode") == 0:
            st.success("Tác vụ hoàn tất.")
        else:
            st.error(f"Tác vụ kết thúc với mã {last.get('returncode')}.")
        st.code(last.get("log") or "Không có output.")

st.subheader("2 · Gửi dữ liệu mô phỏng")
st.caption("Phát sự kiện qua Kafka để minh họa ingest, lọc trùng và cập nhật trạng thái người đọc.")
if not FAKE_CSV.exists():
    if st.button("Chuẩn bị dữ liệu mẫu (5.000 sự kiện)", disabled=bool(busy_task)):
        with st.spinner("Đang chuẩn bị dữ liệu mẫu…"):
            write_fake()
        st.success("Đã chuẩn bị dữ liệu mẫu.")
        st.rerun()
else:
    count_col, rate_col, late_col = st.columns(3)
    limit = count_col.number_input("Số sự kiện", min_value=50, max_value=5000, value=300, step=50)
    rate = rate_col.number_input("Tốc độ / giây", min_value=5, max_value=100, value=20, step=5)
    late = late_col.slider("Tỷ lệ sự kiện trễ", 0.01, 0.20, 0.05, step=0.01, format="%.2f")
    if st.button("Gửi sự kiện vào Kafka", type="primary", disabled=bool(busy_task)):
        launch("late", limit=int(limit), rate=int(rate), late=float(late))
        st.rerun()

st.subheader("3 · Kiểm chứng")
audit_col, nosql_col = st.columns(2)
if audit_col.button("Đối chiếu Kafka ↔ MongoDB", disabled=bool(busy_task), use_container_width=True):
    launch("kafka_audit")
    st.rerun()
if nosql_col.button("Kiểm chứng MongoDB", disabled=bool(busy_task), use_container_width=True):
    launch("nosql_evidence")
    st.rerun()

with st.expander("Train lại model batch", expanded=False):
    st.caption("Dừng stream tạm thời, gộp archive, train ALS, xuất gợi ý rồi bật stream lại. Có thể mất vài phút.")
    train_user = st.number_input("User dùng để so sánh", min_value=1, value=1, step=1)
    exclude_simulated = st.checkbox("Bỏ sự kiện mô phỏng khỏi dữ liệu train", value=False)
    if st.button("Train lại", type="primary", disabled=bool(busy_task)):
        launch("retrain", user=int(train_user), exclude_simulated=bool(exclude_simulated))
        st.rerun()

with st.expander("Bảo trì · xóa dữ liệu streaming thử nghiệm", expanded=False):
    st.warning("Thao tác này xóa topic/checkpoint/archive và các collection streaming đã thử.")
    confirmed = st.checkbox("Tôi hiểu và muốn xóa dữ liệu thử nghiệm", key="confirm_stream_reset")
    if st.button("Xóa dữ liệu streaming", disabled=not confirmed or bool(busy_task)):
        launch("reset", confirmed=True)
        st.rerun()
