"""Demo controls for streaming, event replay, audits, and batch retraining; results are shown as numbers."""
import ast
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from common.fake import FAKE_CSV, write_fake
from common.settings import REPORT_DIR, get_db

CONTROL_URL = os.getenv("CONTROL_URL", "http://spark:8600")
VN = timezone(timedelta(hours=7))
VN_MIN = datetime.min.replace(tzinfo=VN)
TASK_LABELS = {
    "reset": "Xóa dữ liệu streaming",
    "late": "Gửi sự kiện mô phỏng",
    "kafka_audit": "Đối chiếu Kafka ↔ MongoDB",
    "nosql_evidence": "Kiểm chứng MongoDB",
    "retrain": "Train lại model",
}


@st.cache_resource
def db():
    return get_db()


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


def server_status():
    try:
        return call("/status", timeout=2)[1]
    except OSError:
        return None


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


def full_log(name, fallback=""):
    try:
        return call(f"/log/{name}")[1].get("log", fallback)
    except OSError:
        return fallback


def latest_report(prefix):
    files = sorted(REPORT_DIR.glob(f"{prefix}_2*.json"))
    if not files:
        return None, None
    try:
        report = json.loads(files[-1].read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None
    return report, datetime.fromtimestamp(files[-1].stat().st_mtime, VN)


def when(moment):
    return moment.strftime("%H:%M:%S · %d/%m") if moment else ""


def producer_counts(log):
    """Last counts dict printed by streaming/producer.py ('{...} after 7s' or 'done: {...} to topic ...')."""
    for line in reversed(log.splitlines()):
        start, end = line.find("{"), line.rfind("}")
        if start != -1 and end > start:
            try:
                return ast.literal_eval(line[start:end + 1]), line.startswith("done:")
            except (ValueError, SyntaxError):
                continue
    return None, False


def check(ok, text):
    st.markdown(f"{'✅' if ok else '❌'} {text}")


def show_log(log, label="Log"):
    with st.expander(label, expanded=False):
        st.code(log or "Không có output.")


# ---------------------------------------------------------------- 1 · streaming
@st.fragment(run_every=5)
def streaming_panel():
    status = server_status()
    if status is None:
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

    try:
        database = db()
        valid = (database.stream_state.find_one({"_id": "valid_events"}) or {}).get("events", 0)
        rejected = (database.stream_state.find_one({"_id": "rejected_events"}) or {}).get("events", 0)
        refreshed = database.recs_live.count_documents({"source": "als-foldin"})
    except Exception as exc:  # MongoDB down: keep the buttons usable
        st.caption(f"Chưa đọc được MongoDB ({type(exc).__name__}).")
        return
    progress, _ = latest_report("stream_progress")
    query = ((progress or {}).get("queries") or {}).get("valid", {}) if running else {}
    cols = st.columns(5)
    cols[0].metric("Sự kiện hợp lệ", f"{valid:,}")
    cols[1].metric("Bị loại (sai dữ liệu)", f"{rejected:,}")
    cols[2].metric("Bản trùng bị bỏ", f"{query.get('duplicates_dropped', 0):,}" if query else "–")
    cols[3].metric("Sự kiện trễ bị bỏ", f"{query.get('rows_dropped_by_watermark', 0):,}" if query else "–")
    cols[4].metric("Người có gợi ý mới", f"{refreshed:,}")
    task = status.get("tasks", {}).get("start_stream")
    if task and task.get("log"):
        show_log("\n".join(task["log"].splitlines()[-20:]), "Log streaming")


# ---------------------------------------------------------------- running task
@st.fragment(run_every=2)
def monitor_task(name):
    status = server_status()
    if status is None:
        st.warning("Mất kết nối server điều khiển; đang thử lại…")
        return
    task = status.get("tasks", {}).get(name)
    if not task:
        st.caption("Đang chờ tác vụ bắt đầu…")
        return
    if not task.get("running"):
        st.session_state.pop("active_control_task", None)
        st.rerun()
    if name == "late":
        counts, _ = producer_counts(task.get("log", ""))
        sent = (counts or {}).get("sent", 0)
        total = max(1, int(st.session_state.get("late_limit", 300)))
        st.progress(min(1.0, sent / total), text=f"Đang gửi {sent}/{total} sự kiện · {task.get('elapsed', 0)} giây")
    else:
        st.info(f"⏳ {TASK_LABELS.get(name, name)} · đang chạy {task.get('elapsed', 0)} giây")
        show_log("\n".join(task.get("log", "").splitlines()[-14:]))


def task_failed(status, name):
    """Show an error with the log if the last run of `name` failed; True when it did."""
    task = (status or {}).get("tasks", {}).get(name)
    if task and not task.get("running") and task.get("returncode") not in (0, None):
        st.error(f"{TASK_LABELS[name]} lỗi (mã {task['returncode']}).")
        show_log(full_log(name, task.get("log", "")))
        return True
    return False


# ---------------------------------------------------------------- results
def send_result(status):
    task = (status or {}).get("tasks", {}).get("late")
    if not task or task.get("running") or task_failed(status, "late"):
        return
    counts, done = producer_counts(full_log("late", task.get("log", "")))
    if not counts:
        return
    started = datetime.fromtimestamp(task["started"], VN)
    st.caption(f"Lần gửi gần nhất · {when(started)}")
    cols = st.columns(4)
    cols[0].metric("Đã gửi", f"{counts['sent']:,}")
    cols[1].metric("Bản trùng (cố ý)", f"{counts['duplicates']:,}")
    cols[2].metric("Sự kiện lỗi (cố ý)", f"{counts['invalid']:,}")
    cols[3].metric("Sự kiện trễ (cố ý)", f"{counts['late']:,}")
    if counts.get("failed"):
        st.error(f"{counts['failed']} sự kiện Kafka không xác nhận.")


def audit_result(status):
    if task_failed(status, "kafka_audit"):
        return
    report, at = latest_report("kafka_audit")
    if not report:
        st.caption("Chưa có kết quả đối chiếu.")
        return
    mongo = report.get("mongo_check", {})
    archive = report.get("archive_check", {})
    loss = report.get("kafka_offset_loss", {})
    st.markdown(f"**Kết quả · Đối chiếu Kafka ↔ MongoDB** (luồng streaming) · {when(at)}")
    cols = st.columns(4)
    cols[0].metric("Tin trong Kafka", f"{report['messages']:,}")
    cols[1].metric("Sự kiện khác nhau", f"{report['distinct_event_ids']:,}")
    cols[2].metric("MongoDB đếm hợp lệ", f"{mongo.get('valid_events_counted', 0):,}",
                   help=f"Kỳ vọng {mongo.get('expected_valid_minus_late', 0):,} "
                        "(sự kiện hợp lệ trừ sự kiện trễ)")
    cols[3].metric("MongoDB đếm bị loại", f"{mongo.get('rejected_events_counted', 0):,}",
                   help=f"Trong Kafka có {report.get('invalid_messages', 0):,} tin sai dữ liệu")
    if not report.get("messages"):
        st.info("Kafka chưa có sự kiện nào. Gửi dữ liệu mô phỏng hoặc chấm sách rồi đối chiếu lại.")
        return
    check(mongo.get("no_loss_no_double_count"), "Không mất sự kiện, không đếm trùng")
    check(mongo.get("rejected_match"), "Số sự kiện bị loại khớp với Kafka")
    if archive.get("present"):
        check(archive.get("exactly_once"), f"Archive Parquet đúng một lần ({archive.get('rows', 0):,} dòng)")
    check(loss.get("lost_records", 0) == 0, "Không mất offset Kafka")


def nosql_result(status):
    if task_failed(status, "nosql_evidence"):
        return
    report, at = latest_report("nosql_evidence")
    if not report:
        st.caption("Chưa có kết quả kiểm chứng.")
        return
    latency = report.get("read_latency_ms", {})
    overlap = report.get("recs_vs_training_ratings", {})
    sample = report.get("sample_document", {})
    st.markdown(f"**Kết quả · Kiểm chứng MongoDB** (gợi ý batch) · {when(at)}")
    cols = st.columns(4)
    cols[0].metric("Người có gợi ý", f"{report.get('users_with_recs', 0):,}")
    cols[1].metric("Đọc 1 người (p50)", f"{latency.get('p50', 0)} ms")
    cols[2].metric("Đọc 1 người (p95)", f"{latency.get('p95', 0)} ms")
    cols[3].metric("Gợi ý trùng sách đã đọc", f"{overlap.get('rec_pairs_already_rated', 0):,}")
    if sample.get("top_5"):
        st.caption(f"Top 5 của người đọc #{sample.get('_id')}")
        st.dataframe(
            pd.DataFrame(sample["top_5"])[["rank", "title", "score"]]
            .rename(columns={"rank": "Hạng", "title": "Sách", "score": "Điểm"}),
            hide_index=True, use_container_width=True,
        )


def retrain_result(status):
    if task_failed(status, "retrain"):
        return
    report, at = latest_report("retrain")
    if not report:
        return
    if report.get("failed_step"):
        st.error(f"Lần train gần nhất lỗi ở bước {report['failed_step']}.")
        return
    als = ((report.get("train") or {}).get("ranking_test") or {}).get("als", {})
    clean = report.get("clean") or {}
    st.caption(f"Lần train gần nhất · {when(at)}")
    cols = st.columns(4)
    cols[0].metric("Thời gian", f"{report.get('total_seconds', 0):.0f} giây")
    cols[1].metric("Sự kiện stream gộp vào", f"{clean.get('output_rows_from_stream') or 0:,}")
    cols[2].metric("Precision@10", f"{als.get('precision@10', 0):.3f}")
    cols[3].metric(f"Sách đổi trong top 10 (#{report.get('user')})", report.get("top10_books_changed") or 0)


# ---------------------------------------------------------------- page
st.title("⚙️ Điều khiển demo")

st.subheader("1 · Streaming")
streaming_panel()

busy_task = st.session_state.get("active_control_task")
status = None if busy_task else server_status()

st.subheader("2 · Gửi dữ liệu mô phỏng")
if not FAKE_CSV.exists():
    if st.button("Chuẩn bị dữ liệu mẫu (5.000 sự kiện)", disabled=bool(busy_task)):
        with st.spinner("Đang chuẩn bị dữ liệu mẫu…"):
            write_fake()
        st.rerun()
else:
    count_col, rate_col, late_col = st.columns(3)
    limit = count_col.number_input("Số sự kiện", min_value=50, max_value=5000, value=300, step=50)
    rate = rate_col.number_input("Tốc độ / giây", min_value=5, max_value=100, value=20, step=5)
    late = late_col.slider("Tỷ lệ sự kiện trễ", 0.01, 0.20, 0.05, step=0.01, format="%.2f")
    if st.button("Gửi sự kiện vào Kafka", type="primary", disabled=bool(busy_task)):
        st.session_state["late_limit"] = int(limit)
        launch("late", limit=int(limit), rate=int(rate), late=float(late))
        st.rerun()
if busy_task == "late":
    monitor_task("late")
elif not busy_task:
    send_result(status)

st.subheader("3 · Kiểm chứng")
audit_col, nosql_col = st.columns(2)
for column, name in ((audit_col, "kafka_audit"), (nosql_col, "nosql_evidence")):
    if column.button(TASK_LABELS[name], disabled=bool(busy_task), use_container_width=True):
        st.session_state["verify_view"] = name
        launch(name)
        st.rerun()
if busy_task in ("kafka_audit", "nosql_evidence"):
    monitor_task(busy_task)
elif not busy_task:
    view = st.session_state.get("verify_view")
    if view is None:  # nothing clicked yet in this session: show the check that ran last
        audit_at, nosql_at = latest_report("kafka_audit")[1], latest_report("nosql_evidence")[1]
        if audit_at or nosql_at:
            view = "kafka_audit" if (audit_at or VN_MIN) >= (nosql_at or VN_MIN) else "nosql_evidence"
    if view == "kafka_audit":
        audit_result(status)
    elif view == "nosql_evidence":
        nosql_result(status)

with st.expander("Train lại model batch", expanded=busy_task == "retrain"):
    st.caption("Gộp sự kiện stream, train ALS, ghi gợi ý mới. Mất vài phút.")
    train_user = st.number_input("So sánh top 10 của người đọc", min_value=1, value=1, step=1)
    exclude_simulated = st.checkbox("Bỏ sự kiện mô phỏng khỏi dữ liệu train", value=False)
    if st.button("Train lại", type="primary", disabled=bool(busy_task)):
        launch("retrain", user=int(train_user), exclude_simulated=bool(exclude_simulated))
        st.rerun()
    if busy_task == "retrain":
        monitor_task("retrain")
    elif not busy_task:
        retrain_result(status)

with st.expander("Bảo trì · xóa dữ liệu streaming", expanded=busy_task == "reset"):
    st.warning("Xóa topic Kafka, checkpoint, archive và dữ liệu streaming trong MongoDB. Job sẽ bị dừng.")
    confirmed = st.checkbox("Tôi muốn xóa", key="confirm_stream_reset")
    if st.button("Xóa dữ liệu streaming", disabled=not confirmed or bool(busy_task)):
        launch("reset", confirmed=True)
        st.rerun()
    if busy_task == "reset":
        monitor_task("reset")
    elif not busy_task and not task_failed(status, "reset"):
        task = (status or {}).get("tasks", {}).get("reset")
        if task and not task.get("running"):
            st.success(f"Đã xóa lúc {when(datetime.fromtimestamp(task['started'], VN))}. Nhớ bấm **Bật job** lại.")
