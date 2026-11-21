# Lần chạy v2 đầu tiên ngày 21/11 (đã thay thế)

- `train_als_implicit_20261121_064336.json`, `write_recs_20261121_064614.json`, `nosql_evidence_20261121_064627.json`: mô hình
  `20261121_064323_987378`. Số liệu xếp hạng giống bản đang dùng; được train lại sau khi thêm bước tự kiểm tra
  item/user factors vào `train_als.py`.
- `foldin_eval_20261121_064929.json`, `foldin_eval_20261121_071704.json`: chạy khi `foldin_eval.py` chưa cache phép chia
  train/test; lọc theo `user_id` làm `rand()` chia lại khác, nên phép so vector và replay sai. Thay bằng
  `foldin_eval_20261121_072720.json`.
- `kafka_audit_20261121_073358.json`: `false` do `measure_latency.py` cũ xoá user test mà không tính lại
  `rating_stats` (đã sửa bằng `common/cleanup.py`). Lượt sạch: `kafka_audit_20261121_073750.json`.
- `demo_trace_user1_20261121_073213.json`: lượt thử đầu; lượt dùng cho demo là `demo_trace_user1_20261121_073722.json`.
- `train_als_20261121_063254.json`: ALS explicit khi còn học chỉ trên rating ≥ 4 (đoán số sao mà chỉ thấy 4–5 sao nên so sánh
  không công bằng). Thay bằng `train_als_20261121_095750.json`: explicit học trên mọi rating trừ tập test chung.
