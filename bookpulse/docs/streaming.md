# Streaming trong BookPulse

Phân tích NoSQL và phác thảo cloud được ghi tại [`serving-deployment.md`](serving-deployment.md); đó là thiết kế, không phải thành phần đã chạy.

Tài liệu này mô tả luồng streaming hiện hành. Số đo được dẫn từ báo cáo đã lưu trong `reports/`; chúng phản ánh những lượt chạy được ghi lại, không phải kết quả nghiệm thu của các thay đổi mới nhất.

## 1. Kiến trúc

```text
Streamlit / producer.py
        │ rating event
        ▼
Kafka: rating-events
        │
        ▼
Spark Structured Streaming: streaming/stream_stats.py
   ├── valid ──► MongoDB: user_ratings, user_seen, recommendation_jobs,
   │                                rating_stats, online_eval, stream_state
   ├── rejected ──► MongoDB: stream_state
   └── archive ──► data/parquet/stream_events
                                     │
                                     └── batch clean.py (lần huấn luyện sau)

recommendation_worker.py
   recommendation_jobs + user_profiles_<model_version>
        └── ALS fold-in ──► recs_live ──► Streamlit: top 10 gợi ý
```

Job Spark có đúng **ba query** (`valid`, `rejected`, `archive`); `recommendation_worker.py` là service riêng,
không phải query streaming thứ tư. Kafka giữ log sự kiện; Spark xử lý sự kiện trực tiếp và lưu bản archive để batch
dùng lại. Worker gộp rating hiện hành theo cặp `(user_id, book_id)`, giữ rating mới nhất theo `event_time`, rồi tính
lại vector user trên item factors bất biến. Đây là fold-in inference, không phải incremental training.

## 2. Quy tắc rating và trạng thái gợi ý

- Mỗi rating hợp lệ được lưu làm trạng thái mới nhất cho cặp user–sách. Rating đến sau thay thế giá trị hiện tại.
- Rating đạt `ALS_MIN_RATING` (mặc định 4) là tín hiệu thích để fold-in. Rating thấp không tham gia tính vector, nhưng vẫn được đưa vào danh sách sách đã xem để không đề xuất lại.
- Mặc định `FOLD_IN_MIN_RATINGS=3`: user cần rating stream cho ba sách khác nhau mới từ 4 sao trở lên kể từ snapshot gần nhất, trước khi worker giải lại vector ALS. Trước ngưỡng này app giữ gợi ý hiện tại và báo tiến độ; rating 1–3 vẫn chỉ đánh dấu sách đã xem.
- Worker cập nhật `recs_live`; Streamlit theo dõi các pha gửi Kafka → chờ job → xử lý → có kết quả, hiển thị thời gian chờ và nạp lại 100 gợi ý trong riêng lưới sách sau khi hoàn tất. Danh sách chia 8 sách/trang, có lọc thể loại và sắp xếp.
- Genre chỉ là metadata dùng để hiển thị, lọc và sắp xếp danh sách 100 gợi ý; không thay đổi điểm ALS.
- Job và kết quả được gắn model version. Rating mới nhất và toàn bộ sách đã chấm được dùng để ngăn gợi ý lặp.

App không ẩn sách chỉ bằng trạng thái phiên sau khi bấm gửi. Sách được lọc khỏi danh sách khi trạng thái rating đã tới MongoDB (`user_ratings`/`user_seen`); do đó nếu streaming dừng hoặc sự kiện vượt watermark 10 phút, việc ẩn có thể chờ streaming chạy lại hoặc batch kế tiếp. Phiên app chỉ lưu thông tin lượt gửi để hiển thị tiến độ.

Các tham số runtime tập trung ở `common/settings.py`. Nếu thay đổi ngưỡng, cần khởi động lại service tương ứng để cấu hình có hiệu lực.

## 3. Query và khả năng phục hồi

| Query | Vai trò | Sink/checkpoint |
|---|---|---|
| `valid` | Xác thực schema, watermark 10 phút, bỏ trùng theo `event_id`, cập nhật trạng thái rating và job | MongoDB; `checkpoints/stream_valid` |
| `rejected` | Đếm sự kiện không hợp lệ | MongoDB `stream_state`; `checkpoints/stream_rejected` |
| `archive` | Lưu mọi sự kiện hợp lệ, kể cả bản lặp và sự kiện trễ | Parquet; `checkpoints/stream_events_archive` |

Mỗi query có checkpoint riêng. Query có trạng thái áp dụng watermark; archive không watermark và không bỏ trùng để batch có thể quyết định xử lý lại. `clean.py` khử trùng lặp khi ghép dữ liệu archive vào lần huấn luyện tiếp theo.

MongoDB writes diễn ra trong `foreachBatch` trên Spark driver. `rating_stats` được dựng lại từ trạng thái hiện hành trong `user_ratings`, thay vì cộng từng event; việc ghi lại cùng trạng thái không làm số cặp tăng. `online_eval` có bảo vệ batch id; `user_seen` và `user_ratings` được upsert theo cặp. Những bảo vệ này không biến toàn hệ thống thành exactly-once: Kafka retention, mất checkpoint hoặc hỏng dữ liệu vẫn có thể cần khôi phục thủ công.

Compose có healthcheck và restart policy cho các dịch vụ. Query streaming thử khởi động lại từ checkpoint sau lỗi; một broker, MongoDB standalone và Spark `local[*]` vẫn là điểm lỗi đơn, không phải HA. Kafka giữ log hữu hạn (mặc định 7 ngày). Với `failOnDataLoss=false`, Spark có thể bỏ qua offset đã hết hạn; `stream_stats.py` hiện so offset đầu micro-batch với earliest offset trên Kafka và ghi rõ sự cố vào `stream_state` để `kafka_audit.py` báo cáo. Chưa ghi nhận sự cố mất offset thật trong phiên này; không có sự cố không chứng minh rằng chưa từng mất dữ liệu.

## 4. Trạng thái bằng chứng

Các report được gắn nhãn theo lần chạy thực tế. Report có `run_metadata.commit_hash` ghi revision nền; `working_tree_dirty=true` nghĩa là lượt chạy dùng thay đổi chưa commit. Kết quả lịch sử không được dùng làm xác nhận cho code mới.

| Thành phần | Trạng thái | Bằng chứng gần nhất trong repo | Ghi chú |
|---|---|---|---|
| Batch clean / SQL / ALS / write-recs | ◐ Lịch sử | `reports/clean_20261116_034934.json`, `reports/analytics_sql_20261115_132952.json`, `reports/train_als_implicit_20261116_035334.json`, `reports/write_recs_20261116_035549.json` | Chưa chạy lại trong lượt này; không dùng làm chứng cứ cho các sửa đổi worker. |
| Fold-in offline và vector | ✓ Đã chạy | `reports/foldin_eval_20261116_092620.json` | 1.000 user; vector so khớp 20 user; có Precision/Recall theo 1/2/3/5/10 positive ratings. |
| Rating trong/ngoài top-10 | ✓ Đã chạy | `reports/foldin_diagnostic_20261116_091506.json` | Cả hai job `done`, `als-foldin`; vector đổi; sách vừa chấm bị lọc khỏi app top-10. |
| Latency end-to-end | ✓ Đã chạy | `reports/stream_latency_20261116_091624.json` | 100/100 event hoàn tất, 0 timeout; số đo mô phỏng, không phải cam kết production. |
| Streaming ingest / Kafka audit | ✓ Đã chạy | `reports/kafka_audit_20261116_094508.json`, `reports/kafka_audit_20261116_093943.json` | Producer sạch: 3.000 / 3.000 valid event được đếm, 19 invalid bị reject; archive khớp 3.059 valid messages (có duplicate); 0 offset gap được ghi nhận. Đây không phải thử nghiệm retention loss. |
| Kafka / Mongo outage | ✓ Đã chạy | `reports/streaming/outage_test_20261116_164126/result.json`, `reports/kafka_audit_20261116_094508.json` | Kafka dừng 40 giây, Mongo dừng 30 giây; sau phục hồi 3.000 rating hiện hành, 0 job pending/processing/failed. Một lượt mô phỏng trên cấu hình local. |
| Worker lease recovery | ✓ Đã chạy | `reports/streaming/worker_recovery/worker_recovery_20261116_164706.json` | Khôi phục synthetic job có lease hết hạn sau restart; đây không phải kill worker giữa lúc tính toán. |
| Worker / Spark restart fault test | ✓ Đã chạy | `reports/streaming/fault_test_20261116_163751/result.json`, `reports/kafka_audit_20261116_093943.json` | Spark bị dừng/khởi động lại từ checkpoint; 1.500 / 1.500 distinct valid event được đếm, duplicate không làm tăng số đếm. |
| Hadoop MapReduce ↔ Spark | ◐ Lịch sử | `reports/mapreduce_verify_20261105_170058.json` | Chưa chạy lại trong lượt này. |
| Toàn bộ pytest | ✓ Đã chạy | `docker compose exec -T spark pytest -q -p no:cacheprovider` | 54 passed sau thay đổi worker và test. |

## 5. Đo lường đã lưu (kết quả lịch sử)

**Baseline fold-in offline ngày 16/11/2026**, model `20261116_035325_280940`; các bảng phía dưới là kết quả của report mới `foldin_eval_20261116_092620.json` sau thay đổi worker:

| Kiểm chứng | Số đo |
|---|---:|
| Replay offline, số user / replay / target ratings | 1.000 / 7.793 / 7.276 |
| Vector khớp Spark `userFactors`, 20 user: cosine trung bình / max absolute error | 0.9999999999999997 / 2.0158e-08 |
| Precision@10, batch → fold-in | 0.1166 → 0.0802 |
| Recall@10, batch → fold-in | 0.1609 → 0.1070 |
| Top-10 Jaccard batch/fold-in | 0.1675 |
| Fold-in, 1.000 user: mean / p95 | 0.00942 / 0.02816 giây/user |
| Tuần tự 1.000 user: mean / p95 / tổng | 0.00882 / 0.02710 giây/user / 8.82 giây |

| Positive ratings trong fold-in | User | Precision@10 batch → fold-in | Recall@10 batch → fold-in |
|---:|---:|---:|---:|
| 1 | 1.000 | 0.1191 → 0.0408 | 0.1642 → 0.0564 |
| 2 | 998 | 0.1216 → 0.0504 | 0.1669 → 0.0713 |
| 3 | 984 | 0.1243 → 0.0595 | 0.1696 → 0.0820 |
| 5 | 903 | 0.1331 → 0.0739 | 0.1730 → 0.0962 |
| 10 | 239 | 0.1778 → 0.1218 | 0.1672 → 0.1137 |

Fold-in khớp vector user train rất sát trong các mẫu đã đo, nhưng chất lượng ranking giảm ở cả 5 nhóm; đây là số đo
thực nghiệm, không phải bảo đảm fold-in cải thiện Precision/Recall.

End-to-end với 100 user test riêng biệt (`stream_latency_20261116_091624.json`): đủ 100 event, 0 timeout. Gửi →
quan sát `recs_live` mean 5.15 giây, trung vị 5.17, p95 6.01; event → recommendation p95 5.91; ingestion mean
2.82 giây. Worker fold-in mean 0.00636 giây (p95 0.01944), ranking mean 0.00440 (p95 0.00744), ghi snapshot
Mongo mean 0.00146 (p95 0.00302; có số đo cho 99/100 event). Đây là tải mô phỏng theo batch, không phải benchmark
production; phép đo gửi → live gồm cả polling 0.1 giây.

Trace mới trên hai user test khác nhau (`foldin_diagnostic_20261116_091506.json`): rating sách trong top-10 làm đổi
vector (`delta norm` 0.1441, cosine trước/sau 0.9496, Jaccard danh sách 0.5385); rating sách ngoài top-10 cũng làm
đổi vector (`delta norm` 0.1296, cosine 0.8418, Jaccard 0.6667). Cả hai job hoàn tất bằng `als-foldin`, và sách vừa
chấm bị loại khỏi top-10 app. Hai lượt test xác nhận đúng đường đi ở hai trường hợp, không chứng minh hành vi cho mọi user.

Fault test mới dừng và khởi động lại Spark từ checkpoint sau khi gửi 1.500 sự kiện; 24 event trễ không vào trạng thái rating, còn archive giữ 1.537 valid messages gồm bản lặp. Lượt outage riêng gửi 3.000 valid event khi Kafka và Mongo lần lượt gián đoạn; sau phục hồi các 3.000 cặp rating được ghi, job đã xử lý hết. Đây là các lượt mô phỏng đơn lẻ trên máy local, không phải chứng minh HA hay retention-loss thật. Report cũ trước reset vẫn được giữ làm lịch sử và không dùng làm xác nhận.

Các lệnh đo mới (cần service healthy, model implicit active và dataset trong container) là:

```powershell
docker compose exec spark python scripts/foldin_eval.py --vector-check-users 20 --benchmark-users 1000
docker compose exec spark python scripts/diagnose_foldin.py
docker compose exec spark python scripts/measure_latency.py --users 100
```

`foldin_eval.py` báo cáo thêm replay theo 1, 2, 3, 5, 10 positive ratings; các lượt này là mô phỏng offline trên holdout, không phải sự kiện production. `diagnose_foldin.py` và `measure_latency.py` phát rating mô phỏng, ghi report rồi xóa document của các user test đã hoàn tất; message Kafka và Parquet archive được giữ lại. Chạy lại audit sau đo để kiểm tra tổng thể.

Trước hai script end-to-end, bật job qua trang **Điều khiển demo** hoặc chạy riêng lệnh
`docker compose exec -d spark python streaming/stream_stats.py`. `diagnose_foldin.py` tạo hai hồ sơ test biệt lập,
thử rating 5 sao cho một sách trong top-10 và một sách latent-score thấp ngoài top-10; chờ tới khi job, vector,
`recs_live` và app view đã được kiểm tra. Cả hai script chỉ xóa document MongoDB của user test đã hoàn tất; event
Kafka và archive giữ lại để audit. Nếu timeout, user chưa hoàn tất được giữ lại để kiểm tra và không bị dọn âm thầm.

## 6. Chạy và kiểm tra

Từ thư mục `bookpulse`:

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

Sau khi dịch vụ healthy, kiểm tra log và audit:

```powershell
docker compose logs --tail 100 spark
python scripts/kafka_audit.py
```

Kiểm thử Python hiện có:

```powershell
python -m compileall common batch streaming app scripts
pytest
```

Không chạy lệnh reset để kiểm tra thông thường. `scripts/reset_streaming.py` có thể xóa trạng thái streaming; chỉ chạy khi đã xem xét phạm vi dữ liệu và chủ động xác nhận.

## 7. Giới hạn

- Watermark 10 phút có nghĩa là sự kiện đến quá trễ không cập nhật ngay `user_ratings`, `user_seen` hoặc recommendation job. Archive Parquet vẫn giữ sự kiện hợp lệ đó cho lần batch tiếp theo.
- Kafka giữ dữ liệu hữu hạn; nếu job vắng mặt lâu hơn retention hoặc topic/checkpoint không còn đồng bộ, Spark có thể không khôi phục đủ offset.
- Fold-in chỉ tính lại vector user, không cập nhật item factors. Sách mới không có trong model không thể fold-in; user chưa có profile/rating cũng dùng fallback batch.
- Dữ liệu Goodbooks không có timestamp gốc. `event_time` chỉ thể hiện thời gian sự kiện streaming; thứ tự thời gian giữa các rating lịch sử trùng cặp không thể khôi phục chính xác.
- Các lượt kiểm thử lỗi cũ là bằng chứng lịch sử. Cần chạy lại audit, unit tests và kiểm thử UI/end-to-end sau khi các container khởi động được để xác nhận phiên bản hiện tại.
