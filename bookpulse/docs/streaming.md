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
- Mặc định `FOLD_IN_MIN_RATINGS=1`: chỉ cần một rating stream mới từ 4 sao trở lên kể từ snapshot gần nhất là worker giải lại vector ALS (có thể nâng ngưỡng qua biến môi trường). Trước ngưỡng này app giữ gợi ý hiện tại và báo tiến độ; rating 1–3 vẫn chỉ đánh dấu sách đã xem.
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

Mọi số dưới đây đo ngày 21/11/2026 trên code v2 hiện tại, mô hình `20261121_071127_693046`, sau đó nút Train lại tạo phiên bản `20261121_100150_952595` với cùng tham số và cùng kết quả (giờ trong tên file là UTC). Lần chạy đầu cùng ngày, trước khi sửa bước kiểm tra vector và phép chia train/test, nằm ở `reports/superseded/v2_first_model_20261121/` kèm README; không dùng số của nó.

| Thành phần | Trạng thái | Bằng chứng | Ghi chú |
|---|---|---|---|
| Batch clean / SQL / ALS / write-recs | ✓ Đã chạy | `reports/clean_20261121_062652.json`, `analytics_sql_20261121_062739.json`, `train_als_implicit_20261121_071137.json`, `write_recs_20261121_071405.json`, `pipeline_20261121_064931.json` | Pipeline đầy đủ 1.408 giây. `train_als.py` tự kiểm tra fold-in tái tạo đúng `userFactors` của 20 user (sai số 2,3e-8) trước khi lưu. |
| Đánh giá xếp hạng | ✓ Đã chạy | `evidence/session9/E_model_validation.json`, `G1_implicit_semantics.json` | 5.000 user test: P@10 0,248, Recall@10 0,168, NDCG@10 0,293 so với sách phổ biến 0,074 / 0,050 / 0,091; ngưỡng ≥ 3 sao cho P@10 0,215. |
| Fold-in offline và vector | ✓ Đã chạy | `reports/foldin_eval_20261121_072720.json` | 1.000 user; vector so khớp 20 user (sai số 2,1e-8); bảng ở mục 5. |
| Rating trong/ngoài top-10 | ✓ Đã chạy | `reports/foldin_diagnostic_20261121_073553.json` | Cả hai job `done`, `als-foldin`; vector đổi; sách vừa chấm bị loại khỏi top-10 app. |
| Lượt demo user 1 | ✓ Đã chạy | `reports/demo_trace_user1_20261121_073722.json`, `_101528.json` | Chấm 5 sao *The Great Gatsby* → 8,0 và 9,1 giây sau `als-foldin`, hai sách mới vào top-10. |
| Nút Train lại | ✓ Đã chạy | `reports/retrain_20261121_100422.json` | 390 giây: clean 53, train một lần fit 196 (fit 90), write_recs 140; P@10 vẫn 0,248, fold-in khớp 2,3e-8. |
| Latency end-to-end | ✓ Đã chạy | `reports/stream_latency_20261121_073152.json`, `_073528.json` | 100/100 event hoàn tất, 0 timeout; số đo mô phỏng, không phải cam kết production. |
| Streaming ingest / Kafka audit | ✓ Đã chạy | `reports/kafka_audit_20261121_073750.json` | 1.101 = 1.113 hợp lệ − 12 trễ; 989 cặp `user_ratings` = tổng `rating_stats`; 11 lỗi; kho Parquet 1.136 = 1.136. |
| Spark kill -9 fault test | ✓ Đã chạy | `reports/streaming/fault_test_20261121_142753/`, `reports/kafka_audit_20261121_073115.json` | 1.474 = 1.500 − 26 trễ; 15 lỗi; kho Parquet 1.532 = 1.532 (exactly-once); 0 offset gap. |
| Kafka / Mongo outage | ✓ Đã chạy | `reports/streaming/outage_test_20261121_171048/`, `reports/kafka_audit_20261121_101453.json` | Kafka tắt 44 giây, MongoDB 31 giây khi producer gửi 3.000 sự kiện; job streaming vẫn chạy qua cả hai lần; 3.000 = 3.000, 19 lỗi, kho Parquet 3.059 = 3.059. |
| Hadoop MapReduce ↔ Spark | ◐ Lịch sử | `reports/mapreduce_verify_20261105_170058.json` | Code MapReduce không đổi; chưa chạy lại. |
| Toàn bộ pytest | ✓ Đã chạy | `docker compose exec -T spark python -m pytest -q -p no:cacheprovider tests` | 55 passed. |

## 5. Đo lường đã lưu

**Fold-in offline** (`foldin_eval_20261121_072720.json`). Với mỗi user trong 1.000 user, một nửa sách test được "chấm lại" (replay), nửa còn lại là mục tiêu. "Trước" là danh sách batch đã ẩn các sách vừa chấm (cách bản cũ làm); "sau" là fold-in từ lịch sử train cộng các rating replay (cách worker làm).

| Kiểm chứng | Số đo |
|---|---:|
| Số user / replay / target ratings | 1.000 / 7.793 / 7.276 |
| Vector khớp Spark `userFactors`, 20 user: cosine / sai số tuyệt đối lớn nhất | 1,0000000 / 2,1e-08 |
| Precision@10, trước → sau | 0,1361 → 0,1380 |
| Recall@10, trước → sau | 0,1859 → 0,1882 |
| Top-10 Jaccard trước/sau | 0,816 |
| Fold-in, 1.000 user: trung bình / p95 | 0,0087 / 0,019 giây/user |

| Rating thích mới | User | Precision@10 trước → sau | Recall@10 trước → sau |
|---:|---:|---:|---:|
| 1 | 1.000 | 0,1243 → 0,1246 | 0,1718 → 0,1730 |
| 2 | 998 | 0,1265 → 0,1274 | 0,1742 → 0,1745 |
| 3 | 984 | 0,1289 → 0,1307 | 0,1770 → 0,1793 |
| 5 | 903 | 0,1381 → 0,1403 | 0,1794 → 0,1831 |
| 10 | 239 | 0,1908 → 0,1941 | 0,1813 → 0,1842 |

Fold-in tái tạo đúng vector Spark và cải thiện xếp hạng một chút ở cả năm nhóm, tăng dần theo số rating mới. Mức tăng nhỏ vì danh sách "trước" đã ẩn sách vừa chấm; lợi ích chính là gu đổi ngay thay vì chờ train lại. Đây là mô phỏng offline trên tập holdout, không phải đánh giá người dùng thật.

**End-to-end với 100 user test** gửi cùng lúc: đủ 100 event, 0 timeout ở cả hai lần đo. Gửi → `recs_live`: trung vị 8,59 giây, p95 9,40 (`stream_latency_20261121_073152.json`) và trung vị 21,81 giây, p95 23,11 (`_073528.json`, job vừa khởi động, batch đầu chậm). Phép tính fold-in trung vị khoảng 4 ms, ghi Mongo dưới 3 ms; thời gian còn lại là chờ trigger 5 giây và hàng đợi của một worker. Một user chấm riêng lẻ (lượt demo): 5,0 và 8,0 giây.

**Trace hai user test** (`foldin_diagnostic_20261121_073553.json`): chấm sách đang trong top-10 làm đổi vector (delta norm 0,145, cosine trước/sau 0,940, Jaccard top-10 0,667); chấm sách ngoài top-10 cũng đổi vector (0,166, 0,770, Jaccard 0,538). Cả hai job hoàn tất bằng `als-foldin`, sách vừa chấm bị loại khỏi top-10 app.

**Fault test** (`fault_test_20261121_142753/`): giết job bằng kill -9 sau 45 giây, chạy lại từ checkpoint; producer gửi 1.500 sự kiện hợp lệ (32 bản trùng, 15 lỗi, 26 trễ); MongoDB đếm 1.474 = 1.500 − 26, 15 lỗi bị loại, kho Parquet giữ đúng 1.532 tin hợp lệ. Một lượt trên máy local, không phải chứng minh HA.

**Hai lỗi tìm ra khi đo lại 21/11 (đã sửa):**
- Phép chia train/test sắp theo `rand(seed)` trong cửa sổ theo user. Khi một script lọc vài `user_id` rồi mới tính, Spark đẩy bộ lọc xuống trước cửa sổ nên `rand()` cho giá trị khác và tập train của user đó đổi; `foldin_eval.py` vì vậy so vector với sai tập train. Nay script cache phép chia trước khi lọc (ghi chú trong `common/recs.py`).
- `measure_latency.py` và `diagnose_foldin.py` xoá user test nhưng không tính lại `rating_stats`, nên `kafka_audit.py` báo lệch. Nay dùng `common/cleanup.py`.

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
