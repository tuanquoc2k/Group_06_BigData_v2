# BookPulse – Tóm tắt các việc cần cải thiện

Tổng hợp theo các buổi thảo luận về kiến trúc, streaming và ALS fold-in.

## Quyết định đã chốt

- Mọi lượt chấm đi qua **một đường duy nhất**: Kafka → query live → worker fold-in → `recs_live`. Không rẽ nhánh theo "trong hay ngoài top-10".
- **Chỉ rating ≥ 4** được dùng làm tín hiệu thích để train và fold-in (Cách 1). Rating thấp chỉ dùng để loại cuốn đó khỏi gợi ý.
- Fold-in là **inference** trên vector sách cố định, không phải ALS học tăng dần. Sách mới và user mới vẫn chờ lần train kế tiếp.
- Không huấn luyện lại ALS theo từng sự kiện: một lần train mất khoảng 5 phút, trong khi sự kiện đến liên tục (20 sự kiện/giây) và tranh tài nguyên với streaming. Dùng fold-in cho phản ứng tức thì, train lại theo lịch hoặc theo ngưỡng.

---

## 1. Lớp batch

- [x] `train_als.py`: lọc `rating >= ALS_MIN_RATING` (mặc định 4); split positive ratings seeded theo user, giữ rating stream ở train. Implicit ALS dùng `nonnegative=False` để fold-in giải đúng normal equation. Code hỗ trợ ngưỡng 3/4; benchmark Goodbooks cho cả hai cấu hình còn phải chạy.
- [x] Xuất `itemFactors` và `YtY` kèm `alpha`, `regParam`, `min_rating`, `model_version` trong artifacts/manifest phiên bản.
- [x] `clean.py`: giữ một dòng cho mỗi cặp (user, book); stream thắng lịch sử và event mới hơn thắng trong stream.
- [x] `write_recs.py`: loại mọi sách đã chấm, tạo `user_profiles_<version>` từ lịch sử, gate đầy đủ profile/recommendation trước khi active.
- [x] Khi `meta.active` đổi phiên bản, xóa live results cũ và chỉ phục vụ kết quả khớp active model.
- [ ] Tự động chạy train theo lịch/ngưỡng chưa triển khai; hiện có nút/manual `scripts/retrain.py`. Mỗi lần train sinh `model_version`.

## 2. Lớp streaming

- [x] Thêm **query live**: upsert `user_ratings` theo `(user_id, book_id)` (bản mới nhất thắng), tạo/gộp job `pending`.
- [x] Sự kiện có `event_time` do producer/app gán và `ingested_at` do stream job ghi.
- [x] Giữ `user_seen` làm lớp cầu nối để ẩn sách ngay; ghi chú sự kiện quá watermark có thể chưa vào lớp này.
- [x] `rating_stats` đếm sự kiện; chấm lại cùng sách có thể đếm hai lần, batch dedupe theo cặp ở lần retrain.

## 3. Recommendation Worker (mới)

- [x] Poll/atomic claim, debounce, retry và thu hồi timeout trong `recommendation_worker.py`.
- [x] Tính vector từ batch profile cộng stream overlay; rating thấp loại tín hiệu cũ nhưng vẫn nằm trong seen set. Ghi top-30, source/version/timestamps.
- [x] Ngưỡng fold-in mặc định 1 positive rating đã biết; dưới ngưỡng app dùng batch.
- [x] `alpha`/`regParam` lấy từ manifest; automated Spark test so fold-in với trained `userFactors` sai số tương đối < 1e-4.

## 4. MongoDB

- [x] `user_ratings`: unique `(user_id, book_id)`.
- [x] `recommendation_jobs`: index trạng thái/thời gian; chỉ một pending/user.
- [x] `recs_live`: `_id = user_id`, kèm `source`, `model_version`, timestamps/latency.

## 5. App Streamlit

- [x] Đọc live chỉ khi version khớp, nếu không dùng batch; lọc `user_seen`.
- [x] Hiển thị nguồn và `model_version`.
- [x] Cold-start dùng popular từ `meta`.

## 6. Đánh giá và đo đạc

- [ ] Chạy full Goodbooks với cấu hình mới, ngưỡng 3/4; báo cáo metric theo `model_version`. 0,204 là lịch sử, không dùng lại.
- [x] Split per-user seeded, test chỉ positive ratings; metric helper kiểm tra Precision/Recall.
- [x] `foldin_eval.py` replay/target holdout, P@10/Recall@10, Jaccard, vector match và timing; full-data result còn chờ chạy.
- [x] Worker lưu latency từ `event_time` tới `ingested_at` và `recommended_at`; chưa có production latency đo được.
- [x] Pipeline ghi elapsed time gồm `write_recs.py`; full pipeline chưa chạy lại để lấy số mới.
- [x] Tài liệu cảnh báo `online_eval` là sanity check vòng tròn, không phải offline quality.

## 7. Báo cáo và thuyết trình

- [x] Cập nhật speaker notes Markdown về MapReduce, kiến trúc fold-in và phần đã làm/kế hoạch; PPTX không chỉnh theo yêu cầu.
- [x] Sơ đồ HTML/README mô tả đường fold-in.
- [x] Bổ sung thiết kế cluster/cloud; đánh dấu rõ chưa triển khai.
- [x] Bổ sung MongoDB/Cassandra theo access pattern và CAP; không tuyên bố benchmark.
- [x] Tài liệu tránh kết luận Spark nhanh hơn Hadoop nếu không có phép đo cùng workload/config.

## Những điều phải nói đúng khi trình bày

- Fold-in **không phải** học tăng dần: vector sách giữ nguyên, chỉ vector user được tính lại.
- Sách mới và user mới vẫn chờ lần train sau.
- Sự kiện streaming là mô phỏng, không phải hành vi người dùng thật.
- Hệ thống chạy một node (một broker Kafka, một Mongo), chưa chứng minh được scale hay chịu lỗi; CAP chỉ ở mức lý thuyết.
- Hệ thống không dùng tín hiệu "không thích" (rating thấp chỉ để ẩn sách).
- `rating_stats` tính theo sự kiện; sự kiện trễ quá watermark có thể bị loại khỏi thống kê.
- Cluster/cloud mới ở mức thiết kế.

## Thứ tự làm gợi ý

1. Xuất `itemFactors` và lọc rating ≥ 4 ở train, rồi chạy lại P@10.
2. Viết worker, kiểm tra fold-in khớp `userFactors` của một user cũ.
3. Thêm query live và các collection mới.
4. Sửa app đọc `recs_live`.
5. Đo độ trễ và P@10 trước/sau fold-in.
6. Cập nhật slide và báo cáo.
