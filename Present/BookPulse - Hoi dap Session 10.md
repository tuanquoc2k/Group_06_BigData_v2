# BookPulse – Hỏi đáp Session 10 (Final Assessment)

Ôn hỏi đáp cho bài cuối kỳ. Các số liệu ALS bên dưới là **lịch sử của cấu hình cũ**, không đại diện cho code hiện tại (ngưỡng rating, split và cấu hình implicit đã đổi); không nêu chúng như kết quả mới. Chạy `scripts/run_pipeline.sh`, rồi trích báo cáo mới trong `bookpulse/reports/` trước buổi bảo vệ. Số liệu streaming và Hadoop vẫn là các lần đo được ghi rõ ngày, không phải cam kết chung. Syllabus ghi giảng viên hỏi về: **quyết định triển khai, xử lý phân tán, khả năng mở rộng và chuẩn đầu ra môn học**.

## Nguyên tắc

- **Ai trình bày phần nào trả lời phần đó**; Kiệt điều phối.
- **Trả lời bằng số đo và chỉ ra file bằng chứng** (`reports/…`).
- **Nói thẳng giới hạn** (một máy, sự kiện mô phỏng, thiết kế cluster chưa chạy thật). Giảng viên đánh giá cao việc tự chỉ ra vấn đề.
- **Không biết thì ghi nhận**, không đoán.

## Trả lời nhanh

| Nếu bị hỏi | Một câu |
|---|---|
| MapReduce làm gì? | Thống kê rating theo sách trên HDFS + YARN; combiner giảm 99,2% dữ liệu shuffle; khớp 100% Spark SQL |
| Spark SQL ở đâu? | `batch/analytics_sql.py`: temp view, 6 câu hỏi, query plan, kiểm tra join và SQL = DataFrame |
| Mô hình nào, tốt cỡ nào? | ALS implicit với tín hiệu rating ≥ 4, test split theo user và fold-in cho user đã hoạt động. Chưa có benchmark Goodbooks mới cho cấu hình hiện tại; 0,204/0,202 là số lịch sử, không dùng làm kết quả hiện tại. Chỉ báo cáo số sau khi `run_pipeline.sh` tạo report mới |
| Vì sao MongoDB? | Truy vấn chính theo user_id → document mỗi người, đọc bằng `find_one({_id})`; p50 0,51–0,94 ms, p95 1,14–2,81 ms (hai lần đo, một client, dữ liệu nằm trong cache: chưa đo dưới tải) |
| Batch và streaming chạy cùng lúc? | Không, trên máy này chạy lần lượt: hai driver 4 GB không vừa 7,7 GiB; trong lúc batch, sự kiện chờ trong Kafka (K19). Nút Train lại (`scripts/retrain.py`) tự dừng job streaming, chạy clean → train → write_recs rồi bật lại (164–179 s, `retrain_20261114_*.json`) |
| Streaming có gì? | Kafka 3 partition; 4 query: watermark + bỏ trùng, cửa sổ trượt 10/5 phút, kho lưu trữ, đếm lỗi; lượt kill -9 và lượt tắt Kafka/MongoDB: 0 mất, 0 trùng (kết quả đo, không phải bảo đảm; còn khe hở khi chết giữa lúc ghi MongoDB) |
| Lambda hay Kappa? | Theo ý tưởng Lambda; chưa hợp nhất batch view và speed view, app chỉ gộp lúc đọc; không Kappa vì 6 triệu rating lịch sử chưa từng qua Kafka (K3) |
| Chỉ số "Lượt 4–5 sao trúng top-10" nghĩa là gì? | Số lượt 4–5 sao mà cuốn đó nằm trong 10 gợi ý đã lưu. Với dữ liệu giả (cặp chưa có trong train) chỉ số khác 0 (15/11: 6/656 rồi 8/658 sau khi chấm 2 cuốn, `nosql_evidence_20261114_*.json`); phát lại `ratings.csv` thì luôn 0 vì sách đó đã bị loại khỏi gợi ý (08/11: 2/672, 2 lượt do `rate_from_recs.py`). Chỉ là sanity check, không phải thước đo chất lượng |
| Có phải Big Data không? | Goodbooks là bộ phát triển; đã đo giới hạn một máy; bộ đầy đủ ×38 cần cluster (slide 18) |

---

## Kiến trúc – phần giảng viên chú trọng (Hảo trả lời, cả nhóm cần nắm)

Mở slide 4 (sơ đồ tổng quan) khi trả lời, chỉ tay theo đường đi của dữ liệu; hỏi sâu về một thành phần thì mở slide 5 (sơ đồ chi tiết, cùng hình trong `bookpulse/docs/kien-truc.html`). Các câu hay bị hỏi có phần **Nói ngắn** (khoảng 30–45 giây) để nói trước; phần sau để đọc thêm khi bị hỏi tiếp.

**K1. Trình bày kiến trúc trong 30 giây.**
**Nói ngắn:** "Trái sang phải: lớp batch Spark làm sạch, phân tích, huấn luyện ALS và tính sẵn gợi ý; lớp tốc độ đọc Kafka, cập nhật số liệu, lưu rating mới và xếp job. Recommendation worker fold-in vector người dùng trên item factors cố định rồi ghi `recs_live`; app ưu tiên live, fallback batch. Hadoop MapReduce là nhánh kiểm chứng."

Slide 4 là sơ đồ tổng quan, slide 5 là sơ đồ chi tiết. **Nguồn · thu nhận**: Goodbooks CSV và Kafka topic `rating-events` (3 partition, key `user_id`). **Batch**: `clean.py` → Parquet → Spark SQL/ALS → `write_recs.py` ghi collection mới rồi chuyển `meta.active`; chỉ tín hiệu rating ≥ 4 vào ALS, mọi rating vẫn dùng để loại sách đã chấm. **Streaming**: `stream_stats.py` cập nhật speed view, `user_ratings` và `recommendation_jobs`, đồng thời lưu kho `stream_events`; `recommendation_worker.py` xử lý job qua debounce, giải user vector với item factors cố định rồi ghi top-30 vào `recs_live`. App ưu tiên `recs_live` cùng `model_version`, sau đó batch/popular; chấm sao chỉ gửi Kafka. Hadoop (tím) là nhánh MapReduce riêng để đối chiếu Spark SQL, không cấp dữ liệu cho app.

**K2. Vì sao chọn từng công nghệ?**
Kafka: log bền, đọc lại theo offset trong thời gian retention. Spark: một engine cho batch, SQL, ML và streaming; cache không đồng nghĩa mọi workload luôn chạy trong bộ nhớ. Parquet: dạng cột, nén và column pruning. MongoDB: truy vấn chính theo user nên document `_id=user_id` phù hợp. HDFS + YARN: nhánh MapReduce theo yêu cầu môn học và phép tính độc lập để kiểm chứng Spark SQL. Không kết luận Spark luôn nhanh hơn Hadoop; chỉ nêu phép đo có workload, cấu hình và plan cụ thể.

**K3. Đây là Lambda hay Kappa?**
**Nói ngắn:** "Theo ý tưởng Lambda: batch huấn luyện lại, streaming cập nhật số liệu trong vài giây, MongoDB phục vụ; chưa hợp nhất hai view. Không chọn Kappa vì 6 triệu rating lịch sử chưa từng qua Kafka và Kafka chỉ giữ 7 ngày, nên log không phải nguồn dữ liệu gốc."

Theo ý tưởng Lambda: lớp batch (huấn luyện lại ALS theo đợt), lớp tốc độ (streaming, vài giây), lớp phục vụ (MongoDB). Chưa đầy đủ: chưa hợp nhất batch view và speed view thành một số liệu (`rating_stats` chỉ chứa rating đến qua streaming); app chỉ gộp lúc đọc, ví dụ ẩn sách có trong `user_seen`. Không chọn Kappa **không phải** vì ALS không cập nhật từng sự kiện (Kappa cũng huấn luyện lại theo đợt bằng cách đọc lại log). Lý do thật: 6 triệu rating lịch sử chưa từng đi qua Kafka, và Kafka chỉ giữ 7 ngày, nên log không phải nguồn dữ liệu gốc. Muốn Kappa phải nạp lịch sử vào topic, giữ log lâu dài (tiered storage) rồi huấn luyện lại bằng cách đọc lại log. Cái giá của Lambda: hai đường xử lý riêng, và hiện chúng **không** cho cùng kết quả: `clean.py` giữ một rating mới nhất cho mỗi cặp (người dùng, sách), còn lớp tốc độ đếm mọi `event_id` khác nhau, nên chấm lại bị đếm hai lần trong `rating_stats`; sự kiện trễ hơn watermark bị lớp tốc độ bỏ nhưng batch giữ. Vì vậy nhóm không cộng hai view (K20).

**K4. Một thành phần chết thì sao?**
**Nói ngắn:** "Job streaming chết thì chạy lại từ checkpoint: kill -9 hai lần, 0 mất 0 trùng. Kafka tắt 40 giây: producer giữ trong bộ đệm, Spark tự nối lại; MongoDB tắt: job dừng, chạy lại từ checkpoint vẫn khớp 3.000 = 3.000. Một máy nên mọi thành phần là điểm lỗi đơn, chưa có restart policy."

| Chết | Hệ quả | Phục hồi |
|---|---|---|
| Job streaming | Sự kiện vẫn nằm trong Kafka | Chạy lại từ checkpoint; đã thử kill -9: 1.474 = 1.500 − 26, không mất, không trùng; lặp lại lần hai có kiểm tra kho Parquet: 1.475 = 1.500 − 25, kho 1.537 = 1.537 (`kafka_audit_20261108_040236.json`) |
| Kafka (đã thử tắt 40 giây) | Producer giữ tin trong bộ đệm, mỗi tin chờ được tới 120 giây (`request_timeout_ms`), failed = 0; job Spark vẫn chạy và tự nối lại, không cần khởi động lại | Topic nằm trên volume `kafka-data`. Lượt thử đầu, producer để mặc định của kafka-python 2.0.2: tin chờ trong bộ đệm hết hạn sau 30 giây, Kafka ngừng hơn 40 giây, không callback nào kiểm tra: mất 571 tin (gửi 3.078, Kafka có 2.507); phía Spark không mất gì so với Kafka. Đã sửa: `request_timeout_ms` 120 giây, `retries=10`, callback đếm tin gửi lỗi (K16) |
| MongoDB (đã thử tắt 30 giây) | App không đọc được; job Spark **dừng** (pymongo `ServerSelectionTimeoutError` sau 30 giây trong `foreachBatch`) | Script thử nghiệm chạy lại job từ cùng checkpoint sau 11 giây (`events.log` 11:11:17 → 11:11:28); chưa có restart policy, ngoài thử nghiệm phải chạy lại tay. Batch chưa ghi `last_batch` được phát lại; kết quả 3.000 = 3.000, trending 6.000 = 2 × 3.000, kho 3.059 = 3.059, 0 mất, 0 trùng (`kafka_audit_20261108_041203.json`). Một node, chưa có dự phòng; production dùng replica set |
| Job batch giữa chừng | App không bị ảnh hưởng, vẫn phục vụ bản cũ | `write_recs` ghi collection mới, đếm document đã ghi: đủ bằng số người dùng cần ghi mới chuyển `meta.active`; thiếu thì xóa collection mới, job báo lỗi, `meta.active` giữ bản cũ. Tên sách (`books`) được upsert theo `_id` trước khi chuyển nên không bị trống. Cổng đếm đủ đã chạy thật ở mỗi lần bấm Train lại (`write_recs_20261114_190314.json`, `_190701.json`: `expected_users` 53.424 = `users_written`); nhánh "thiếu" vẫn chỉ có trong code |
| Checkpoint bị xóa hoặc chuyển chỗ, MongoDB vẫn giữ | Batch id bắt đầu lại từ 0, nhỏ hơn `stream_state.last_batch` | Job **dừng và báo lỗi** thay vì âm thầm bỏ qua mọi batch tới `last_batch` (sửa 08/11, 3 unit test `AlreadyAppliedTest`); xử lý: chạy `reset_streaming.sh` để xóa cả hai cho khớp |
| Cụm Hadoop | Không ảnh hưởng luồng chính | MapReduce chỉ để thống kê và đối chiếu |

Nói thẳng: chạy một máy nên mọi thành phần là điểm lỗi duy nhất; không có restart policy: job dừng phải chạy lại tay (trong lượt thử là script chạy lại); production dùng restart policy hoặc orchestrator kèm cảnh báo. Thiết kế cluster ở slide 18. Bằng chứng: `bash scripts/stream_outage_test.sh` (khoảng 5 phút), dòng thời gian ở `reports/streaming/outage_test_20261108_110750/events.log`.

**K5. Tải tăng 100 lần thì nghẽn ở đâu trước?**
**Nói ngắn:** "Chưa thử tải. Nghẽn đầu tiên là phần ghi MongoDB của streaming đang ở driver; tiếp theo Kafka 1 broker và Spark local. Sửa: ghi từ executor sau khi gộp theo sách, thêm broker và partition, Spark trên Kubernetes hoặc YARN."

Đây là suy luận từ thiết kế, chưa có phép thử tải (K23). (1) Phần ghi MongoDB của streaming đang chạy ở driver (cả `valid` và `trending` đều `collect()` micro-batch về driver): chuyển sang executor (foreachPartition hoặc MongoDB Spark connector), gom theo sách trước khi ghi. (2) Kafka 1 broker, 3 partition: mặc định Spark đọc mỗi partition bằng một task (tùy chọn `minPartitions` chia nhỏ hơn được), phần có trạng thái sau shuffle chạy trên 8 partition (`spark.sql.shuffle.partitions`); thêm broker, replication 3, tăng partition. (3) Spark local: chạy trên YARN/Kubernetes với nhiều executor. (4) Huấn luyện ALS đã dùng 4,6/7,7 GiB RAM: cần cluster. (5) Kho sự kiện sinh nhiều file nhỏ (trigger 5 giây, chia theo ngày, không gộp file): kéo dài trigger của query lưu trữ hoặc gộp file định kỳ, hoặc dùng Delta Lake/Iceberg. Đã có `maxOffsetsPerTrigger` (20.000) để một batch không kéo quá nhiều sau khi job ngừng lâu.

**K6. Dữ liệu lưu ở đâu, giữ bao lâu?**
Kafka: mặc định retention 7 ngày trên volume. Kho Parquet giữ stream events cho batch đến khi reset; production không reset. Checkpoint nằm trên volume và phải khớp `stream_state`. MongoDB giữ recommendation/profile collections cho model active và previous; `trending` TTL 24 giờ; stream counters/seen tăng đến lần reset. ALS model artifacts có version; Parquet `ratings` vẫn overwrite khi batch chạy.

**K7. Vì sao app không gọi Spark mà đọc MongoDB?**
Gợi ý được tính trước theo đợt; truy vấn chính của app là một `find_one` theo `_id` đọc cả document người dùng (30 gợi ý + 10 sách bù), trung vị 0,51–0,94 ms qua hai lần đo (`find_one({_id}, {recs: {$slice: 10}})` trong `nosql_evidence.py`, 1.000 lần đọc tuần tự, một client, dữ liệu nằm trong cache: độ trễ đọc RAM, chưa đo dưới tải). Mỗi trang còn đọc `meta`, `user_seen` (index user_id), `books` và `rating_stats` theo `$in _id`, `stream_state`, `online_eval`, `trending`. App không phụ thuộc Spark đang chạy, và đổi mô hình chỉ là chuyển `meta.active` (nguyên tử). Đánh đổi xem K13.

**K8. Batch và stream có mâu thuẫn nhau không?**
**Nói ngắn:** "Profile theo active model chứa lịch sử đã chấm; stream overlay giữ rating mới nhất; worker dùng chúng để fold-in và loại seen books. Số liệu thống kê batch/stream vẫn khác ngữ nghĩa nên không cộng hai view."

Profile `user_profiles_<version>` được viết cùng model version và chứa mọi sách lịch sử đã chấm; rating stream mới nhất override rating profile theo cặp. Fold-in dùng chỉ rating ≥ ngưỡng làm preference; cả positive/low ratings vẫn thuộc seen set. `user_seen` giúp app ẩn rating ngay trong khi worker chạy. `clean.py` merge archive, bỏ duplicate event_id và giữ rating mới nhất cho batch. Lưu ý: lớp live `rating_stats` đếm event, batch giữ một giá trị mới nhất mỗi cặp; watermark có thể loại event khỏi thống kê live trong khi archive vẫn giữ. Demo events là mô phỏng; mọi P@10 cũ chỉ là số lịch sử.

**K9. Vì sao Spark không đọc HDFS, không chạy trên YARN?**
HDFS + YARN trong đồ án chỉ phục vụ MapReduce. Spark chạy `local[*]`: một JVM trên một máy, nên đọc HDFS có 1 DataNode, replication 1 không thêm chịu lỗi hay song song, chỉ thêm độ trễ RPC. Hai cụm tách thành hai file compose để giữ nguyên image Hadoop của bài lab và bật/tắt cụm Hadoop độc lập, vì cả hai cùng chạy trên một máy ảo Docker và chia chung 7,7 GiB RAM (tắt Hadoop sau phần MapReduce là để trả RAM); chúng chỉ nối với nhau qua thư mục chung `bookpulse/`. Trên cluster thật, Spark đọc HDFS/S3 và chạy trên YARN/Kubernetes (K18).

**K10. CAP và nhân bản thì sao?**
Hiện chưa có nhân bản: MongoDB standalone, Kafka 1 broker RF 1; một node chết là thành phần đó ngừng, nên chưa có lựa chọn CAP thật sự. Production: MongoDB replica set 3 node, ghi `w: majority` để không mất dữ liệu khi node chính đổi; đọc gợi ý bằng `secondaryPreferred` vì gợi ý cũ vài giây vẫn chấp nhận được (ưu tiên sẵn sàng), nhưng phải xử lý trường hợp secondary chưa có collection mới (K26). Kafka 3 broker, replication factor 3, `min.insync.replicas` 2, producer `acks=all`, để một broker chết không mất tin đã xác nhận. Producer hiện đã đặt `acks="all"`, nhưng với RF 1 thì tương đương `acks=1`; chỉ có ý nghĩa khi RF 3 và `min.insync.replicas` 2.

**K11. Schema thay đổi hoặc sự kiện lỗi thì sao?**
Sự kiện là JSON, không có schema registry; `from_json` dùng schema 7 trường (gồm `event_time`, `ingested_at`, `simulated`). Thêm trường thì bị bỏ qua; thiếu, đổi tên hay sai kiểu thì thành null và đi vào `rejected`. Sự kiện lỗi chỉ được đếm, không lưu, nên hết thời gian giữ của Kafka là mất hẳn. Production: thêm topic dead-letter cho sự kiện lỗi, dùng Avro/Protobuf với Schema Registry để kiểm soát thay đổi schema.

**K12. Vì sao 4 query riêng cùng đọc Kafka, không gộp một?**
Vì ngữ nghĩa khác nhau: `valid` và `trending` cần watermark và trạng thái (bỏ trùng, cửa sổ), `trending` ghi theo update mode; kho lưu trữ (`events_to_parquet`) không dùng watermark để giữ cả sự kiện trễ, ghi bằng file sink exactly-once; `rejected` chỉ đếm. Mỗi query một checkpoint riêng nên chạy lại độc lập. Cái giá: Kafka bị đọc 4 lần, và trạng thái bỏ trùng bị lặp lại ở các query có trạng thái. Với tải của đồ án thì chấp nhận được.

**K13. Vì sao tính gợi ý trước thay vì tính online? Người dùng mới, sách mới?**
Batch lists vẫn được tính trước để app đọc nhanh. Sau rating stream, worker có thể fold-in user vector khi có ít nhất 3 rating tích cực cho sách đã có item factors; vector sách không đổi và việc này không phải incremental training. User chưa đủ ngưỡng dùng batch/popular; sách mới chưa có item factor nên chờ batch retrain.

**K14. Bảo mật thế nào?**
Hiện cổng MongoDB (27017) và Kafka (9094) mở ra máy host, MongoDB không xác thực, Kafka PLAINTEXT. Chỉ chấp nhận được trên laptop. Production: mạng nội bộ, không mở cổng ra ngoài; SCRAM + TLS cho Kafka và MongoDB; người dùng MongoDB theo vai trò (app chỉ đọc); mật khẩu trong secret manager.

**K15. Theo dõi lag của streaming thế nào?**
Spark không commit offset vào consumer group của Kafka, nên công cụ lag exporter của Kafka không thấy lag của job. Dùng metric nguồn của Spark `avgOffsetsBehindLatest` (đã có trong `reports/stream_progress_*.json`) hoặc `StreamingQueryListener`, đưa lên Prometheus/Grafana và cảnh báo khi lag tăng. Spark UI chỉ mở cổng 4040; không chạy job batch khi job streaming đang chạy (UI thứ hai sang 4041, và hai driver 4 GB vượt 7,7 GiB RAM của máy ảo Docker; xem K19).

**K16. Kafka tắt thì có mất sự kiện không?**
**Nói ngắn:** "Phía Spark không mất: Kafka tắt 40 giây, job tự nối lại. Phía producer, lượt đầu mất 571 tin vì kafka-python hủy tin chờ quá 30 giây và không ai kiểm tra; nay chờ tới 2 phút, gửi lại và đếm tin lỗi, lượt sau Kafka có đủ 3.078 tin."

Phía Spark thì không: đã thử tắt Kafka 40 giây, job vẫn chạy và tự nối lại, đọc tiếp từ offset trong checkpoint. Phía producer thì lượt thử đầu có mất 571 tin (gửi 3.078, Kafka có 2.507; `reports/superseded/outage_test_20261108_110251_producer_no_retry/`). Nguyên nhân: kafka-python 2.0.2 cho tin nằm chờ trong bộ đệm hết hạn sau `request_timeout_ms` (mặc định 30 giây), Kafka ngừng hơn 40 giây, và không có callback nào kiểm tra kết quả gửi. Sửa quan trọng nhất là `request_timeout_ms` 120 giây (tin chờ được tới 2 phút); thêm `retries=10` (chờ 1 giây giữa các lần) cho request lỗi giữa chừng, và callback đếm tin gửi lỗi ("failed"). Lượt sau: failed = 0, Kafka có đủ 3.078 tin, MongoDB 3.000 = 3.000. Còn lại: Kafka ngừng lâu hơn khoảng 2 phút thì vẫn mất tin, nhưng nay được đếm chứ không mất âm thầm. Gửi lại có thể ghi một tin hai lần (bỏ trùng theo `event_id` xử lý được); kafka-python 2.0.2 không có producer idempotent nên sau khi gửi lại, thứ tự trong partition không bảo đảm; job dùng `event_time` nên chấp nhận được.

**K17. Phiên bản mô hình và quay lại bản cũ?**
**Nói ngắn:** "ALS artifact có `model_version`; writer ghi recommendations và user profile vào collection mới, chỉ chuyển `meta.active` sau khi cả hai đủ. Giữ bản active và bản trước; chưa có quality gate hay rollback tự động."

`model_version` đến từ manifest ALS; `serving_version` là lần `write_recs.py` chạy. `meta` trỏ tới `recs_collection` và `user_profiles_collection`, đồng thời giữ pointer trước đó. Cổng đếm document kiểm tra recommendations và profiles trước khi chuyển; collection staging bị xóa nếu kiểm tra thất bại. Gợi ý, profile và item-factor artifacts có phiên bản; Parquet `ratings` vẫn được thay tại chỗ. Chưa benchmark chất lượng trước khi active và chưa có lệnh rollback an toàn tự động; không sửa riêng một pointer vì profile/ALS model cũng phải khớp.

**K18. Lên cluster thật cần sửa gì?**
**Nói ngắn:** "Logic xử lý giữ nguyên; phải sửa lớp cấu hình (đường dẫn URI thay pathlib, SPARK_MASTER, checkpoint lên HDFS/S3, số shuffle partition, gói Kafka) và viết lại phần ghi MongoDB của streaming sang executor."

Logic xử lý (DataFrame, SQL, ALS) giữ nguyên; phải sửa lớp cấu hình và viết lại vài đoạn I/O (`has_parquet()` trong `clean.py`, phần ghi MongoDB của `valid`/`trending`): (1) `common/settings.py` dựng mọi đường dẫn bằng `pathlib`, mà `pathlib` biến `hdfs://nn/x` thành `hdfs:/nn/x` (s3a cũng vậy), nên phải dùng chuỗi URI; `has_parquet()` trong `clean.py` phải dùng FileSystem API của Hadoop. (2) Master lấy từ biến `SPARK_MASTER` (mặc định `local[*]`): đặt thành `yarn` hoặc `k8s://…`; chỉ dùng cờ `--master` của spark-submit thì bị `.master()` trong code ghi đè. (3) Thư mục checkpoint của ALS (`train_als.py`) và checkpoint streaming phải nằm trên HDFS/S3 vì executor ở máy khác. (4) Gửi `common/` kèm `--py-files`; executor cần `MONGO_URI` (`write_recs` ghi từ executor bằng `foreachPartition`). (5) Chuyển phần ghi MongoDB của `valid` và `trending` từ driver sang executor. (6) `settings.py` đang đặt mặc định cho laptop: `spark.sql.shuffle.partitions` = 8, driver 4 GB; đổi qua biến `SPARK_SHUFFLE_PARTITIONS`, `SPARK_DRIVER_MEMORY` hoặc bật AQE. Gói `spark-sql-kafka` đang tải từ Maven lúc chạy: phải đóng sẵn vào image hoặc dùng mirror nội bộ.

**K19. Lớp batch và lớp tốc độ có chạy cùng lúc không?**
Trên máy này không: hai driver 4 GB không vừa 7,7 GiB RAM, nên batch và streaming chạy lần lượt. `scripts/retrain.py` dừng stream, clean → train → write_recs rồi bật lại. Thời gian pipeline thay đổi theo máy, lấy từ `reports/pipeline_*.json`; sự kiện chờ trong Kafka. Trên cluster có thể chạy batch riêng và giữ streaming thường trực, nhưng thiết kế đó chưa triển khai.

**K20. Batch view và speed view có cho cùng con số không? Gộp thế nào?**
Không. `clean.py` giữ một rating mới nhất cho mỗi cặp (người dùng, sách) theo `event_time`; lớp tốc độ đếm mọi `event_id` khác nhau, nên một lượt chấm lại bị đếm hai lần trong `rating_stats`. Sự kiện trễ hơn watermark 10 phút bị lớp tốc độ bỏ, batch vẫn giữ (qua kho Parquet). Vì vậy nhóm không cộng hai view. Muốn gộp cần: (1) lớp tốc độ theo khóa (người dùng, sách) để trừ rating cũ khi chấm lại; (2) một mốc cắt: ghi vào `meta` `event_time`/offset lớn nhất mà batch đã học, speed view lưu theo từng khoảng thời gian, chỉ cộng các khoảng sau mốc, xóa các khoảng batch đã học. Chưa làm: hiện `rating_stats` và `user_seen` tăng dần cho đến khi reset bằng tay.

**K21. Rating đến rất trễ có bị ẩn khỏi gợi ý không?**
Chưa: `user_seen` được ghi trong query `valid`, tức sau watermark. Một rating thật đến trễ hơn 10 phút (ví dụ máy khách offline) bị bỏ ở đó, nên app chưa ẩn sách cho đến lần batch sau, dù kho Parquet vẫn giữ sự kiện. `user_seen` là upsert idempotent, nên có thể ghi từ một nhánh không trạng thái, không watermark, giống query lưu trữ. Nhóm chưa sửa.

**K22. Lượt kill -9 có chứng minh khe hở cộng lặp đã đóng không?**
Không. Khe hở trước 15/11 là cả khoảng ghi ba collection của một batch (`rating_stats` $add, `user_seen`, `online_eval` $inc) trước `mark_applied`, với batch 20.000 dòng có thể tới vài giây; kill ở giây thứ 45 gần như chắc chắn không rơi vào đó, nên "0 trùng" chỉ chứng minh đường phục hồi bình thường. **Đã đóng ngày 15/11** (`streaming/stream_stats.py`, hàm `apply_valid_rows`): mỗi document `rating_stats`/`online_eval` ghi `last_batch` nó đã hấp thụ và update được lọc `last_batch < batch_id`, MongoDB nguyên tử trên một document nên không cần transaction; `user_seen` vốn là upsert idempotent. Kiểm chứng: ghi cùng một batch hai lần vào database thử → n_ratings, hits không đổi. Còn lại: `failOnDataLoss=false`.

**K23. Đã thử tải chưa? Chịu được bao nhiêu sự kiện mỗi giây?**
Chưa thử tải, chưa biết thông lượng tối đa. Ở 20 sự kiện/giây, batch mất 3–6 giây so với trigger 5 giây khi đo 08/11 với Docker 4 CPU (lần tập 15/11 với 12 CPU: batch dài nhất 3 giây), phần lớn là chi phí cố định (4 query cùng đọc Kafka). `maxOffsetsPerTrigger` 20.000 chưa được hiệu chỉnh. Cách thử: tăng dần tốc độ producer cho đến khi `avgOffsetsBehindLatest` tăng mãi, đó là trần của cấu hình hiện tại.

**K24. Vì sao watermark 10 phút, trigger 5 giây, cửa sổ 10 phút trượt 5 phút?**
Chọn theo kịch bản, không đo (dữ liệu mô phỏng nên không có độ trễ thật). Production: đo (processing time − event time) rồi đặt watermark gần p99. Kho lưu trữ giữ sự kiện trễ cho batch, nên watermark ngắn hơn chỉ làm số liệu tức thời kém đủ hơn, không mất dữ liệu huấn luyện.

**K25. Kho sự kiện do job streaming ghi và bị reset xóa: có ổn không?**
Không ổn cho production: dữ liệu gốc để huấn luyện lại phụ thuộc vào job tính speed view, và `reset_streaming.sh` xóa luôn kho. Production: tách việc lưu ra một thành phần riêng (ví dụ Kafka Connect S3 sink) ghi topic vào bộ lưu trữ bất biến; job streaming chỉ tính speed view.

**K26. Đọc từ secondary (K10) có vấn đề gì với việc chuyển `meta.active`?**
Có: secondary có thể đã thấy `meta` mới nhưng chưa có collection gợi ý mới, `find_one` trả về None và người dùng bị coi là người mới. Sửa: ghi gợi ý và `meta` bằng `w: majority`, đọc `meta` và gợi ý bằng read concern majority trong một causally consistent session; hoặc không thấy thì đọc `previous_collection`.

**K27. App đọc thẳng MongoDB có ổn không?**
Ổn cho demo. Production đặt một API chỉ đọc ở giữa: xác thực, cache, giới hạn số request, gộp batch view và speed view ở một chỗ, và giấu `meta.active` khỏi client.

**K28. Lên bộ UCSD đầy đủ (876 nghìn người dùng, 2,36 triệu sách) thì nghẽn ở đâu?**
Phía batch: nghẽn là `recommendForAllUsers` (người dùng × sách ≈ 2 × 10^12 phép nhân vô hướng ở rank 50), không phải bước fit; bản thân các vector nhỏ (khoảng (0,88 + 2,36) triệu × 50 × 4 byte ≈ 650 MB). Giảm bằng tập ứng viên, hoặc lưu vector rồi tìm láng giềng gần đúng (ANN) lúc truy vấn. MongoDB: ước tính 1,5–2 KB mỗi document người dùng (cần xác nhận bằng `collStats`), tức khoảng 1,5–2 GB mỗi phiên bản, vừa một replica set; chỉ cần sharding theo hashed user_id khi lên hàng chục triệu người dùng. Streaming: ở 2.000 sự kiện/giây, trạng thái bỏ trùng 10 phút khoảng 1,2 triệu khóa; state store mặc định (HDFSBackedStateStore) giữ trong heap, nên dùng RocksDB state store (có trong Spark 3.5) và checkpoint trên S3.

**K29. Chi phí chạy trên cloud?**
Chưa tính chi tiết. Chi phí cố định: Kafka (3 broker), MongoDB (3 node) và job streaming chạy thường trực. Chi phí thay đổi: cụm Spark tạm thời cho huấn luyện theo lịch (dùng spot instance được, vì job chạy lại được). Vài GB Parquet trên S3 là không đáng kể.

---

## Hảo – dữ liệu, kiến trúc, MapReduce, Spark SQL, cluster

**1. 6 triệu dòng thì có phải Big Data không?**
**Nói ngắn:** "Không, đây là bộ phát triển; nhóm đã đo giới hạn một máy: luồng batch đỉnh 4,6/7,7 GiB RAM (lúc write_recs; ALS 2,2–2,5 GiB), local[4] chỉ nhanh 1,95 lần so với local[1], và với bộ UCSD gấp 38 lần thì recommendForAllUsers là 2×10¹² tích vô hướng, ước tính không vừa một máy. Lên cluster thì logic xử lý giữ nguyên; phải sửa lớp cấu hình (đường dẫn URI thay pathlib, SPARK_MASTER, checkpoint lên HDFS/S3, số shuffle partition, gói Kafka) và viết lại phần ghi MongoDB của streaming sang executor."

Không, và nhóm nói rõ đây là bộ phát triển. Nhóm đã đo giới hạn của một máy: RAM đỉnh 4,6/7,7 GiB (`reports/scaling/docker_stats_20261104_batch.csv`), 4 core chỉ nhanh 1,95 lần. Bộ UCSD đầy đủ có 229 triệu tương tác (gấp 38 lần) sẽ không vừa. Khi lên cluster, logic xử lý (DataFrame, SQL, ALS) giữ nguyên nhưng phải sửa lớp cấu hình và viết lại vài đoạn I/O: đường dẫn đang dựng bằng `pathlib` (biến `hdfs://` thành `hdfs:/`) phải thành chuỗi URI, `clean.py` dùng FileSystem API của Hadoop, master đặt qua biến `SPARK_MASTER` (cờ `--master` của spark-submit bị `.master()` trong code ghi đè), checkpoint lên HDFS/S3; chi tiết ở K18, thiết kế triển khai ở slide 18.

**2. Vì sao vẫn cần MapReduce khi đã có Spark?**
Môn học yêu cầu, và nó cho thấy mô hình map → shuffle/sort → reduce mà Spark kế thừa. Nhóm dùng nó như một cách tính độc lập để kiểm chứng Spark SQL: hai hệ thống, hai cách viết, cùng kết quả. Trên thực tế Spark nhanh hơn vì giữ dữ liệu trong bộ nhớ giữa các bước.

**3. Combiner có luôn dùng được không?**
Chỉ với phép toán có tính kết hợp và giao hoán (tổng, đếm). Trung bình thì không: trung bình của trung bình là sai. Nhóm mang theo (tổng, số lượt) và chia ở reducer; unit test kiểm tra kết quả không đổi khi combiner chạy 0, 1 hay nhiều lần.

**4. Vì sao có 4 map task khi chỉ có 3 block?**
Số map task thay đổi theo lần chạy: 05/11 có 4 (134 giây), lần tập 07/11 có 3 (job 52 giây), kết quả giống hệt nhau. Task thứ tư là bản speculative execution: ApplicationMaster của job MapReduce (không phải YARN) chạy thêm bản dự phòng cho task chậm, bản nào xong trước thì giữ, bản kia bị hủy; đây là cơ chế chịu lỗi của MapReduce. Log `job_20261105_165718.log`: Launched map tasks = 4, Killed map tasks = 2, Killed reduce tasks = 1; mỗi split trong 3 split hoàn tất đúng một lần.

**5. Có data locality không?**
Không: mọi map task đều rack-local, 0 data-local (`Rack-local map tasks` trong `reports/mapreduce/job_*.log`), vì DataNode và NodeManager chạy ở hai container riêng. Cluster thật đặt hai dịch vụ chung một node để map đọc block tại chỗ.

**6. Shuffle là gì, sao quan trọng?**
Là bước gom dữ liệu cùng key về cùng reducer/partition, phải truyền qua mạng và sắp xếp, nên tốn kém nhất. Nhóm giảm shuffle bằng combiner (MapReduce) và broadcast join (Spark SQL); query plan chỉ có một `Exchange` shuffle, cộng một `BroadcastExchange` gửi bảng sách nhỏ.

**7. Vì sao BroadcastHashJoin?**
Bảng sách 10.000 dòng (đọc từ `books.csv`) nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold` (10 MB), nên Spark gửi nguyên bảng tới mọi task, không phải shuffle bảng rating 6 triệu dòng.

**8. Vì sao LEFT JOIN chứ không phải inner join?**
Inner join âm thầm bỏ rating của sách không có trong `books.csv`. Nhóm dùng LEFT JOIN, gắn nhãn UNKNOWN và kiểm tra số dòng không đổi (5.976.479 trước và sau; 0 dòng UNKNOWN với Goodbooks).

**9. Parquet nhanh hơn CSV bao nhiêu? Sao số thay đổi?**
3,9–8,4 lần qua các lần đo (lần chạy cuối 4,22 s so với 1,08 s = 3,9 lần); dao động do bộ nhớ đệm của hệ điều hành. Bằng chứng chắc chắn là query plan: Parquet chỉ đọc 2 cột cần dùng (column pruning), có nén và schema sẵn.

**10. Có partition dữ liệu không?**
Không: mọi truy vấn quét toàn bộ, và partition theo book_id sẽ sinh 10.000 thư mục nhỏ. Với dữ liệu lớn hơn, partition theo thể loại (cách lấy tập con) hợp lý hơn.

**11. Triển khai lên cloud thế nào? Đã chạy thật chưa?**
Chưa chạy thật, đây là thiết kế: Kafka 3 broker, object storage (S3/MinIO) cho dữ liệu và checkpoint, Spark trên Kubernetes/YARN mở rộng theo executor, MongoDB replica set, Prometheus/Grafana (lag lấy từ metric `avgOffsetsBehindLatest` của Spark, K15), secret manager và TLS. Hiện mọi thành phần chạy một bản trên một máy (Kafka 1 broker, Mongo standalone, Spark `local[*]`, 1 DataNode), nên mỗi thành phần là một điểm lỗi đơn.

**11b. Vì sao MongoDB thay vì Cassandra? CAP được xử lý thế nào?**
Truy vấn gợi ý chính là point read theo `user_id` và thay danh sách top-30 theo user; MongoDB document `_id=user_id` khớp access pattern và hỗ trợ metadata linh hoạt. Cassandra cũng phù hợp nếu partition theo `user_id`, nhưng cần thiết kế query-specific/denormalized và quản lý version rõ ràng. Không gán CAP tuyệt đối: Mongo replica set với majority có thể ngừng ghi khi mất majority; Cassandra cho chọn consistency level theo thao tác để cân bằng consistency/availability. Đây là phân tích thiết kế, không phải benchmark; cấu hình một node hiện tại chưa kiểm chứng CAP/HA.

## Kiệt – Spark ML, MongoDB, hiệu năng

**12. Vì sao không chọn mô hình có RMSE tốt nhất?**
Người dùng thấy top-10 nên chọn implicit và đánh giá ranking; RMSE không áp dụng cho implicit. Các số P@10 0,204 và NDCG@10 0,235 là benchmark lịch sử của cấu hình cũ, không phải kết quả threshold/split/fold-in hiện tại. Chỉ đưa số mới sau khi chạy pipeline và trích report phiên bản mới.

**13. ALS implicit khác explicit thế nào?**
Implicit dùng rating ≥ ngưỡng (mặc định 4) làm tín hiệu thích; số sao điều chỉnh confidence `c = 1 + alpha × rating`. Rating thấp không phải tín hiệu không thích nhưng vẫn loại sách khỏi gợi ý.

**14. Có data leakage không?**
Không có leakage kiểu feature: baseline và popularity tính từ train; tham số chọn trên validation; test chỉ dùng một lần. CSV không có timestamp nên random split có thể tạo temporal leakage giữa người dùng cùng sách; cần thận trọng khi diễn giải metric. Cấu hình hiện tại bỏ rating dưới ngưỡng khỏi train và fold-in; mọi rating vẫn được loại khỏi recommendations.

**15. Precision@10 0,204 có thấp không?**
0,204 là kết quả lịch sử, không trả lời cho mô hình hiện tại. Sau khi chạy `scripts/run_pipeline.sh`, đọc `ranking_test` trong report mới, kèm model version, ngưỡng và số user đánh giá; không suy rộng từ một lần đo hay `online_eval`.

**16. Mô hình implicit có chỉ gợi ý sách phổ biến không?**
Đánh giá độ phủ trên report cùng phiên bản: `distinct_books_recommended` và baseline popularity. Số 1.224/1.208 là benchmark cũ, không dùng làm kết quả hiện tại; cần xem top-N mới trước khi kết luận về độ phổ biến.

**17. Mô hình phục vụ học trên bao nhiêu dữ liệu?**
Mô hình phục vụ fit trên `train` sau khi tách test theo user; mọi rating stream được giữ trong train. Tham số được chọn trên validation; test không tham gia fit. Xem `rows` và `best_params` trong report cùng `model_version`; số dòng lịch sử không đại diện cho lần chạy mới.

**18. Dữ liệu trong MongoDB tổ chức thế nào? Vì sao không Cassandra?**
Mỗi user có document `_id=user_id`, `recs` top-30 và fallback; `recs_live` cũng keyed theo user. MongoDB khớp point-read và atomic replacement theo user, metadata linh hoạt. Cassandra có thể dùng partition key `user_id` cho read/write phân tán, nhưng schema phải định hướng truy vấn và denormalize theo access pattern. Với replica set MongoDB, majority read/write ưu tiên consistency và có thể từ chối thao tác khi không còn majority; Cassandra consistency level có thể chỉnh theo từng thao tác. Không thể kết luận CAP chỉ từ tên database, và benchmark cũ (một node, cache nóng) không so được hiệu năng/HA giữa hai hệ.

**19. Đổi mô hình mới thì app có bị gián đoạn không?**
Không: gợi ý mới ghi vào collection mới; ghi đủ số người dùng mới chuyển `meta.active` (cập nhật một document là nguyên tử), thiếu thì bỏ bản mới và giữ bản cũ; giữ bản đang dùng và bản trước đó để quay lại. Tên sách (`books`) được upsert theo `_id` trước khi chuyển, nên không có lúc trống. Giới hạn: chỉ gợi ý có phiên bản, mô hình và Parquet bị ghi đè, kiểm tra trước khi chuyển chỉ về độ đủ, chưa so chất lượng (P@10), quay lại phải sửa `meta` bằng tay (K17).

**20. Vì sao 4 core chỉ nhanh 1,95 lần?**
Định luật Amdahl: phần tuần tự ở driver và shuffle giữa các vòng lặp ALS không tăng tốc được (1,95× ở 4 luồng ⇒ phần tuần tự ≈ 35%). Nói chính xác: thí nghiệm đo `local[1|2|4]` tức số luồng task của Spark, không giới hạn CPU của JVM: `docker stats` thấy tới 451% CPU ngay ở local[1] (GC, Python, I/O), nên không phải "4 core vật lý"; đo 04/11 khi Docker cấp 4 CPU, nay 12 CPU; mô hình đo là ALS explicit rank 10, mỗi điểm 1 lần.

## Tuấn – streaming, kiểm thử

**21. Event time khác processing time thế nào? Watermark để làm gì?**
Event time là lúc rating xảy ra, processing time là lúc Spark xử lý. Thống kê dựa trên event time. Watermark (10 phút) là ngưỡng chờ sự kiện trễ: cũ hơn thì bị loại khỏi phép tính có trạng thái, và trạng thái cũ được giải phóng nên bộ nhớ không tăng mãi. Checkpoint mỗi query có ba phần: `offsets/N` là khoảng offset của batch N, ghi **trước** khi chạy batch (write-ahead log); `commits/N` chỉ là dấu "batch N đã xong" (có `nextBatchWatermarkMs`, không chứa offset); `state/` là state store (bỏ trùng, cửa sổ). Batch có trong `offsets/` mà chưa có trong `commits/` được chạy lại với đúng batch id và offset cũ.

**22. Vì sao output mode `update` cho cửa sổ?**
`append` chỉ ghi khi cửa sổ đã đóng (sau watermark), tức chậm khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot".

**23. Có exactly-once không?**
**Nói ngắn:** "Không hoàn toàn: Kafka + checkpoint là at-least-once; kho Parquet exactly-once, trending ghi đè nên idempotent, rating_stats bỏ batch đã ghi bằng last_batch. Còn khe hở nếu chết đúng lúc ghi MongoDB, và 0 mất là kết quả đo."

Không hoàn toàn. Kafka + checkpoint là at-least-once; kho Parquet exactly-once nhờ file sink; `trending` ghi đè document (ReplaceOne) nên phát lại vẫn đúng. `rating_stats` bỏ qua batch đã ghi bằng `last_batch`, nhưng còn một khe hở: nếu job chết đúng lúc đang ghi MongoDB (`rating_stats` và `online_eval` cộng dồn trước khi ghi `last_batch`), batch phát lại có thể cộng lặp một phần; lượt kill -9 gần như chắc chắn không rơi vào khe hở này (K22). Sửa triệt để bằng transaction (replica set) hoặc chặn theo từng document (`last_batch` trong mỗi document sách, K22). Nếu checkpoint bị xóa mà MongoDB còn giữ, job nay dừng và báo lỗi thay vì bỏ qua batch âm thầm (K4). "0 mất" là kết quả đo, không phải bảo đảm: job đặt `failOnDataLoss=false`, nên nếu job dừng lâu hơn thời gian Kafka giữ log (mặc định 168 giờ) hoặc topic bị tạo lại, Spark chỉ ghi cảnh báo rồi bỏ qua offset đã mất. Production: `failOnDataLoss=true` và cảnh báo theo lag.

**24. Đã thử sự cố thế nào?**
**Nói ngắn:** "stream_fault_test.sh kill -9 job giữa chừng, chạy lại từ checkpoint, kafka_audit đếm lại Kafka: 1.474 = 1.500 trừ 26 trễ, 0 mất 0 trùng. Lặp lại lần hai và thử tắt Kafka, MongoDB cũng khớp."

`bash scripts/stream_fault_test.sh` (khoảng 3 phút, lặp lại được): 1.500 sự kiện, `kill -9` cả Python lẫn JVM sau 45 giây, chạy lại từ checkpoint. `kafka_audit.py` đối chiếu Kafka với MongoDB: 1.474 = 1.500 − 26 sự kiện trễ (số 26 suy từ kafka_audit, xem #25), tổng cửa sổ 2.948 = 2 × 1.474, 15 sự kiện lỗi; lượt này 0 mất, 0 trùng (`reports/kafka_audit_20261107_220738.json`). Lặp lại lần hai có thêm kiểm tra kho Parquet: 1.475 = 1.500 − 25, kho 1.537 dòng = 1.537 tin hợp lệ, 1.500 `event_id` khác nhau, 0 file chưa commit (`kafka_audit_20261108_040236.json`). Thêm `bash scripts/stream_outage_test.sh` (khoảng 5 phút): tắt Kafka 40 giây rồi MongoDB 30 giây khi đang gửi 3.000 sự kiện; 3.000 = 3.000, 0 mất, 0 trùng (`kafka_audit_20261108_041203.json`, xem K4, K16). Giới hạn: các lượt này kiểm đường phục hồi bình thường, chưa cài lỗi đúng giữa lúc ghi MongoDB (K22).

**25. Sự kiện trễ thì sao?**
Lớp tốc độ bỏ: 26 sự kiện trễ ở lượt kill -9 (suy từ kafka_audit: 1.474 = 1.500 − 26) và 19 ở lượt demo (983 = 1.002 − 19). Số 26 không do Spark đếm: file tiến độ chỉ ghi 0 trước khi kill + 23 sau khi chạy lại, vì batch commit ngay trước `kill -9` nằm sau lần ghi file tiến độ cuối (mỗi 10 giây), nên không có trong file nào; lượt lặp lại cũng vậy (file ghi 0 + 23, kafka_audit 25 trễ). Tức bộ đếm tiến độ có thể **thiếu** batch commit ngay trước khi bị kill. Điều kiện: sự kiện trễ đến sau khi job đã có watermark; Spark 3.5 lọc bằng watermark của batch trước, nên 1–2 batch đầu sau khi khởi động **mới** (checkpoint trống) chưa lọc; chạy lại từ checkpoint thì watermark được khôi phục (job thứ hai của lượt kill -9 vẫn loại 23). Vì vậy producer chỉ gửi sự kiện trễ sau 30 giây: `--late 0.05` là 5% số sự kiện gửi sau giây thứ 30 (lượt demo: 19/1.000). Kho lưu trữ giữ lại, nên lần huấn luyện sau vẫn học được rating đó.

**26. Vì sao key Kafka là user_id?**
Mọi sự kiện của một người vào cùng một partition (Kafka giữ thứ tự trong một partition), và tải chia đều giữa các partition. Job **không** dựa vào thứ tự đến: thống kê dùng `event_time`, `clean.py` giữ rating mới nhất theo `event_time`. Mặc định nguồn Kafka của Spark tạo một task đọc cho mỗi partition (tùy chọn `minPartitions` chia nhỏ hơn được); phần có trạng thái sau shuffle chạy trên 8 partition (`spark.sql.shuffle.partitions`). Đánh đổi: người dùng hoạt động nhiều có thể làm lệch tải giữa các partition.

**27. Theo dõi hệ thống thế nào?**
Mỗi query có tên trên Spark UI (tab Structured Streaming); file `reports/stream_progress_*.json` ghi tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, kích thước trạng thái, watermark; `kafka_audit.py` lưu phép đối chiếu Kafka – MongoDB – kho Parquet. Lag: Spark không commit offset vào consumer group của Kafka nên lag exporter của Kafka không thấy; dùng `avgOffsetsBehindLatest` (đã có trong `stream_progress_*.json`) hoặc `StreamingQueryListener` (K15). Batch đầu sau khi khởi động mất 7–12 giây (JVM còn lạnh), các batch sau thường 3–6 giây, sát trigger 5 giây (lượt kill -9 và lượt demo); trong lượt tắt Kafka/MongoDB có batch kéo dài gần 1 phút (57,7 giây, `stream_progress_20261108_040808.json`; lượt đầu với producer chưa sửa: 61 giây).

**28. Tỷ lệ trúng online trên app bằng 0? Vì sao hiện "2 / 672"?**
Số 2/672 là lượt 08/11 khi `producer.py` còn phát lại `ratings.csv`: rating đã có trong dữ liệu huấn luyện nên 670 lượt 4–5 sao không thể trúng, 2 lượt trúng do `rate_from_recs.py`. Từ 10/11 demo dùng dữ liệu giả (cặp chưa có trong train) nên chỉ số khác 0 ngay khi producer chạy (15/11: 6/656, rồi 8/658 sau khi chấm 2 cuốn; `nosql_evidence_20261114_*.json`; lượt 10/11 là 11/651 nhưng không lưu report). Đánh giá chất lượng mô hình vẫn dựa trên tập test offline; chỉ số này chỉ là sanity check (dữ liệu giả rút theo độ phổ biến, form Chấm sao chỉ cho chấm sách trong top-10 nên lượt chấm tay luôn là hit). Định nghĩa "trúng": sách nằm trong 10 gợi ý đầu **đã lưu** của phiên bản đang dùng (không bỏ qua sách đã ẩn, không tính sách bù); bộ đếm cộng dồn qua các phiên bản, `model_version` là bản mới nhất.

**29. Ghi MongoDB của streaming có phân tán không?**
Không: hai query `valid` và `trending` gom mỗi micro-batch về driver (`collect()`) rồi ghi bằng một client MongoDB (`rejected` chỉ đếm), đủ cho tốc độ sự kiện của đồ án; `maxOffsetsPerTrigger` 20.000 giới hạn một batch sau khi job tắt lâu. Khi tải lớn, chuyển việc ghi sang executor (`foreachPartition` hoặc MongoDB Spark connector) và gộp theo sách trong Spark trước khi ghi.

**30. Tắt Docker thì Kafka có mất dữ liệu không?**
`docker compose down` không mất: Kafka lưu log trên volume `kafka-data` (trước đây topic nằm trong container và bị xóa, nhóm đã sửa). `down -v` xóa mọi volume, khi đó phải chạy `bash scripts/reset_streaming.sh --yes` để xóa luôn checkpoint, `stream_state` và kho `stream_events` cho khớp nhau.

**31. Kiểm thử gồm những gì?**
13 unit test, đều qua (MapReduce, hàm gợi ý, 2 test chia train/test giữ rating từ streaming trong tập train, file tiến độ, và 3 test `AlreadyAppliedTest`: batch mới được ghi, batch phát lại bị bỏ qua, checkpoint lệch MongoDB thì dừng) và các phép đối chiếu độc lập: MapReduce so với Spark SQL, SQL so với DataFrame, Kafka so với MongoDB (`kafka_audit.py`, khối `mongo_check`), gợi ý không chứa sách đã chấm (`nosql_evidence.py`), thử `kill -9` bằng `stream_fault_test.sh` (nay kiểm tra cả kho Parquet), và thử tắt Kafka, MongoDB bằng `stream_outage_test.sh`.

## Câu chung

**32. Dữ liệu thay đổi thì hệ thống xử lý thế nào?** (câu giảng viên đã hỏi ở Session 8)
Hai tốc độ: streaming cập nhật số liệu và nhận rating; worker gộp rating stream với `user_profiles_<version>` rồi fold-in user vector khi có ít nhất 3 rating tích cực đã biết, không huấn luyện lại ALS. App ưu tiên `recs_live` nếu khớp active model version, đồng thời `user_seen` ẩn sách vừa chấm; nếu chưa đủ ngưỡng thì dùng batch/popular. Rating được archive để batch retrain sau, nhưng sách mới vẫn cần batch tạo item factor. Retrain thủ công qua nút /setting; scheduler chưa cấu hình.

**33. Điều khó nhất / bài học lớn nhất?**
Kiểm chứng: nhiều vấn đề chỉ lộ ra khi chạy thật và đọc ngược kết quả. Ví dụ: mô hình RMSE tốt nhưng gợi ý kém; sự kiện trễ mất khỏi dữ liệu huấn luyện; watermark chưa lọc ở batch đầu; topic Kafka mất khi `docker compose down`; sự kiện phát lại không thể làm đổi gợi ý; image MongoDB và pyarrow lỗi do không ghim phiên bản; producer để timeout mặc định 30 giây và không kiểm tra kết quả gửi nên mất 571 tin khi Kafka tắt hơn 40 giây.

**34. Ai làm gì?** *(cần thống nhất)*
Hảo: dữ liệu, batch, Spark SQL, MapReduce, thiết kế triển khai. Tuấn: streaming và đánh giá. Kiệt: Spark ML, MongoDB, app. Nhóm xác nhận lại trước khi nộp.

**35. Chạy lại được không?**
Có: `docker compose up -d`, rồi `bash scripts/run_pipeline.sh` (khoảng 30 phút; dừng job streaming trước, sự kiện chờ trong Kafka, K19) hoặc nút Train lại (3 phút, một bộ tham số); MapReduce theo README mục 8 (trong Git Bash cần `export MSYS_NO_PATHCONV=1`); thử sự cố streaming bằng `bash scripts/stream_fault_test.sh` (khoảng 3 phút) và `bash scripts/stream_outage_test.sh` (khoảng 5 phút); mọi số đo tự ghi vào `reports/`. Nói thẳng ba giới hạn: (1) chưa thử clone sạch trên máy lạ, chưa có `requirements.lock`; (2) `randomSplit` seed 42 chỉ tái lập khi Parquet không đổi: ba lần clean cho ba split khác nhau (train 4.780.721 / 4.780.968 / 4.780.731) nên P@10 dao động 0,201–0,209, còn hai lần Train lại 15/11 trên cùng Parquet cho đúng 0,2021; muốn bền phải chia theo hash(user, book); (3) thư viện ghim (pyspark 3.5.3, mongo 7.0.14, kafka 3.8.0, Hadoop 3.5.0) nhưng base image chưa ghim digest.

## Góp ý ngày 10/11 (đã kiểm tra bằng chạy thật, code cũ so với code mới)

**36. Chấm sao rồi huấn luyện lại, sao gợi ý của user không đổi?**
**Nói ngắn:** "Có hai nhịp: worker fold-in cập nhật vector user ngay sau rating stream, còn train lại ALS theo đợt để cập nhật item factors. Nếu list không đổi, kiểm tra `user_seen`, `recommendation_jobs`, `user_profiles_<version>` và `recs_live.model_version`; batch retrain không cần thiết cho fold-in."

Thí nghiệm 10/11 (user 16470/user 22, Harry Potter và các sách khác) là bằng chứng lịch sử cho batch retrain; không dùng như bằng chứng của worker fold-in hiện tại. Các lượt đánh giá kết quả mới cần ghi cả `model_version`, `source=als-foldin`, và report mới. Rating stream được archive để batch retrain; nút Train lại ở /setting chạy `scripts/retrain.py`, nhưng không phải prerequisite để worker cập nhật user vector.

**36b. Streaming có train lại mô hình không? Vì sao không train theo từng sự kiện?**
**Nói ngắn:** "Không train ALS theo từng sự kiện: streaming ghi rating/job, worker fold-in user vector khi đủ ít nhất 3 rating tích cực thuộc sách đã có factor. Item factors giữ nguyên; đây là inference, không phải incremental training. Batch retrain hiện chạy thủ công qua nút Train lại; lịch cron/Airflow mới là phương án triển khai, chưa cấu hình."

**37. Chấm 1 sao mà sách vẫn lên "Sách đang hot"?**
**Nói ngắn:** "Trước đây bảng xếp theo số lượt chấm nên đúng là vậy. Nay bảng chỉ tính sách có lượt 4–5 sao và xếp theo số lượt 4–5 sao, nên lượt 1–3 sao không đẩy sách lên."

Thử 10/11: 3 người chấm *Twilight* 1 sao. Cách xếp cũ (`n_ratings`): *Twilight* đứng #1 với 0 lượt thích, điểm TB 1,0. Cách xếp mới: *Twilight* không có trong bảng. Đây là khác biệt giữa **phổ biến** (nhiều người nhắc tới, kể cả chê) và **được thích**. Job streaming vẫn lưu đủ mọi lượt theo cửa sổ (`n_ratings`, `avg_rating`, `n_liked`); chỉ cách app chọn và xếp thay đổi, nên các phép kiểm chứng streaming không đổi. Huấn luyện lại ALS không làm bảng này đổi: sách đang hot là lớp tốc độ (chỉ streaming, 10 phút gần nhất), gợi ý cá nhân là lớp batch (ALS), hai thứ tách riêng.

**38. User chấm hết danh sách gợi ý thì app hiện gì?**
**Nói ngắn:** "App bù bằng batch/popular để đủ 10; khi có đủ rating tích cực đã biết, worker có thể thay top-30 live sau debounce."

Mỗi user có batch top-30 và fallback riêng; app lọc sách đã chấm, ưu tiên top-30 live nếu `recs_live` khớp active `model_version`, rồi bù từ batch/popular. Dưới ngưỡng fold-in, sách chưa có item factor, hoặc user mới, app vẫn fallback batch/popular.

**39. Dữ liệu huấn luyện chia theo thời gian thế nào? Sách đang hot tính từ lúc nào?**
**Nói ngắn:** "Goodbooks không có timestamp nên split rating tích cực ngẫu nhiên theo từng user; mỗi user còn ít nhất một rating trong train và rating stream luôn ở train. Split chỉ ổn định khi input Parquet không đổi. Sách hot tính theo event_time, cửa sổ 10 phút trượt 5 phút."

Huấn luyện: rating dưới ngưỡng (mặc định 4) không làm tín hiệu ALS; rating tích cực được hold out seeded per-user để mỗi user có train history; user có một positive rating ở lại train; rating `from_stream` luôn ở train. Tập test chỉ dùng một lần sau khi chọn tham số trên validation. Các số dòng cũ không đại diện cho cấu hình hiện tại. Sách hot dùng `event_time`, cửa sổ 10 phút trượt 5 phút; watermark có thể loại sự kiện trễ khỏi thống kê live, archive giữ cho batch.

**40. Dữ liệu streaming lấy ở đâu? Có trùng với dữ liệu đã huấn luyện không?**
**Nói ngắn:** "Goodbooks không có người dùng đang hoạt động nên producer mô phỏng rating. App chấm sao gửi sự kiện Kafka giống đường đi đó. Mỗi sự kiện có `simulated`; có thể loại các sự kiện mô phỏng khi batch retrain."

`scripts/make_fake_stream.py` tạo `data/fake/stream_ratings.csv`: 5.000 lượt chấm (seed 2026), 4.775 user, 2.869 sách; user chọn theo số rating đã có (người hoạt động chấm nhiều hơn), sách theo độ phổ biến, số sao theo tỷ lệ thật (giả / thật: 5★ 32,2% / 33,2%, 4★ 35,1% / 35,8%, 3★ 24,2% / 22,9%); đã kiểm 0 cặp trùng với 5.976.479 rating gốc, 0 cặp lặp. Chạy thử 10/11 đúng lệnh demo (1.000 sự kiện, 5% trễ): MongoDB đếm 978 = 1.000 − 22 trễ, 8 lỗi bị loại, 20 bản trùng bị bỏ, và **11/651** lượt 4–5 sao trúng top-10 (phát lại `ratings.csv` thì luôn là 0, vì sách đó đã bị loại khỏi gợi ý). Mô hình và mọi số đánh giá không đổi vì không train lại. Lúc đầu producer phát lại dòng có sẵn trong `ratings.csv` (vẫn là mặc định, dùng cho các bài thử kill -9 và ngắt Kafka): nội dung trùng y chang dữ liệu đã train, chỉ thử được đường ống, không mang thông tin mới. Nếu có trùng thì cũng không bị học hai lần: `clean.py` bỏ sự kiện trùng theo `event_id`, rồi mỗi cặp user–sách giữ đúng một rating (ưu tiên bản từ streaming). Mọi sự kiện mô phỏng mang `simulated=true`, `clean.py --exclude-simulated` loại chúng khỏi lần huấn luyện sau. Cách chuẩn hơn (chưa làm vì phải train lại và đổi mọi số đã báo cáo): tách 10% rating làm luồng "tương lai", train trên 90%.
