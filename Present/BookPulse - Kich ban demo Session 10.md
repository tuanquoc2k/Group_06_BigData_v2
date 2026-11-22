# BookPulse – Kịch bản demo Session 10

Demo trực tiếp cho Final Assessment: MapReduce → Spark SQL → Spark ML/MongoDB → streaming và fold-in, mỗi phần có kết quả nhìn thấy được và phép kiểm chứng. Các thời gian MapReduce/streaming là số đo buổi tập, có thể thay đổi theo máy. Mọi lệnh chạy trong Git Bash, từ thư mục `bookpulse/` trừ khi ghi khác.

**Mỗi terminal Git Bash mở ra đều chạy trước:** `export MSYS_NO_PATHCONV=1`. Thiếu dòng này, Git Bash đổi đường dẫn trong container `/bookpulse/...` thành `C:/Program Files/Git/bookpulse/...` và lệnh MapReduce báo "No such file".

## 1. Chuẩn bị trước buổi (ít nhất 30 phút trước)

```bash
cd "D:\Big Data\Group_06_BigData\bookpulse"
export MSYS_NO_PATHCONV=1
docker compose up -d --build                          # spark, kafka, mongo, app, recommendation-worker
docker compose ps                                     # 5 service "running"
cd mapreduce/hadoop && docker compose up -d && cd ../..    # chỉ `stop`/`start` cụm Hadoop, KHÔNG `down`: down xoá container, lần up sau phải build lại image (nhiều dòng "pull access denied" rồi mới Building), tạo volume mới và HDFS trống cho tới khi run_hadoop.sh upload lại
docker compose -f mapreduce/hadoop/docker-compose.yml logs client | grep "BOOKPULSE HADOOP READY"   # phải in ra dòng này; chưa có thì chờ rồi chạy lại
docker compose -f mapreduce/hadoop/docker-compose.yml exec -T client hdfs dfsadmin -safemode wait   # chờ đến "Safe mode is OFF"
bash scripts/reset_streaming.sh --yes                 # streaming bắt đầu từ số 0
docker compose restart app                            # app nạp code mới nhất
```

Tùy chọn, làm trước buổi (khoảng 3 phút): `bash scripts/stream_fault_test.sh` để có bằng chứng `kill -9` mới (mục 5), rồi chạy lại `bash scripts/reset_streaming.sh --yes`.

Tùy chọn, **không** làm trong demo trực tiếp (khoảng 5 phút): `bash scripts/stream_outage_test.sh` tắt Kafka 40 giây rồi MongoDB 30 giây trong lúc producer gửi 3.000 sự kiện và job streaming đang chạy, rồi chạy `kafka_audit.py`. Script tự reset streaming lúc bắt đầu; chạy xong thì chạy lại `bash scripts/reset_streaming.sh --yes` để demo bắt đầu từ số 0. Kết quả ghi vào `reports/streaming/outage_test_<thời điểm>/` (`events.log` là dòng thời gian).

**Máy đã có bản cũ (`bookpulse`) chạy Docker:** trước mọi lệnh `docker compose` của bản này, chạy `export COMPOSE_PROJECT_NAME=bookpulse_v2` trong từng terminal. Mongo, Kafka và checkpoint của v2 nằm trong volume `bookpulse_v2_*` riêng, nên không đè dữ liệu bản cũ. Lần train 21/11 đã chạy theo cách này; dữ liệu và mô hình đã có sẵn.

Kiểm tra nhanh:
- Mô hình đang dùng: `20261121_100150_952595` (tạo bằng nút Train lại ngày 21/11, `reports/retrain_20261121_100422.json`; tham số chọn trên validation ở lần chạy pipeline `train_als_implicit_20261121_071137.json`). `db.meta.findOne({}, {popular: 0})` phải có `model_version` này và `foldin_available: true`. Chỉ chạy lại `bash scripts/run_pipeline.sh` (23,5 phút ngày 21/11) khi mất dữ liệu hoặc mô hình; chạy lại thì tên phiên bản đổi, còn danh sách gợi ý giữ nguyên (cùng dữ liệu, seed 42).
- http://localhost:8088: YARN có 1 NodeManager.
- Mở sẵn 3 terminal Git Bash ở thư mục `bookpulse/` (đã `export MSYS_NO_PATHCONV=1`) và các tab trình duyệt: app (8501), YARN (8088), HDFS (9870).
- Máy cần đủ RAM: tắt bớt ứng dụng khác. Nếu máy chậm, chạy MapReduce trước giờ demo và chỉ trình bày kết quả (mục 6).
- **Không chạy batch khi job streaming đang chạy.** Dừng job streaming trước (Ctrl+C): hai driver 4 GB không vừa 7,7 GiB RAM. Thời gian pipeline tùy máy, xem `reports/pipeline_*.json`; sự kiện chờ trong Kafka và app vẫn phục vụ bản cũ.

## 2. MapReduce (Hảo, ~2 phút)

Bắt đầu job **ngay đầu phần demo** vì job mất khoảng 1–2 phút (lần tập 07/11: job 52 giây, 72 giây tính cả khởi động; ngày 05/11: 134 giây); trong lúc chờ thì chuyển sang phần 3.

```bash
# Terminal 1
docker compose -f mapreduce/hadoop/docker-compose.yml exec client bash /bookpulse/mapreduce/run_hadoop.sh
```

Khi job xong:
- Chỉ trên màn hình: `3 block(s)`, `map 100% reduce 100%`, counter `Combine input records` 5.976.479 → `Combine output records` 47.087 (giảm 99,2%).
- Số map task có thể là 3 hoặc 4 (bản speculative), thời gian cũng khác nhau giữa các lần chạy; kết quả thì giống hệt.
- YARN UI (8088): application `BookPulse-book-rating-stats` ở trạng thái FINISHED.
- Kiểm chứng (khoảng 15 giây; lần tập 15/11: 14 giây):

```bash
docker compose exec spark python mapreduce/verify_with_spark.py | grep -E 'rows_only|"match"|combiner_reduction'
```

Nói: "`rows_only_in_mapreduce` và `rows_only_in_spark` đều bằng 0, `match: true`: MapReduce và Spark SQL cho cùng kết quả trên 10.000 sách; combiner giảm 99,2% dữ liệu shuffle."

## 3. Spark SQL (Hảo, ~1,5 phút, chạy song song với job MapReduce)

Không chạy lại (mất khoảng 2 phút); mở kết quả đã có:

```bash
# Tên file cố định: mọi report có cùng mtime sau khi checkout git, nên `ls -t | head -1` trả về file 04/11 (sai: không có join_check, plan Inner join)
grep -A4 -E '"join_check"|"sql_vs_dataframe"|"csv_vs_parquet_seconds"' reports/analytics_sql_20261121_062739.json
head -22 reports/sql_plan_20261121_062723.txt
```

Chỉ vào:
- `join_check`: `joined_rows` = `ratings_rows` = 5.976.479, `rows_preserved: true`.
- `sql_vs_dataframe`: `only_in_sql` 0, `only_in_dataframe` 0 trên 10.000 dòng.
- `csv_vs_parquet_seconds`: csv 4.31, parquet 1.05 (4,1 lần; qua các lần đo 3,9–8,4 lần, dao động theo bộ nhớ đệm).
- Trong plan: `BroadcastHashJoin LeftOuter`, **một `Exchange` shuffle cộng một `BroadcastExchange`** (gửi bảng sách nhỏ tới mọi task); `Scan parquet` có `Output [2]: [book_id, rating]`, tức chỉ đọc 2 cột; phía sách là `Scan csv` (đọc `books.csv`).

## 4. Spark ML + MongoDB (Kiệt, ~2 phút)

- App (http://localhost:8501, trang **Khám phá & gợi ý**): ở thanh bên trái nhập **Mã người đọc** 1, rồi 2, rồi 22; mục **Gợi ý cho bạn**. Dòng nguồn ghi "Gợi ý batch · model 20261121_100150_952595". Danh sách đo ngày 21/11 (reports/demo_trace_user1_20261121_101528.json):
  - User 1: The Great Gatsby, The Catcher in the Rye, Anna Karenina, Cutting for Stone, Animal Farm, The Diary of a Young Girl…
  - User 2: To Kill a Mockingbird, Animal Farm, Freakonomics, 1984, Lord of the Flies, The 7 Habits…
  - User 22: A Game of Thrones, The Shining, The Drawing of the Three, The Road, The Waste Lands, Dune, 11/22/63, Misery…

  Không nói là ba thể loại tách bạch: Animal Farm có trong cả danh sách user 1 và 2 (mô hình hơi nghiêng về sách nổi tiếng).
- Nói lý do chọn implicit, chỉ học rating từ 4 sao: Precision@10 0,248, NDCG@10 0,293, so với sách phổ biến 0,074 / 0,091 và ALS explicit 0,018 / 0,019 (slide 11). Explicit học trên mọi rating, đoán sao tốt hơn baseline (RMSE 0,754 so với 0,799) nhưng chỉ 670/5.000 user có ít nhất một cuốn trúng. Nếu bị hỏi: mô hình phục vụ fit trên tập train (80% rating thích của mỗi người).
- MongoDB (khoảng 3 giây):

```bash
docker compose exec mongo mongosh bookpulse --quiet --eval 'db.meta.findOne({}, {popular: 0})'
docker compose exec mongo mongosh bookpulse --quiet --eval '
  const c = db.meta.findOne().recs_collection;
  printjson(db[c].find({_id: 1}, {recs: {$slice: 3}, fallback: 1}).next());
  print(db[c].find({_id: 1}).explain("executionStats").queryPlanner.winningPlan.stage);
  printjson(db.recs_live.findOne({_id: 1}, {source: 1, model_version: 1,
      event_to_recommend_seconds: 1, ingestion_to_recommend_seconds: 1}))'
```

Nói: "Mỗi người dùng một document; truy vấn theo `_id` dùng IDHACK, đọc đúng một document (p50 0,72 ms, `nosql_evidence_20261121_071419.json`). Ba gợi ý đầu của user 1: sách 5 (0,755), 8 (0,691), 172 (0,639). Gợi ý batch và hồ sơ rating được ghi theo phiên bản mô hình; sau khi chấm sao, worker fold-in dùng lịch sử thích cùng rating mới và ghi `recs_live`, app chỉ đọc nó nếu cùng phiên bản đang dùng." Trước khi chấm sao, dòng `recs_live` in ra `null`.

*Nếu được hỏi về `previous_collection`: `meta` giữ bản đang dùng và bản dùng trước; `write_recs.py` chỉ chuyển active sau khi đủ documents cho recommendations và user profiles. Đọc collection/version hiện tại từ `meta`, không dùng tên phiên bản đã lưu trong ghi chú cũ.*

## 5. Streaming (Tuấn, ~3,5 phút)

Chỉ bắt đầu khi `verify_with_spark.py` ở mục 2 đã xong: không chạy job Spark batch cùng lúc với job streaming (Spark UI thứ hai sang cổng 4041, không được mở ra host; hai driver 4 GB vượt 7,7 GiB RAM của máy ảo Docker).

```bash
# Terminal 2: job streaming (để chạy)
docker compose exec spark python streaming/stream_stats.py
# Terminal 3: khi Terminal 2 in "streaming from …"; khoảng 55 giây
# Tạo dữ liệu giả một lần (khoảng 15 giây; có rồi thì bỏ qua): 5.000 cặp user–sách chưa có trong ratings.csv
docker compose exec spark python scripts/make_fake_stream.py
# --late 0.05: 5% số sự kiện gửi sau giây thứ 30 là sự kiện trễ (lần 21/11: 12/1.000)
docker compose exec spark python streaming/producer.py --file data/fake/stream_ratings.csv --limit 1000 --late 0.05
```

Cách không dùng dòng lệnh: trang **Điều khiển demo** (/setting), mục **2 · Gửi dữ liệu mô phỏng** → chọn số sự kiện, tốc độ, tỷ lệ trễ → **Gửi sự kiện vào Kafka** (lần đầu bấm **Chuẩn bị dữ liệu mẫu**). Job streaming cũng bật/tắt được ở mục **1 · Streaming** của trang này.

Nói: "Bộ Goodbooks không có người dùng đang hoạt động, nên nhóm sinh rating giả cho các cặp user–sách **chưa có** trong dữ liệu, chọn theo độ phổ biến và tỷ lệ số sao thật. Producer gửi chúng như lượt chấm vừa xảy ra, kèm cố ý 2% bản trùng, 1% sự kiện lỗi và 5% sự kiện trễ để thấy hệ thống loại."

Số trùng/lỗi/trễ thay đổi theo lần chạy (21/11: `sent 1000, duplicates 23, invalid 11, late 12`): đọc các giá trị producer in ra. Mỗi người dùng trong dữ liệu giả có rating từ 4 sao cũng được worker tính lại gu (21/11: 659 người, 0 job lỗi).

Trong lúc producer chạy:
- Mở http://localhost:4040 → tab **Structured Streaming**: 3 query có tên (`valid`, `rejected`, `archive`); bấm vào `valid` để thấy tốc độ vào khoảng 20 sự kiện/giây, batch đầu khoảng 6 giây (JVM lạnh), các batch sau 1–2 giây, watermark (ảnh 21/11: `reports/streaming/spark_ui_valid_20261121.png`).
- App: thanh bên trái báo "Streaming đang hoạt động". Bản này không còn mục "Sách đang hot" (đã bỏ cửa sổ trượt để tập trung vào gợi ý tức thì); nếu bị hỏi về window thì nói thẳng điều đó.

**Chấm sao → gu đổi ngay (ALS fold-in).** Khi producer báo `done`, nhập **Mã người đọc 1**, mục **Gợi ý cho bạn**: cuốn #1 là *The Great Gatsby*, dòng nguồn ghi "Gợi ý batch". Bấm **5 sao** ngay dưới bìa cuốn #1. App chỉ gửi sự kiện vào Kafka, rồi tự theo dõi các pha: gửi Kafka → chờ job → worker xử lý → có kết quả (nếu trang không tự đổi thì bấm **Làm mới**).

Sau khoảng 5–9 giây (đo 21/11: 5,0, 8,0 và 9,1 giây; trong đó worker chỉ mất khoảng 1 giây, phần còn lại là chờ trigger 5 giây), chỉ vào:
- Thông báo "Gợi ý fold-in mới nhất đã sẵn sàng." và dòng nguồn "ALS fold-in · model 20261121_100150_952595".
- Danh sách đổi, không chỉ dồn lên: trước là Gatsby, Catcher, Anna Karenina, Cutting for Stone, Animal Farm, Diary of a Young Girl, Wuthering Heights, A Tale of Two Cities, The Joy Luck Club, The Goldfinch; sau là **Anna Karenina**, Catcher, Cutting for Stone, ***The Old Man and the Sea*** (mới), Animal Farm, A Tale of Two Cities, ***Guernsey*** (mới), Wuthering Heights, The Goldfinch, Diary. The Joy Luck Club rơi khỏi top 10 (`reports/demo_trace_user1_20261121_101528.json`).

Nói: "App chỉ gửi sự kiện vào Kafka. Streaming ghi rating vào `user_ratings`, đánh dấu sách đã xem và tạo một job; worker lấy job, tính lại vector gu của user 1 từ lịch sử thích cộng rating mới, trên vector sách cố định của lần train gần nhất, rồi ghi danh sách mới vào `recs_live`. Đây là suy luận, không phải train lại: phép tính mất khoảng 4 mili giây, và vector khớp đúng vector mà Spark ALS tính khi train (sai số cỡ 10⁻⁸)."

Nếu được hỏi "nhiều người chấm cùng lúc thì sao": 100 người chấm cùng lúc, trung vị từ lúc gửi tới khi có gợi ý mới là 8,6–21,8 giây qua hai lần đo (`reports/stream_latency_20261121_073152.json`, `_073528.json`), vì một worker xử lý lần lượt và lần sau job vừa khởi động; production sẽ chạy nhiều worker chia theo user_id (slide 19).

Dự phòng nếu trình duyệt có vấn đề: cùng việc đó bằng dòng lệnh (cuốn đầu danh sách), rồi xem `recs_live`:

```bash
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 1 --rating 5
docker compose exec mongo mongosh bookpulse --quiet --eval 'printjson(db.recs_live.findOne({_id: 1}, {source: 1, model_version: 1, event_to_recommend_seconds: 1}))'
```

**Kiểm chứng** (chờ khoảng 15 giây sau lượt chấm), một lệnh (hoặc nút **Đối chiếu Kafka ↔ MongoDB** ở trang Điều khiển demo):

```bash
docker compose exec spark python scripts/kafka_audit.py | grep -E 'distinct_valid|invalid_messages|counted|expected|current_pairs|sum_unique|_match|no_loss|exactly_once'
```

Nói: "Script đếm lại toàn bộ topic Kafka rồi đọc MongoDB: `valid_events_counted` = `expected_valid_minus_late`, tức hợp lệ trừ trễ, không mất, không trùng; số cặp user–sách trong `user_ratings` bằng tổng trong `rating_stats`; số lỗi khớp; kho Parquet `exactly_once: true`. Các dòng `_match` / `no_loss…` đều `true`, và kết quả được lưu vào `reports/kafka_audit_*.json`." (Lần chạy 21/11: 1.113 hợp lệ, 12 trễ, 1.101 đếm được, 989 = 989 cặp, 11 lỗi, kho 1.136 = 1.136; con số gồm cả 100 user đo độ trễ chạy trước đó. Buổi thật sau reset: khoảng 1.001 hợp lệ.)

**Khôi phục sau sự cố.** Không làm `kill -9` trực tiếp trên sân khấu; trình bày bằng chứng của `bash scripts/stream_fault_test.sh` chạy trên bản này ngày 21/11: thư mục `reports/streaming/fault_test_20261121_142753/` (`kill.log`, `audit.log`) và `reports/kafka_audit_20261121_073115.json`: 1.474 = 1.500 − 26 trễ, `rating_stats` 1.474 cặp, 15 lỗi, kho Parquet 1.532 = 1.532 (exactly-once), các phép so `true`. Nói rõ: "Lượt thử này 0 mất, 0 trùng; đó là kết quả đo, không phải bảo đảm." Nếu được hỏi thêm: lượt tắt Kafka rồi MongoDB trên bản này (21/11, `bash scripts/stream_outage_test.sh`, `reports/streaming/outage_test_20261121_171048/events.log`, `reports/kafka_audit_20261121_101453.json`): Kafka tắt 44 giây, MongoDB tắt 31 giây trong lúc producer gửi 3.000 sự kiện; job streaming tự thử lại và vẫn chạy qua cả hai lần; 3.000 = 3.000 đếm được, 19 lỗi bị loại, kho Parquet 3.059 = 3.059 (exactly-once), 0 mất, 0 trùng. Nếu còn thời gian, khôi phục nhẹ: Ctrl+C ở Terminal 2, chờ dấu nhắc, chạy lại cùng lệnh (đọc tiếp từ checkpoint), gửi thêm `producer.py --limit 200`, rồi chạy lại lệnh kiểm chứng: vẫn `true`.

Kết thúc: Ctrl+C ở Terminal 2.

**Tuỳ chọn, nếu còn thời gian hoặc bị hỏi "khi nào vector sách học rating mới": nút Train lại.** Trang Điều khiển demo, mục **Train lại model batch** → bấm **Train lại** (user so sánh: 1). Script `scripts/retrain.py` tự dừng job streaming, chạy `clean.py` (gộp kho sự kiện, bỏ trùng), `train_als.py --implicit` một bộ tham số (có bước tự kiểm tra fold-in), `write_recs.py` (phiên bản mới, chuyển `meta.active`), rồi bật lại job. Nút fit một lần với tham số của mô hình đang phục vụ; đo 21/11: 390 giây (clean 53, train 196 trong đó fit 90, write_recs 140; `reports/retrain_20261121_100422.json`). Bấm rồi nói tiếp phần kiểm chứng, quay lại khi xong: app hiện phiên bản mô hình mới. Nói: "Fold-in đổi gu người dùng ngay; còn vector sách, và sách hay người dùng mới, chỉ học lại theo đợt, bằng nút này hoặc theo lịch."

## 6. Phương án dự phòng

| Sự cố | Cách xử lý |
|---|---|
| Lệnh MapReduce báo "C:/Program Files/Git/bookpulse/...: No such file" | Chưa `export MSYS_NO_PATHCONV=1` trong terminal đó; chạy rồi chạy lại lệnh |
| `grep "BOOKPULSE HADOOP READY"` không in gì | Cụm chưa sẵn sàng (khoảng 40 giây sau `up -d`); chờ rồi chạy lại |
| `run_hadoop.sh` báo "Name node is in safe mode" | NameNode vừa khởi động (gặp khi chạy thử 09/11 dù đã có dòng READY); chạy `hdfs dfsadmin -safemode wait` trong container client rồi chạy lại |
| Job MapReduce chậm hoặc lỗi | Trình bày kết quả đã chạy: `reports/mapreduce/job_*.log`, `reports/mapreduce_verify_*.json`, ảnh `reports/mapreduce/yarn_applications.png` |
| Spark UI không mở được | Ảnh `reports/streaming/spark_ui_valid_20261121.png`; số liệu trong `reports/stream_progress_20261121_*.json` |
| App không hiện thay đổi code | `docker compose restart app` |
| App báo không gửi được vào Kafka khi bấm sao | Kafka chưa chạy hoặc container app cũ chưa có biến `KAFKA_BOOTSTRAP`: `docker compose up -d kafka app`, rồi bấm lại; hoặc dùng `rate_from_recs.py` |
| Sau khi chấm sao app vẫn ghi "Gợi ý batch" | Job streaming và service `recommendation-worker` phải đang chạy (`docker compose ps`; log: `docker compose logs --tail 30 recommendation-worker`); chờ thêm một trigger rồi bấm Làm mới. Nếu vẫn không: trình bày danh sách trước/sau trong `reports/demo_trace_user1_20261121_073722.json` |
| Thanh bên trái báo "Streaming đang tạm dừng" | Bật job ở trang Điều khiển demo, mục 1 · Streaming, hoặc chạy lại lệnh ở Terminal 2 |
| Một phép so của `kafka_audit.py` ra `false` | Thường do sự kiện trễ đến khi job chưa có watermark (1–2 batch đầu sau khi khởi động mới, checkpoint trống). Nói thẳng điều đó, rồi trình bày `reports/kafka_audit_20261121_073115.json` (kill -9) và `_073750.json` (lượt demo 21/11), đều `true` |
| Job streaming báo lỗi topic | Job tự tạo topic; nếu vẫn lỗi: `bash scripts/reset_streaming.sh --yes` rồi chạy lại |
| Job streaming dừng với lỗi "checkpoint and stream_state do not match" | Checkpoint bị xóa hoặc chuyển chỗ mà MongoDB vẫn giữ `stream_state`; job cố ý dừng thay vì bỏ qua batch. Chạy `bash scripts/reset_streaming.sh --yes` rồi chạy lại |
| Máy hết RAM | Tắt cụm Hadoop sau phần MapReduce: `docker compose -f mapreduce/hadoop/docker-compose.yml stop` |
| Mọi thứ đều hỏng | Trình bày bằng slide + ảnh trong `reports/` và `docs/` (mọi số đo đều có file); bằng chứng `kill -9` ở `reports/streaming/fault_test_20261121_142753/` |

## 7. Sau buổi tập dượt

```bash
export MSYS_NO_PATHCONV=1
# 1. Dừng job streaming: Ctrl+C ở Terminal 2, chờ dấu nhắc
bash scripts/reset_streaming.sh --yes                          # 2. buổi thật bắt đầu lại từ số 0
docker compose -f mapreduce/hadoop/docker-compose.yml stop     # 3. dừng cụm Hadoop (giữ dữ liệu HDFS)
docker compose ps                                              # 4. stack chính vẫn "running"
git status --short reports/                                    # 5. tìm file report mới của lần tập
```

**Không bao giờ di chuyển hay xoá** `clean_*`, `train_als_*`, `train_als_implicit_*`, `write_recs_*`, `retrain_*` của lần chạy sinh ra mô hình đang phục vụ (report của lần train 10/11 đã mất vì dọn nhầm). Mỗi lần tập sinh thêm file trong `reports/` (`kafka_audit_*`, `stream_progress_*`, `mapreduce_verify_*`, `reports/mapreduce/*_<thời điểm>*`, có thể cả `nosql_evidence_*`). Chuyển chúng vào `reports/superseded/rehearsal_<ngày>/` để `reports/` chỉ giữ các lần chạy chính thức:

```bash
mkdir -p reports/superseded/rehearsal_<ngày>
mv reports/kafka_audit_<ts>.json reports/stream_progress_<ts>.json reports/mapreduce_verify_<ts>.json \
   reports/mapreduce/*_<ts>.* reports/superseded/rehearsal_<ngày>/
```

Tên file theo giờ UTC (chậm 7 giờ so với giờ Việt Nam). **Không chuyển** các file chính thức của bản này (lần chạy 21/11): `clean_20261121_062652.json`, `analytics_sql_20261121_062739.json`, `sql_plan_20261121_062723.txt`, `train_als_20261121_063254.json`, `train_als_implicit_20261121_071137.json` (mô hình đang phục vụ `20261121_100150_952595`), `write_recs_20261121_071405.json`, `nosql_evidence_20261121_071419.json`, `foldin_eval_20261121_072720.json`, `pipeline_20261121_064931.json`, `kafka_audit_20261121_073115.json` / `_073604.json` / `_073750.json` / `_101453.json`, `retrain_20261121_100422.json` cùng `clean_20261121_095846.json`, `train_als_implicit_20261121_100202.json`, `write_recs_20261121_100421.json` (mô hình đang phục vụ), `train_als_20261121_095750.json` (explicit), `nosql_evidence_20261121_100436.json`, `foldin_eval_20261121_100651.json`, `demo_trace_user1_20261121_101528.json`, thư mục `reports/streaming/outage_test_20261121_171048/`, `stream_latency_20261121_073152.json` / `_073528.json`, `foldin_diagnostic_20261121_073553.json`, `demo_trace_user1_20261121_073722.json`, `stream_progress_20261121_*.json`, thư mục `reports/streaming/fault_test_20261121_142753/` và ảnh `reports/streaming/spark_ui_valid_20261121.png`; cùng với `evidence/session9/E_model_validation.json`, `G1_implicit_semantics.json` (đo lại 21/11). Lần chạy đầu ngày 21/11 (trước khi sửa bước kiểm tra vector và phép chia train/test) nằm ở `reports/superseded/v2_first_model_20261121/`, có README giải thích. Các file cũ hơn vẫn là bằng chứng của bản trước: MapReduce 05/11 (`*_20261105_*`); không đọc số liệu mô hình 06/11 hay 14/11 như kết quả hiện tại.
