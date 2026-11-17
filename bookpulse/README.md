# BookPulse – hệ thống gợi ý sách (BDA501)

Toàn bộ hệ thống chạy bằng Docker trên một máy: Spark (batch + Structured Streaming), Kafka, MongoDB, worker fold-in và app Streamlit;
thành phần MapReduce chạy trên một cụm Hadoop riêng (HDFS + YARN), là nhánh kiểm chứng, xem mục 8. Hệ thống tổ chức
theo ý tưởng Lambda: lớp batch, lớp tốc độ, lớp phục vụ; app đọc `recs_live` trước, nếu không có thì dùng batch view,
đồng thời lọc sách trong `user_seen` và `user_ratings` sau khi trạng thái tới MongoDB. Sơ đồ tổng quan hiện tại [docs/img/architecture_overview.svg](docs/img/architecture_overview.svg)
(slide 4), chi tiết [docs/img/architecture_detail.png](docs/img/architecture_detail.png) (slide 5) và bản
tương tác [docs/kien-truc.html](docs/kien-truc.html).

```
MapReduce:        CSV → HDFS → Hadoop Streaming (mapper → combiner → reducer) → thống kê theo sách
(kiểm chứng)                                                              → đối chiếu với Spark SQL (app không dùng)
Luồng batch:      CSV → clean.py → Parquet → analytics_sql.py (Spark SQL)
                                          → train_als.py (ALS) → write_recs.py → MongoDB
Luồng streaming:  producer.py → Kafka (rating-events) → stream_stats.py → user_ratings + recommendation_jobs
                                                      → recommendation_worker.py (ALS fold-in) → recs_live
                                                      → rating_stats + user_seen + online_eval
                                                                     → Parquet (stream_events) → clean.py lần sau
Phục vụ:          MongoDB → app/app.py (Streamlit, http://localhost:8501; trang /setting điều khiển demo)
```

## 0. Yêu cầu

- Docker Desktop đang chạy, cấp **ít nhất 8 GB RAM** (Settings → Resources).
- **Ổ chứa dữ liệu Docker còn trống ít nhất 20 GB.** Nếu ổ C gần đầy, chuyển dữ liệu Docker sang ổ khác:
  Docker Desktop → Settings → Resources → Advanced → **Disk image location** → chọn thư mục trên ổ D → Apply & restart.

## 1. Khởi động

```bash
cd "D:\Big Data\Group_06_BigData\bookpulse"
docker compose up -d --build      # lần đầu mất vài phút để build image
docker compose ps                 # mongo, kafka, spark, app, recommendation-worker đều phải "running"
```

Mọi lệnh bên dưới chạy **bên trong container `spark`** bằng `docker compose exec spark ...`.

## 2. Chạy thử nhanh với dữ liệu mẫu (khoảng 5 phút)

```bash
docker compose exec spark python scripts/make_sample.py
docker compose exec spark python batch/clean.py --input data/sample/ratings.csv
docker compose exec spark python batch/train_als.py --implicit --ranks 10 --regs 0.1 --alphas 1 --eval-users 500
docker compose exec spark python batch/write_recs.py --books data/sample/books.csv
```

Mở http://localhost:8501 để vào trang demo kiểu nhà sách. Không cần đăng nhập/đăng ký:
nhập user ID hiện có trong sidebar để xem gợi ý của user đó, hoặc dùng mã khách tạo tự động;
có thể chuyển giữa gợi ý, khám phá và lịch sử sách đã xem. Mục **Khám phá** hiển thị 12 sách mỗi trang;
mục **Gợi ý cho bạn** có 100 kết quả, chia 8 sách mỗi trang; ưu tiên ALS/fold-in của user rồi bù sách chưa xem nếu cần.
Mỗi thẻ sách có bộ chọn sao không số để gửi rating vào Kafka; cần chấm 3 sách khác nhau từ 4★ trở lên
kể từ lần fold-in gần nhất để kích hoạt lần cập nhật ALS tiếp theo, còn 1–3★ chỉ ghi nhận sách đã xem. Gợi ý có thể lọc thể loại
hoặc sắp theo điểm ALS, thể loại, đánh giá độc giả và độ phổ biến. Trạng thái chờ và cập nhật được xử lý
trong riêng lưới sách. Trang **Điều khiển demo** vẫn dùng để bật streaming và phát lô sự kiện mô phỏng.
App chỉ dùng bìa trong danh mục,
không gọi Google Books API khi render; phân trang và chỉ vẽ tab đang mở để giảm thời gian tải.

### Cập nhật thể loại sách

`books.csv` dùng `book_id` nội bộ còn `book_tags.csv` liên kết bằng `goodreads_book_id`; script ghép qua mã Goodreads
để giữ nguyên ID hiện tại. Tải các bảng tag nếu chưa có và chạy pipeline (không đọc/ghi `ratings.csv`):

```bash
docker compose exec spark python scripts/download_goodbooks.py
docker compose exec spark python scripts/update_book_genres.py
```

Pipeline chuẩn hóa tag Goodreads thành tối đa 3 thể loại có bằng chứng mạnh nhất mỗi sách (tag cần ít nhất 5 lượt);
tag gốc được giữ tối đa 20 tag/sách để kiểm tra. Tag chung như `to-read`, `fiction` hoặc `favorites` không tự tạo
thể loại. Sách không có tag phù hợp được lưu là `["Unknown"]`. Script ghi `data/goodbooks/books_with_genres.csv`,
upsert chỉ các trường genre vào `books` trong MongoDB (không ghi đè title/authors hay trường hiện có), và tạo report
trong `reports/book_genres_*.json`. Chạy lại an toàn; duplicate `book_id` trong file nguồn được bỏ qua.

Tùy chọn bổ sung bằng Google Books chỉ cho sách chưa phân loại; có timeout/retry trong API helper, cache bền trên đĩa,
và giới hạn mặc định tối đa 500 lượt tra mới mỗi lần:

```bash
docker compose exec spark python scripts/update_book_genres.py --google-books --google-limit 500
```

Trong tab **Khám phá**, chọn một hoặc nhiều thể loại; lựa chọn được kết hợp với tìm theo tên/tác giả và các bộ lọc
hiện có. Genre hiển thị trên cả thẻ sách khám phá lẫn gợi ý; thuật toán ALS và luồng ratings không đổi.

Kết quả thực tế từ dữ liệu Goodbooks hiện có: 10.000 sách đã xử lý, 9.966 có thể loại chuẩn hóa, 34 `Unknown`,
0 sách không có tag, 0 duplicate ID bị bỏ và 9.478 có nhiều thể loại (tối đa 3). Số sách theo thể loại:

| Thể loại | Số sách | Thể loại | Số sách |
|---|---:|---|---:|
| Fantasy | 3.583 | Romance | 3.107 |
| Mystery | 2.220 | Thriller | 1.954 |
| Science Fiction | 1.881 | Horror | 743 |
| Historical Fiction | 1.739 | Contemporary Fiction | 1.569 |
| Young Adult | 2.615 | Adventure | 1.322 |
| Biography | 961 | History | 1.264 |
| Self-Help | 440 | Philosophy | 712 |
| Poetry | 272 | Non-Fiction | 1.837 |
| Children | 1.100 | | |

Đây là phân loại heuristic dựa trên tag cộng đồng và số lượt tag, không phải nhãn do nhà xuất bản xác nhận.
Một số tag Goodreads vẫn có thể mơ hồ; xem `original_tags` và `genre_source` trong MongoDB để kiểm tra.

Luồng streaming không cần gõ lệnh: trang **Điều khiển demo** (http://localhost:8501/setting) có nút bật/dừng job
streaming, gửi dữ liệu mô phỏng qua Kafka (có thể chèn sự kiện trễ), chạy `kafka_audit.py` / `nosql_evidence.py`,
train lại model batch và xóa dữ liệu streaming thử nghiệm trong khu vực bảo trì có xác nhận
(`scripts/retrain.py`: dừng job streaming → `clean.py` gộp kho sự kiện → `train_als.py --implicit` một bộ tham số →
`write_recs.py` chuyển phiên bản → bật lại job; đo 15/11: 164–179 giây trên máy 12 core, ước 6–8 phút trên 4 core;
report `reports/retrain_*.json` ghi top-10 của một user trước/sau và thời gian từng bước), kèm trạng thái và output của từng lệnh. Các nút gọi `scripts/control_server.py`, tiến trình chính của
container spark (cổng 8600, chỉ trong mạng compose), chỉ chạy đúng các lệnh có sẵn. Sau khi kéo bản này về, chạy
`docker compose up -d` một lần để tạo lại container spark.

Bằng dòng lệnh: mở **hai cửa sổ terminal**.

```bash
# Terminal 1: job Structured Streaming (Ctrl+C để dừng)
docker compose exec spark python streaming/stream_stats.py

# Terminal 2: bộ sinh sự kiện, 20 sự kiện/giây; --late 0.03 = 3% số sự kiện gửi sau --late-after-seconds
# (mặc định 30 giây, khi Spark đã có watermark) là sự kiện trễ 30 phút (watermark sẽ loại)
docker compose exec spark python streaming/producer.py --file data/sample/ratings.csv --limit 1000 --late 0.05

# Tùy chọn kiểm thử fold-in bằng CLI (trang Streamlit không có thao tác chấm sách)
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
```

`producer.py` phát lại rating **đã có trong dữ liệu huấn luyện**,
mà gợi ý đã loại mọi sách người dùng đã chấm, nên sự kiện phát lại không bao giờ làm đổi top-10 và không thể
"trúng" (chỉ số "Lượt 4–5 sao trúng top-10" hiện dạng trúng / tổng; với riêng producer, số trúng luôn là 0). Muốn thấy sách vừa chấm
biến mất khỏi top-10 và cuốn tiếp theo được đẩy lên, dùng `scripts/rate_from_recs.py` (đọc top-10 hiện tại trong MongoDB và gửi rating cho `--n` cuốn
đầu); sau khi query streaming ghi MongoDB (trigger 5 giây, có thể lâu hơn khi batch chậm), app lọc sách đã chấm từ
`user_seen`/`user_ratings`, đồng thời xếp job fold-in. Nếu streaming dừng hoặc sự kiện vượt watermark, sách chưa được
lọc khỏi danh sách cho tới khi trạng thái rating được xử lý. Worker chỉ tính ALS fold-in sau khi user có ít nhất
`FOLD_IN_MIN_RATINGS` rating sách khác nhau mới từ stream đạt `ALS_MIN_RATING` kể từ snapshot gần nhất
(mặc định lần lượt là 3 sách và 4 sao). Rating 1–3 vẫn được giữ là đã xem nhưng
không là tín hiệu thích. `event_time` do producer/app gán, `ingested_at` do stream job ghi.
Trong lúc job chạy, Spark UI → tab **Structured Streaming** (http://localhost:4040) hiện 3 query có tên;
`reports/stream_progress_*.json` ghi tiến độ từng query. Chi tiết thiết kế và bằng chứng: [docs/streaming.md](docs/streaming.md).

**Không chạy luồng batch song song với streaming.** Chỉ cổng 4040 của Spark UI được mở ra máy host (job thứ hai
sang 4041, không xem được), và hai driver 4 GB vượt 7,7 GiB RAM của Docker. Dừng job streaming (Ctrl+C) trước khi
chạy batch. Vì vậy trên máy này hai lớp chạy lần lượt: khi `run_pipeline.sh` chạy, sự kiện chờ trong
Kafka (giữ 7 ngày), app vẫn phục vụ gợi ý và speed view cũ; khi job streaming chạy lại, nó đọc bù tối đa 20.000
offset mỗi batch. Trên cluster: batch chạy trên cụm riêng, streaming chạy thường trực.

## 3. Chạy với Goodbooks-10k (dữ liệu thật)

Chạy cả luồng batch bằng một lệnh: `bash scripts/run_pipeline.sh`. Thời gian thực tế tùy cấu hình máy; xem report
`reports/pipeline_*.json`. Hoặc từng bước:

```bash
docker compose exec spark python scripts/download_goodbooks.py
docker compose exec spark python batch/clean.py
docker compose exec spark python batch/analytics_sql.py --books data/goodbooks/books.csv --raw-csv data/goodbooks/ratings.csv
docker compose exec spark python batch/train_als.py             # ALS explicit + baseline (RMSE, Precision@10)
docker compose exec spark python batch/train_als.py --implicit --ranks 20,50 --regs 0.1 --alphas 1,10
docker compose exec spark python batch/write_recs.py --books data/goodbooks/books.csv
docker compose exec spark python scripts/foldin_eval.py --eval-users 1000
docker compose exec spark python scripts/diagnose_foldin.py
docker compose exec spark python scripts/measure_latency.py --users 100
docker compose exec spark python streaming/producer.py --limit 2000
# hoặc phát dữ liệu giả "mới": các cặp user–sách KHÔNG có trong ratings.csv (không cần train lại, số liệu không đổi)
docker compose exec spark python scripts/make_fake_stream.py   # -> data/fake/stream_ratings.csv (5000 dòng, seed 2026)
docker compose exec spark python streaming/producer.py --file data/fake/stream_ratings.csv --limit 2000
# hoặc không cần lệnh: app → thanh bên trái "Giả lập sự kiện" → Gửi vào Kafka (tự tạo file dữ liệu giả nếu chưa có)
docker compose exec spark python scripts/nosql_evidence.py     # đọc lại MongoDB và kiểm chứng
docker compose exec spark python scripts/kafka_audit.py        # đếm lại Kafka và đối chiếu với số đếm streaming trong MongoDB
```

`foldin_eval.py` báo cáo replay offline chia theo 1, 2, 3, 5, 10 positive ratings, kiểm tra vector và latency tính toán.
Để chạy `diagnose_foldin.py` và đo end-to-end bằng `measure_latency.py`, cần khởi động
`streaming/stream_stats.py` (hoặc nút **Điều khiển demo**) trước đó. Diagnosis dùng hai user ID test mới: seed hồ sơ,
rate một sách đang trong top-10 và một sách latent-score thấp ngoài top-10, rồi kiểm tra rating → job → vector → live
→ danh sách app. Các sự kiện đều được đánh dấu mô phỏng; script xóa document test đã hoàn tất, giữ Kafka/Parquet và
không dọn user chưa hoàn tất nếu bị timeout. Latency script phát tối thiểu 100 rating mô phỏng cho user riêng biệt,
đo đến `recs_live` cùng các pha worker, ghi report rồi xóa riêng các document MongoDB của user đã hoàn tất.

**App dùng ALS implicit** (`models/als_implicit/versions/<model_version>`), chọn theo Precision@10 chứ không theo RMSE; lý do và số liệu ở
[docs/chon-mo-hinh.md](docs/chon-mo-hinh.md). `--implicit` thử mọi tổ hợp rank × regParam × alpha trên một phần
validation tách theo user từ tập rating >= `ALS_MIN_RATING` (mặc định 4), rồi fit lại trên toàn bộ tập train và đánh
giá trên test đã giữ lại ngẫu nhiên theo từng user. Rating thấp không dùng làm tín hiệu ALS nhưng luôn nằm trong tập
sách đã chấm để loại khỏi gợi ý. Mỗi lần train lưu `item_ids.npy`, `item_factors.npy` (float32), `YtY.npy` (float64),
`alpha`, `regParam`, rank, ngưỡng và `model_version` trong thư mục phiên bản riêng; `write_recs.py` chỉ chuyển
`meta.active` sau khi ghi xong. Artifact `.npz` từ các phiên bản cũ vẫn được hỗ trợ để đọc.
Muốn quay về mô hình explicit: `write_recs.py --model als`.

### ALS fold-in: suy luận vector user

Worker không huấn luyện tăng dần và không thay đổi vector sách. Với S là các rating hiện hành >= ngưỡng,
`common/foldin.py` giải:

```text
c1_i = alpha * rating_i
A = YtY + sum(c1_i * y_i * y_i.T) + regParam * |S| * I
b = sum((1 + c1_i) * y_i)
x_u = solve(A, b)
score = Y @ x_u
```

`YtY` được xuất một lần từ toàn bộ `itemFactors`; phép giải dùng NumPy `float64`, không dùng nghịch đảo.
Rating thấp không ảnh hưởng vector nhưng mọi sách đã chấm đều bị loại khỏi top-k. Sách không có trong itemFactors
bị bỏ qua và ghi log. Nếu có ít hơn `FOLD_IN_MIN_RATINGS` positive rating mới từ stream kể từ snapshot gần nhất
(mặc định 3), worker giữ
gợi ý batch thay vì giải vector ALS. Alpha, rank, regParam, threshold và model version được kiểm tra từ manifest;
`FOLD_IN_MIN_RATINGS` và `FOLD_IN_TOP_K` cấu hình qua biến môi trường (mặc định 3 và 100). Sidebar cho phép nhập
user ID, và trạng thái fold-in hiển thị thanh tiến độ theo pha cùng thời gian thực đã trôi qua.

Các phần trong module:

- `load_model(path)`: kiểm tra manifest, shape và `YtY`; tạo map book_id → hàng một lần và cảnh báo lệch model version.
- `fold_in(ratings, model)`: lọc positive rating, bỏ sách mới/không biết và giải vector; trả `None` khi chưa đủ tín hiệu.
- `recommend(user_ratings, seen_ids, model, k)`: tính `Y @ x`, loại mọi sách đã chấm và chọn top-k bằng `argpartition`.
- `batch/train_als.py`: xuất `item_ids`, `factors` float32, `YtY` float64 và metadata vào phiên bản model.
- `streaming/recommendation_worker.py`: gộp profile batch với rating stream mới nhất; claim job có token để worker đã mất lease không thể ghi kết quả; job được xếp lại nếu model đổi trong lúc xử lý.

Chạy kiểm thử fold-in và worker trong image Docker:

```bash
docker compose exec spark pytest -q
```

Sau khi đã train model implicit và có `data/parquet/ratings`, đo độ khớp trên 20 user ngẫu nhiên,
độ trễ cho một user/1.000 user, và Precision@10 trước/sau fold-in:

```bash
docker compose exec spark python scripts/foldin_eval.py --vector-check-users 20 --benchmark-users 1000
```

Báo cáo thực tế được in ra terminal và lưu thành `reports/foldin_eval_*.json`; nếu chưa chạy lệnh này trên model/dataset
cụ thể, không có số đo nào được giả định. Vector user đã lưu của Spark có thể không trùng chính xác phép giải lại trên
itemFactors cuối cùng vì ALS luân phiên cập nhật user/item; hãy đọc cosine similarity và sai số tuyệt đối đo được.
Fold-in không cập nhật vector sách, không tạo vector cho sách mới và không cá nhân hóa user hoàn toàn mới nếu chưa đủ
positive rating.

**Báo cáo lịch sử** của model `20261115_173217_451302` trên 1.000 user replay (20 user kiểm tra vector; không
đại diện model/worker đang chạy). Chạy lại các lệnh trên để tạo bằng chứng cho phiên bản hiện tại:

| Kiểm tra | Kết quả |
|---|---:|
| Cosine similarity trung bình với `model.userFactors` | 0.9999999999999998 |
| Sai số tuyệt đối lớn nhất giữa vector | 2.7462756402307775e-08 |
| Precision@10 batch → fold-in | 0.1167 → 0.0802 |
| Recall@10 batch → fold-in | 0.1611 → 0.1061 |
| Top-10 Jaccard batch/fold-in | 0.1643 |
| Một user: thời gian khuyến nghị | 0.03027 giây |
| 1.000 user tuần tự: trung bình / p95 | 0.01169 / 0.02873 giây mỗi user |
| 1.000 user tuần tự: tổng thời gian | 11.69683 giây |

Fold-in khớp gần như chính xác trên mẫu train, nhưng **Precision@10 và Recall@10 giảm** trong replay này;
fold-in không đảm bảo cải thiện chất lượng. Số đo chi tiết theo từng user nằm trong
[`reports/foldin_eval_20261115_182747.json`](reports/foldin_eval_20261115_182747.json).

Để so sánh ngưỡng 3 và 4, chạy cùng cấu hình hai lần rồi đối chiếu `reports/train_als_implicit_*.json`:
`docker compose exec -e ALS_MIN_RATING=3 spark python batch/train_als.py --implicit --ranks 50 --regs 0.1 --alphas 1`
và tương tự với `ALS_MIN_RATING=4`. `scripts/session9/ranking_eval.py` cũng đo threshold 3 so với model đang phục vụ
trên cùng test set. Không tái sử dụng Precision@10 0,204 từ quy trình train cũ như kết quả mới.

Với UCSD Book Graph: tải tay một file thể loại vào `data/ucsd/`, rồi
`python batch/clean.py --input data/ucsd/<file>.csv` (rating 0 = chưa chấm, tự động bị loại).

## 4. Kết quả đo

Mỗi bước ghi một file JSON vào `reports/`:

| File | Nội dung |
|---|---|
| `clean_*.json` | số dòng vào, số dòng lỗi, số dòng trùng, số dòng ra, thời gian |
| `analytics_sql_*.json` | các câu hỏi phân tích bằng Spark SQL (profile, phân bố rating, top sách), kiểm tra join giữ nguyên số dòng, SQL = DataFrame API (`exceptAll` hai chiều trên toàn bộ kết quả), thời gian CSV so với Parquet |
| `sql_plan_*.txt` | physical plan của truy vấn join: BroadcastHashJoin, một `Exchange` (shuffle) và một `BroadcastExchange` (gửi bảng sách nhỏ); rating đọc từ Parquet chỉ 2 cột, sách đọc từ `books.csv` |
| `nosql_evidence_*.json` | số document mỗi collection, document mẫu, query plan và độ trễ đọc theo `_id`, đối chiếu số đếm streaming; `recs_vs_training_ratings`: kiểm tra độc lập bằng pandas rằng không cặp (user, sách) nào trong gợi ý và danh sách bù đã có trong rating huấn luyện |
| `train_als_*.json` | RMSE (ALS so với 2 baseline), Precision@10 / Recall@10 và số đầu sách được gợi ý (ALS so với sách phổ biến), tham số tốt nhất, thời gian huấn luyện |
| `train_als_implicit_*.json` | ALS implicit: Precision@10 của từng tổ hợp tham số trên validation, kết quả trên tập test |
| `write_recs_*.json` | phiên bản mô hình và bản phục vụ, số người dùng đã ghi |
| `foldin_eval_*.json` | offline replay: Precision@10, Recall@10, Jaccard trước/sau fold-in, theo positive-history size 1/2/3/5/10; vector checks và latency local |
| `foldin_diagnostic_*.json` | hai trace mô phỏng qua Kafka/MongoDB: rating trong top-10 và rating ngoài top-10, vector delta, top-10 và kết quả app |
| `stream_latency_*.json` | độ trễ event mô phỏng → ingestion → `recs_live` cho tối thiểu 100 user riêng biệt, gồm pha fold-in, ranking và MongoDB |
| `pipeline_*.json` | thời gian toàn pipeline, có gồm `write_recs.py` |
| `stream_progress_*.json` | tiến độ ba query streaming (`valid`, `rejected`, `archive`): số batch, dòng vào, tốc độ, thời gian batch, watermark |
| `kafka_audit_*.json` | đếm lại Kafka; đối chiếu `stream_state`, `user_ratings`, `rating_stats`, archive; liệt kê offset-loss incidents đã ghi nhận |
| `streaming/*.png` | ảnh lịch sử Spark UI và app; ảnh app được chụp trước khi gỡ thao tác chấm sao khỏi giao diện |
| `streaming/fault_test_*/` | log của `scripts/stream_fault_test.sh` (thử `kill -9` job streaming) |
| `streaming/outage_test_*/` | log của `scripts/stream_outage_test.sh` (thử ngắt Kafka và MongoDB); `events.log` là dòng thời gian |
| `scaling/docker_stats_*.csv` | `docker stats` mỗi 5 giây khi chạy batch (RAM đỉnh) |

**Đo mức tăng tốc theo số core** (slide: "scaling with cores"):

```bash
docker compose exec -e SPARK_MASTER=local[1] spark python batch/train_als.py
docker compose exec -e SPARK_MASTER=local[2] spark python batch/train_als.py
docker compose exec -e SPARK_MASTER=local[4] spark python batch/train_als.py
```

Spark UI khi job đang chạy: http://localhost:4040

## 5. MongoDB có gì

| Collection | Ghi bởi | Nội dung |
|---|---|---|
| `recommendations_<phiên bản>` | write_recs.py | top-30 sách chưa đọc cho mỗi người dùng, kèm `fallback`: 10 sách phổ biến người đó chưa chấm |
| `meta` (`_id: "active"`) | write_recs.py | phiên bản đang dùng, phiên bản trước (`previous_collection`, để quay lại) + danh sách sách phổ biến (cold start) |
| `books` | write_recs.py | tên sách, tác giả (không có phiên bản; upsert theo `_id` trước khi chuyển `meta.active`) |
| `user_profiles_<phiên bản>` | write_recs.py | rating tích cực và toàn bộ sách đã chấm theo user, tạo từ batch history để worker fold-in không quên lịch sử |
| `user_ratings` | stream_stats.py | Rating stream mới nhất theo unique `(user_id, book_id)`, kèm `event_time` và `ingested_at` |
| `recommendation_jobs` | stream_stats.py + worker | Job fold-in theo user; pending được gộp/debounce, processing có timeout và retry |
| `recs_live` | recommendation_worker.py | `_id=user_id`, top-30, `source=als-foldin`, `model_version`, thời điểm và độ trễ |
| `rating_stats` | stream_stats.py | điểm trung bình và số cặp user–sách hiện hành **chỉ từ luồng streaming**; số liệu được tính lại từ `user_ratings`, nên chấm lại một sách không làm tăng số lượt cặp |
| `user_seen` | stream_stats.py | sách người dùng vừa chấm (app ẩn khỏi top-10) |
| `online_eval` | stream_stats.py | số lượt chấm 4–5 sao và số lượt trong đó cuốn sách nằm trong 10 gợi ý đầu **đã lưu** của phiên bản active (không trừ sách đã ẩn, không tính sách bù); cộng dồn qua các phiên bản, `model_version` là phiên bản mới nhất; app hiện "Lượt 4–5 sao trúng top-10" dạng trúng / tổng |
| `stream_state` | stream_stats.py | batch cuối đã ghi (chống đếm hai lần khi restart), số sự kiện hợp lệ / bị loại |

Xem nhanh: `docker compose exec mongo mongosh bookpulse --eval "db.meta.findOne()"`

## 6. Thiết kế đã chọn

So sánh bốn mô hình NoSQL, lựa chọn MongoDB và phác thảo cloud (đều ghi rõ phần nào chỉ là thiết kế) nằm trong [`docs/serving-deployment.md`](docs/serving-deployment.md).

- **Hai tốc độ cập nhật:** sự kiện chấm sao vào Kafka; streaming upsert latest `user_ratings`, giữ `user_seen` để ẩn sách
  ngay và gộp job; recommendation worker tính lại vector user trên item factors cố định rồi upsert `recs_live`. App đọc
  live trước, rồi batch. Worker kết hợp lịch sử trong `user_profiles_<version>` với rating stream mới nhất; cần ít nhất
  `FOLD_IN_MIN_RATINGS` (mặc định 3) rating stream tích cực (>= `ALS_MIN_RATING`, mặc định 4) cho các sách khác nhau
  trước khi giải vector fold-in.
  User chưa có tín hiệu phù hợp và sách mới vẫn dùng danh sách
  khởi đầu/batch/phổ biến. Tab **Dành cho bạn** hiển thị pending, processing, hoàn tất hoặc lỗi và tự làm mới khi thành công.
- **Top-100, app lọc sách đã xem:** người dùng không thấy lại cuốn vừa đọc mà không phải huấn luyện lại ALS.
  Khi đã đọc gần hết danh sách, app bù bằng `fallback` (sách phổ biến người đó chưa chấm) để đủ kết quả;
  cột "Nguồn" ghi rõ cuốn nào do ALS, cuốn nào là bù.
- **Fold-in không huấn luyện lại ALS:** chỉ giải lại vector user từ rating >= ngưỡng trên vector sách cố định. Train
  lại ALS theo đợt bằng nút (trên môi trường hiện tại; chưa cấu hình scheduler): `scripts/retrain.py` gom cả
  vòng (dừng job → `clean.py` → `train_als.py --implicit --ranks 50 --regs 0.1 --alphas 1` → `write_recs.py` → bật lại
  job) thành một lệnh, chạy từ nút "Train lại với rating mới" ở trang /setting hoặc bằng
  `docker compose exec spark python scripts/retrain.py --user 1`. Cần dừng job streaming vì hai driver 4 GB không vừa
  RAM. Production có thể dùng cron/Airflow gọi script theo lịch hoặc khi kho `stream_events` đạt ngưỡng sự kiện mới.
- **Rating từ streaming vào lần huấn luyện sau:** `stream_stats.py` lưu **mọi** sự kiện hợp lệ (kể cả trùng
  và đến trễ) ra `data/parquet/stream_events`. File sink dùng `_spark_metadata` + checkpoint để nhận diện file đã
  commit khi query chạy lại; đây không phải bảo đảm exactly-once đầu-cuối. `clean.py` bỏ trùng chính xác theo
  `event_id` rồi gộp với CSV; cùng một cặp (user, book) thì
  lấy rating mới hơn. Report `clean_*.json` ghi số dòng lưu trữ, số sự kiện sau bỏ trùng, số cặp cập nhật/mới.
  Vòng này đã chạy thật ngày 06/11 (`reports/clean_20261106_122043.json` gộp 4.901 dòng), nhưng `reset_streaming.sh`
  xóa kho `stream_events` trước mỗi lượt demo/thử, nên lần làm sạch của mô hình cuối (`clean_20261106_123904.json`)
  có `stream_events` = null: mô hình cuối (P@10 0,204) được huấn luyện không có sự kiện streaming. Kho giữ sự kiện cho
  lần huấn luyện sau cho đến khi chạy `reset_streaming.sh`; production: kho sự kiện là dữ liệu gốc bất biến, không reset.
  `producer.py` và `rate_from_recs.py` đánh dấu `simulated=true`. `clean.py` mặc định gộp cả sự
  kiện mô phỏng (để demo được vòng chấm sao → huấn luyện lại → gợi ý mới) và bỏ chúng khi chạy với `--exclude-simulated`;
  report `clean_*.json` đếm riêng `real_events` và `simulated_events`. Kết quả làm sạch có cột `from_stream`, và
  `train_als.py` luôn đưa rating đến từ streaming vào tập train (rating CSV mới chia ngẫu nhiên 80/20), nên mô hình phục vụ
  học được mọi lượt chấm mới; trước 10/11, chia ngẫu nhiên cả bảng làm khoảng 1/5 lượt chấm mới rơi vào tập test và không
  được học (thử 10/11: 1/11 lượt, `A Game of Thrones` của user 22). Kho là file sink trigger 5 giây, chia theo
  `event_date`, không compaction, nên sinh nhiều file nhỏ (production: trigger dài hơn cho query này, hoặc compaction /
  Delta Lake / Iceberg).
- **Lớp nhanh bỏ sự kiện trễ, lớp batch giữ lại:** các phép tính streaming có trạng thái dùng watermark 10 phút
  để giới hạn bộ nhớ trạng thái, nên bỏ sự kiện trễ hơn thế; kho lưu trữ thì giữ lại để không mất rating thật.
  Spark 3.5 lọc sự kiện trễ bằng watermark của batch **trước**, nên 1–2 batch đầu sau khi khởi động mới (checkpoint
  trống) chưa lọc; chạy lại từ checkpoint thì watermark được khôi phục. Vì vậy producer chỉ gửi sự kiện trễ sau 30 giây
  (`--late-after-seconds`). `user_seen` cũng được ghi sau watermark, nên một rating thật trễ hơn 10 phút không bị ẩn
  khỏi app cho tới lần batch sau (có thể sửa bằng cách ghi `user_seen`, vốn là upsert idempotent, từ một nhánh không
  trạng thái như kho lưu trữ).
- **Theo ý tưởng Lambda, chưa hợp nhất hai view:** `rating_stats` chỉ chứa rating từ streaming; thống kê theo sách
  của MapReduce / Spark SQL dùng để phân tích và đối chiếu, không được nạp vào MongoDB; app chỉ gộp lúc đọc. `rating_stats`
  được tính từ rating mới nhất cho mỗi cặp trong `user_ratings`, không phải tổng sự kiện; các sự kiện bị watermark loại
  vẫn có trong kho Parquet để lần batch sau được gộp. Không dùng Kappa vì log Kafka không phải nguồn gốc: 6 triệu rating lịch sử chưa từng đi qua Kafka, và
  Kafka chỉ giữ 7 ngày; muốn Kappa phải nạp lịch sử vào topic, giữ log lâu dài rồi huấn luyện lại bằng cách đọc lại log.
- **Đổi phiên bản gợi ý an toàn:** recommendations và user profiles ghi vào collection mới; chỉ chuyển `meta.active`
  sau khi cả hai collection đủ số user dự kiến. Khi kiểm tra thất bại, staging collections không được phục vụ và active cũ giữ
  nguyên. `books` upsert trước khi chuyển; các phiên bản collection cũ được giữ để không xóa dữ liệu tự động. `model_version`
  đến từ manifest train, `serving_version` là lần ghi gợi ý. Chưa có quality gate Precision@10; model artifacts có
  version trong `models/<name>/versions/`, còn Parquet `ratings` ghi đè tại chỗ.
- **Chống đếm hai lần:** Spark + Kafka + checkpoint là at-least-once. `online_eval` lưu
  `last_batch` ngay trên từng document; `rating_stats` được tính lại từ các cặp hiện hành; `user_seen` và `user_ratings`
  là upsert theo cặp. Các cập nhật dùng MongoDB atomicity theo document, khép khe cộng lặp giữa `bulk_write` và
  `mark_applied`. Batch mismatch với checkpoint vẫn dừng rõ ràng. Kho Parquet exactly-once nhờ file sink metadata.
  Thử nghiệm `kill -9` trước đó vẫn là kết quả đo, không phải bảo đảm về mọi lỗi. "0 mất" không được đảm bảo:
  `stream_stats.py` đặt
  `failOnDataLoss=false`, nên nếu job tắt lâu hơn thời gian giữ Kafka (mặc định 168 giờ) hoặc topic bị tạo lại,
  Spark có thể bỏ qua offset đã mất. `stream_stats.py` hiện so offset bắt đầu của micro-batch với earliest offset
  trên broker và ghi incident vào `stream_state`; `kafka_audit.py` báo cáo các incident đã phát hiện. Cơ chế này
  cảnh báo/ghi nhận, không khôi phục record đã hết retention; chưa có incident thật được đo trong lượt này.
- **Producer chịu được Kafka dừng ngắn:** trước khi sửa, lượt ngắt Kafka 40 giây làm producer mất 571 message, vì
  kafka-python 2.0.2 hủy bản ghi chờ trong buffer quá `request_timeout_ms` (mặc định 30 giây) và không có callback
  kiểm tra. Thay đổi quyết định là `request_timeout_ms=120000` (chờ tới 2 phút); thêm `retries=10`,
  `retry_backoff_ms=1000` cho request lỗi giữa chừng, và callback đếm bản ghi Kafka không xác nhận (`failed` trong dòng
  tiến độ). Kafka dừng lâu hơn khoảng 2 phút thì vẫn mất, nhưng được đếm. Retry có thể ghi một bản ghi hai lần; bỏ trùng
  theo `event_id` xử lý được. Không có idempotent producer, nên sau retry thứ tự trong partition không được bảo đảm (job
  dùng `event_time`). `acks=all` với replication 1 tương đương `acks=1`; chỉ có ý nghĩa khi replication 3,
  `min.insync.replicas=2`.
- **Ghi MongoDB chạy ở driver:** query `valid` gom mỗi micro-batch về driver rồi ghi bằng một
  client MongoDB (`rejected` chỉ đếm). Đủ cho
  tốc độ sự kiện của đồ án; `--max-offsets` (`maxOffsetsPerTrigger`, mặc định 20.000) giới hạn một batch sau khi
  job tắt lâu. Ở quy mô lớn cần chuyển việc ghi sang executor (`foreachPartition` hoặc MongoDB Spark connector,
  gộp theo sách trong Spark trước khi ghi). Mặc định nguồn Kafka đọc mỗi partition bằng một task (`minPartitions` chia
  nhỏ thêm được); phần có trạng thái chạy trên 8 shuffle partition. **Chưa thử tải**: ở 20 sự kiện/giây batch mất 3–6
  giây, phần lớn là chi phí cố định (3 query chia 4 core); 20.000 offset/batch chưa được hiệu chỉnh.
- **Đánh giá online:** mỗi lượt chấm 4–5 sao được đối chiếu với 10 gợi ý đầu đã lưu của phiên bản active (không trừ
  sách đã ẩn, không tính sách bù); bộ đếm cộng dồn qua các phiên bản. Với `producer.py`
  kết quả luôn là 0 trúng (sách phát lại đã bị loại khỏi gợi ý); chỉ lượt chấm sách đang được gợi ý qua CLI `rate_from_recs.py` tạo được lượt trúng.
- **Một máy:** Kafka 1 broker (replication 1), MongoDB standalone, Spark `local[*]`, Hadoop 1 DataNode (replication 1),
  Compose tự khởi động lại các service; query streaming thử kết nối lại từ checkpoint sau lỗi. Đây không phải HA: mỗi
  thành phần là một điểm lỗi đơn. Spark đọc ổ đĩa local, không đọc HDFS: `local[*]` là một JVM trên một máy, nên HDFS
  1 DataNode chỉ thêm độ trễ RPC. Lên cluster không chỉ đổi đường dẫn: logic xử lý (DataFrame, SQL, ALS) giữ nguyên,
  nhưng phải sửa lớp cấu hình (`common/settings.py` dùng pathlib, biến `hdfs://` thành `hdfs:/`, nên phải đổi sang
  chuỗi URI; đặt `SPARK_MASTER`; checkpoint lên HDFS/S3; gửi `common/` bằng `--py-files`; `spark.sql.shuffle.partitions`
  = 8 và driver 4 GB là giá trị cho laptop, đổi qua `SPARK_SHUFFLE_PARTITIONS`, `SPARK_DRIVER_MEMORY`; gói
  `spark-sql-kafka` đang tải từ Maven lúc chạy nên phải đóng sẵn vào image) và viết lại vài đoạn I/O (`has_parquet()`
  trong `clean.py` và chuyển phần ghi MongoDB của streaming từ driver sang executor). Thiết kế cluster nằm trong báo cáo.
- **Chưa có bảo mật:** cổng MongoDB (27017) và Kafka (9094) mở ra máy host, MongoDB không xác thực, Kafka PLAINTEXT;
  chỉ chấp nhận được trên laptop. Sự kiện lỗi chỉ được đếm, không lưu (chưa có dead-letter topic).
- **Chia train/test seeded per-user** vì CSV không có thời điểm chấm; user có một positive rating ở lại train, mọi
  rating stream ở train. Khi input đổi, random holdout của các cặp CSV có thể đổi; nếu dùng UCSD timestamps nên đánh giá
  thêm temporal split. Không dùng các metric lịch sử làm kết quả cho cấu hình hiện tại.
- **Đích cluster/cloud (thiết kế, chưa triển khai):** Spark batch trên YARN hoặc dịch vụ Spark quản lý; Parquet, archive
  và checkpoint trên HDFS/object storage; streaming chạy thường trực; Kafka ít nhất 3 broker với replication 3;
  MongoDB replica set; secrets/TLS và giám sát lag/độ trễ. Cần chuyển `pathlib` sang URI-compatible paths, đưa
  checkpoint lên storage dùng chung, đóng gói connector Kafka, cấu hình executor và chuyển ghi streaming từ driver
  sang executor trước khi chạy phân tán.
- **MongoDB hay Cassandra:** workload hiện tại chủ yếu đọc top-30 và fallback theo một `user_id`, thay toàn bộ kết quả
  theo từng user; MongoDB document `_id=user_id` khớp trực tiếp và thuận tiện khi gắn metadata. Cassandra cũng đáp ứng
  point read/write nếu partition key là `user_id`, nhưng cần schema/denormalization theo query và quản lý phiên bản
  recommendation rõ ràng hơn. Không có lựa chọn CAP tuyệt đối: replica set MongoDB với majority ưu tiên consistency
  và có thể từ chối ghi khi mất majority; Cassandra cho phép chọn consistency level, cân bằng availability/consistency
  theo thao tác. So sánh này là thiết kế theo access pattern, chưa phải benchmark; cấu hình hiện tại là MongoDB
  standalone một node nên chưa kiểm chứng CAP hay HA.

## 7. Dừng và làm lại từ đầu

Chỉ làm sạch phần streaming (trước khi demo): nút **Xoá dữ liệu** ở trang /setting, hoặc `bash scripts/reset_streaming.sh` (hỏi xác nhận, `--yes` để bỏ qua;
dừng job streaming và chờ nó dừng hẳn, xóa topic Kafka, checkpoint streaming, kho `stream_events` và các collection
streaming, giữ nguyên kết quả batch).

```bash
docker compose down                 # dừng, giữ dữ liệu MongoDB, topic Kafka (volume kafka-data) và checkpoint
docker compose down -v              # dừng và XÓA mọi volume: MongoDB, Kafka, checkpoint
bash scripts/reset_streaming.sh --yes   # bắt buộc chạy sau `down -v` (sau khi `up -d` lại)
```

Trạng thái streaming nằm ở ba nơi: checkpoint (volume `spark-checkpoints`), `stream_state` trong MongoDB và
`data/parquet/stream_events/_spark_metadata` (thư mục trên máy, không thuộc volume nào). Phải xóa cả ba cùng lúc.
Sau `down -v`, checkpoint mới đếm batch lại từ 0, nhưng `stream_events` vẫn còn `_spark_metadata` cũ, nên query
lưu trữ âm thầm bỏ qua các batch có số nhỏ hơn batch cũ (sự kiện mới không được lưu cho lần huấn luyện sau), và
`clean.py` lần sau vẫn gộp sự kiện cũ. `reset_streaming.sh` xóa đúng cả ba. Nếu checkpoint bị xóa mà `stream_state`
còn, job streaming nay dừng với lỗi (batch id nhỏ hơn `last_batch`) thay vì âm thầm bỏ qua các batch đó.

## 8. Thành phần MapReduce (Hadoop Streaming)

Job `mapreduce/` tính thống kê rating theo sách trên file `ratings.csv` gốc trong HDFS:

```
book_id  n_ratings  sum_rating  avg_rating  n_liked  n1  n2  n3  n4  n5
```

| File | Vai trò |
|---|---|
| `mapper.py` | Đọc từng dòng, kiểm tra hợp lệ, phát `book_id \t 1,rating,n1..n5`; dòng lỗi được đếm bằng Hadoop counter theo lý do (`WrongColumnCount`, `NotAnInteger`, `RatingOutOfRange`) |
| `combiner.py` | Cộng vector (count, sum, phân bố sao) ngay ở phía map trước khi shuffle. Chỉ cộng số nguyên, không tính trung bình, nên chạy 0 hay nhiều lần đều cho cùng kết quả |
| `reducer.py` | Cộng vector cuối cùng; tính `avg_rating = sum / count` và làm tròn **một lần** ở đây |
| `run_hadoop.sh` | Đưa CSV lên HDFS (file 72 MB, blocksize 32 MB → 3 block → 3 split), chạy job trên YARN với 2 reducer, lấy kết quả về `reports/mapreduce/` |
| `run_local.sh` | Mô phỏng bằng pipe `map → sort → combine → sort → reduce`, không cần Hadoop |
| `verify_with_spark.py` | Tính cùng thống kê bằng Spark SQL trên cùng file, so bằng `exceptAll` theo cả hai chiều; đọc counter của Hadoop; ghi `reports/mapreduce_verify_*.json` |
| `hadoop/` | Cụm Hadoop 3.5.0 của môn học (cùng image và cấu hình với lab Session 2) |

Chạy (Docker Desktop cần đủ RAM cho cả hai cụm; nếu thiếu, tạm dừng `kafka` và `app` của BookPulse):

```bash
export MSYS_NO_PATHCONV=1                     # Git Bash: không đổi /bookpulse/... thành C:/Program Files/Git/bookpulse/...
cd mapreduce/hadoop
docker compose up -d --build                  # lần đầu tải image Hadoop
docker compose logs client | grep "BOOKPULSE HADOOP READY"   # chờ đến khi có dòng này
docker compose exec client bash /bookpulse/mapreduce/run_hadoop.sh

cd ../..                                      # về bookpulse/
docker compose exec spark python mapreduce/verify_with_spark.py | grep -E 'rows_only|"match"|combiner_reduction'
```

Không có `MSYS_NO_PATHCONV=1`, lệnh `run_hadoop.sh` trong Git Bash báo lỗi "C:/Program Files/Git/bookpulse/...: No such file".

Giao diện web: HDFS http://localhost:9870 · YARN http://localhost:8088 · JobHistory http://localhost:19888

Số map task và thời gian thay đổi theo lần chạy: 05/11 có 4 map task (3 split + bản dự phòng do ApplicationMaster của
MapReduce chạy, speculative execution; log `reports/mapreduce/job_20261105_165718.log`: Launched map tasks=4, Killed
map tasks=2, Killed reduce tasks=1; mỗi split chỉ hoàn tất một lần) và 134 giây
(`reports/mapreduce_verify_20261105_170058.json`); lần tập 07/11 có 3 map task, job 52 giây; kết quả giống hệt nhau.
Mọi map task đều là rack-local, không có data-local, vì DataNode và NodeManager chạy ở hai container riêng
(cluster thật đặt chung một node).

Job MapReduce không khử trùng cặp (user, book) vì việc này cần thêm một job khác (key là cặp user–book).
Goodbooks-10k không có dòng trùng (xem `clean_*.json`), nên kết quả bằng đúng thống kê trên dữ liệu đã làm sạch.

## 9. Kiểm thử tự động

```bash
python -m unittest tests.test_mapreduce -v                                  # không cần Spark/Hadoop
docker compose exec spark python -m unittest discover -s tests -t . -v      # tất cả, gồm phần cần Spark
```

| File | Kiểm tra |
|---|---|
| `tests/test_mapreduce.py` | mapper loại đúng từng loại dòng lỗi; combiner chạy 0/1/nhiều lần cho cùng kết quả; điểm TB chỉ làm tròn một lần |
| `tests/test_recs.py` | không gợi ý sách đã chấm; chia test theo user; chỉ rating >= ngưỡng vào tập tín hiệu, rating stream vẫn ở train |
| `tests/test_foldin.py` | giải normal equation implicit ALS và xử lý rating sách ngoài item factors |
| `tests/test_recommendation_worker.py` | một rating tích cực đã biết đủ để fold-in; worker claim job theo thứ tự tạo |
| `tests/test_stream_progress.py` | gộp rating trùng user–sách theo event time, file tiến độ cộng batch đúng một lần, checkpoint lệch thì báo lỗi |

Chạy toàn bộ suite trong container `spark` để xác nhận kết quả trên cấu hình hiện tại.

Ngoài unit test, mỗi thành phần có kiểm chứng trên dữ liệu thật: `mapreduce/verify_with_spark.py` (MapReduce so với Spark SQL),
`scripts/kafka_audit.py` (đếm Kafka và so với số đếm streaming trong MongoDB, khối `mongo_check`),
`scripts/nosql_evidence.py` (đọc lại MongoDB; kiểm tra gợi ý không chứa sách đã chấm). Cổng kiểm tra của
`write_recs.py` được thử trên database riêng: `docker compose exec -e MONGO_DB=bookpulse_test spark python
batch/write_recs.py --books data/goodbooks/books.csv` (`reports/superseded/write_recs_gate_test_20261108/`).

Các phép thử fault dưới đây là số đo lưu từ lần chạy trước, không xác nhận cấu hình hiện tại; các phần kiểm tra
`rating_stats` cũ không nhất thiết khớp semantics hiện hành. Không chạy lại các script reset nếu không chủ định xóa
trạng thái streaming.

Thử sự cố streaming (khoảng 3 phút, chạy từ `bookpulse/`): `bash scripts/stream_fault_test.sh`. Script reset streaming,
chạy job + producer (1.500 sự kiện, 3% trễ sau 30 giây, seed 7), `kill -9` cả Python lẫn JVM sau 45 giây, 15 giây sau
chạy lại từ checkpoint, rồi chạy `kafka_audit.py`. Log ở `reports/streaming/fault_test_<thời điểm>/`. Lần chạy 08/11
(`reports/kafka_audit_20261107_220738.json`, tên file theo giờ UTC): 1.474 = 1.500 − 26 sự kiện trễ (26 sự kiện trễ
bị loại là suy từ phép trừ này; file progress chỉ ghi 0 + 23), 15 sự kiện lỗi,
tổng cửa sổ 2.948 = 2 × 1.474; cả ba phép so đều `true`. Lần chạy thứ hai, có thêm kiểm tra kho Parquet
(`reports/kafka_audit_20261108_040236.json`): 1.475 = 1.500 − 25, kho 1.537 dòng = 1.537 message hợp lệ, 1.500
`event_id` khác nhau, 0 file chưa commit, `exactly_once` = `true`.

Thử ngắt Kafka và MongoDB (khoảng 5 phút, chạy từ `bookpulse/`): `bash scripts/stream_outage_test.sh`. Script reset
streaming, chạy job + producer (3.000 sự kiện, 20 sự kiện/giây), sau 30 giây dừng Kafka 40 giây rồi bật lại, sau đó
dừng MongoDB 30 giây rồi bật lại; sau mỗi lần ngắt, nếu job đã dừng thì chạy lại từ checkpoint; cuối cùng chạy
`kafka_audit.py` (Kafka, MongoDB và kho Parquet). Log ở `reports/streaming/outage_test_<thời điểm>/`, `events.log`
là dòng thời gian. Lần chạy 08/11 (`reports/kafka_audit_20261108_041203.json`): Kafka dừng thì job vẫn chạy và tự
kết nối lại, producer `failed` = 0; MongoDB dừng thì job dừng (`ServerSelectionTimeoutError`) và script thử nghiệm
chạy lại job từ cùng checkpoint sau 11 giây (chưa có restart policy, ngoài thử nghiệm phải chạy lại bằng tay); kết quả 3.078 message, 3.000 = 3.000 sự kiện hợp lệ, tổng cửa sổ 6.000, 19 sự kiện lỗi, kho 3.059 dòng
= 3.059 message hợp lệ; 0 mất, 0 trùng.
