# Chọn mô hình gợi ý: vì sao BookPulse dùng ALS implicit

## Kết luận

App BookPulse dùng **ALS implicit** (artifact có version tại `models/als_implicit/versions/<model_version>`, rank/regParam/alpha trong manifest) thay cho **ALS explicit** dùng trước đây.

> **Cập nhật quan trọng:** Các số liệu 0,204 / 0,008 / 0,078 và các ví dụ bên dưới là kết quả lịch sử của quy trình cũ. Sau thay đổi, chỉ rating >= 4 là tín hiệu tích cực mặc định; test giữ lại ngẫu nhiên theo từng user; sách rating thấp vẫn bị loại khỏi gợi ý. Không dùng các số lịch sử làm kết quả của cấu hình mới. Chạy `scripts/run_pipeline.sh` và đọc `reports/train_als_*.json`, `reports/train_als_implicit_*.json`, `reports/foldin_eval_*.json` để có số đo mới.

Trong benchmark lịch sử, trên cùng tập test và cùng 4.993 người dùng, Precision@10 tăng từ 0,008 lên 0,204. Đây không phải kết quả của pipeline mới; mô hình vẫn được chọn theo Precision@10 thay vì RMSE vì người dùng chỉ thấy danh sách 10 cuốn.

Số liệu: lần chạy cuối 06/11/2026 bằng `scripts/run_pipeline.sh` từ trạng thái sạch, `reports/train_als_20261106_124521.json` (explicit) và `reports/train_als_implicit_20261106_125816.json` (implicit). Báo cáo cuối kỳ dùng cùng các số này. (Lần chạy 05/11 cho kết quả gần như trùng và cùng kết luận.)

> **Mô hình app đang dùng được fit trên toàn bộ tập train (80% dữ liệu).** Mô hình phục vụ chính là mô hình đã đánh giá: lưới tham số chỉ dùng 80% của tập train (phần còn lại làm validation), sau đó mô hình được fit lại trên toàn bộ tập train (4.780.968 rating); khoảng 1,2 triệu rating của tập test không được học. Khi triển khai thật, sau khi đánh giá xong nên fit lại trên 100% dữ liệu với tham số đã chọn (rank 50, alpha 1, regParam 0,1) rồi mới ghi gợi ý. Nhóm chưa làm bước này.

## 1. Vấn đề của mô hình cũ

ALS explicit học cách **đoán số sao** người dùng sẽ chấm, rồi gợi ý những cuốn có số sao dự đoán cao nhất. Về đoán số sao, nó làm tốt: RMSE 0,828, tốt hơn baseline tốt nhất (0,955).

Nhưng mô hình chấm điểm rất cao cho những cuốn **ít người chấm mà ai chấm cũng cho 5 sao**, như các tuyển tập truyện tranh Calvin and Hobbes hay tập thơ The Divan. Hệ quả:

- **Gợi ý gần như giống nhau cho mọi người.** Top-10 của user 1 và user 2 trùng phần lớn: Calvin and Hobbes, The Divan, The Hate U Give, The Revenge of the Baby-Sat, Humans of New York… (ví dụ từ các lần chạy explicit 04–05/11).
- **Gần như không trúng sở thích thật.** Precision@10 chỉ 0,008: trung bình 10 gợi ý trúng 0,08 cuốn. Mô hình còn thua cả cách gợi ý sách phổ biến cho tất cả mọi người (0,078).

![Top-10 của user 1 với ALS explicit (mô hình 04/11): 9/10 cuốn là Calvin and Hobbes](img/app_explicit_user1.png)

Nói gọn: **RMSE tốt không có nghĩa là gợi ý tốt.** RMSE đo độ chính xác khi đoán số sao trên những cặp người–sách đã có rating, còn gợi ý là bài toán chọn đúng 10 cuốn trong 10.000 cuốn.

## 2. ALS explicit và ALS implicit khác nhau thế nào

Cùng thuật toán ALS (phân rã ma trận người dùng × sách) trong Spark MLlib, chỉ khác mục tiêu học:

| | ALS explicit (cũ) | ALS implicit (mới) |
|---|---|---|
| Câu hỏi mô hình học | "Người này sẽ chấm cuốn này mấy sao?" | "Người này có chọn đọc cuốn này không?" |
| Dữ liệu dùng để học | Chỉ những ô đã có rating | Rating >= ngưỡng (mặc định 4) là tín hiệu tích cực; ô trống và rating thấp không phải tín hiệu thích |
| Vai trò của số sao | Là đáp án cần đoán | Độ tin cậy của tín hiệu thích: `c = 1 + alpha × rating` |
| Điểm đầu ra | Số sao dự đoán (khoảng 1–5) | Mức phù hợp (thường 0–1), không phải số sao |
| Tối ưu cho | Sai số số sao (RMSE) | Thứ hạng, tức chọn đúng top-10 |

Implicit học các tương tác tích cực (mặc định rating 4–5); rating 1–3 không dùng để huấn luyện nhưng vẫn được giữ trong tập sách đã chấm để loại khỏi gợi ý.

Trong Spark: `ALS(implicitPrefs=True, alpha=..., rank=..., regParam=...)`.

Với implicit model, BookPulse dùng `nonnegative=False`: khi item factors cố định, user vector có nghiệm đóng
`(YtY + Y_u^T(C_u - I)Y_u + lambda |I_u| I)^-1 Y_u^T C_u p_u`. Giữ cùng cấu hình này giúp worker fold-in
khớp vector mà Spark ALS học được; explicit model giữ cấu hình không âm cũ.

## 3. So sánh công bằng: cách làm

- **Benchmark cũ (không còn đại diện):** 5.976.479 rating, randomSplit theo dòng. Pipeline mới lọc rating >= ngưỡng trước khi train và giữ lại ngẫu nhiên rating theo từng user; người dùng có một rating tích cực duy nhất vẫn được giữ trong train.
- **Cùng cách đánh giá**: 4.993 người dùng lấy ngẫu nhiên từ tập test. Một cuốn được tính là "liên quan" nếu người dùng chấm 4–5 sao trong tập test. Sách người dùng đã chấm trong tập train không bao giờ được gợi ý.
- **Chọn tham số không nhìn tập test**: mỗi mô hình chọn tham số trên một phần validation tách từ tập train.
  - Explicit chọn theo RMSE (TrainValidationSplit).
  - Implicit chọn theo Precision@10 (tách 80/20 từ tập train).
  - Tập test chỉ dùng **một lần** ở cuối, để tránh rò rỉ dữ liệu (leakage).
- **Kiểm tra thêm độ phủ** (số đầu sách khác nhau xuất hiện trong các top-10), để chắc rằng mô hình không chỉ lặp lại vài cuốn nổi tiếng.

## 4. Kết quả

### Benchmark lịch sử (không tái sử dụng)

| Mô hình | RMSE | Precision@10 | Recall@10 | Số đầu sách được gợi ý |
|---|---|---|---|---|
| Sách phổ biến (baseline) | – | 0,078 | 0,051 | 39 |
| ALS explicit (cũ) | **0,828** | 0,008 | 0,006 | 1.370 |
| **ALS implicit (mới)** | không áp dụng | **0,204** | **0,135** | 1.224 |

Baseline RMSE: trung bình theo sách 0,955, trung bình toàn cục 0,992.

Thời gian huấn luyện, tính cùng một cách cho cả hai (4 bộ tham số mỗi mô hình, `local[*]`):

| | Chọn 4 bộ tham số + fit cuối | Riêng lần fit cuối |
|---|---|---|
| ALS explicit | 223 giây (3,7 phút) | không đo riêng (TrainValidationSplit gộp cả hai bước) |
| ALS implicit | 672 giây (11,2 phút) | khoảng 138 giây (2,3 phút) = 672 − 534 giây của 4 lần thử |

Tính cả bước chọn tham số, implicit tốn khoảng 3 lần thời gian của explicit: mỗi bộ tham số phải tính Precision@10 trên validation, và lưới của implicit có rank 20/50 (explicit: rank 10/20).

Cách đọc:
- **Precision@10 = 0,204**: trung bình 10 gợi ý có khoảng 2 cuốn đúng là sách người dùng thích (chấm 4–5 sao). Mỗi người chỉ có khoảng 15 cuốn "liên quan" trong 10.000 cuốn, nên gợi ý ngẫu nhiên chỉ được khoảng 0,0015.
- **Recall@10 = 0,135**: tìm ra được khoảng 13,5% số sách người dùng thích chỉ trong 10 gợi ý.
- **1.224 đầu sách**: gần bằng mô hình explicit (1.370) và gấp khoảng 31 lần baseline (39). Mô hình implicit vẫn cá nhân hóa, không chỉ gợi ý sách phổ biến.

### Chọn tham số trên validation (implicit)

| rank | alpha | Precision@10 (validation) | Recall@10 (validation) | Thời gian |
|---|---|---|---|---|
| 20 | 1 | 0,119 | 0,098 | 110,5 giây |
| 20 | 10 | 0,094 | 0,078 | 91,4 giây |
| **50** | **1** | **0,141** | **0,116** | 159,5 giây |
| 50 | 10 | 0,103 | 0,085 | 173,0 giây |

regParam cố định 0,1. Rank lớn hơn (50 yếu tố ẩn) mô tả gu đọc chi tiết hơn. Alpha 10 kém hơn alpha 1, vì số sao chỉ từ 1 đến 5: nhân lên quá mạnh làm mô hình quá tin vào từng lượt chấm.

Precision@10 trên validation thấp hơn trên test (0,141 so với 0,204) vì lúc chọn tham số, mô hình chỉ được học trên 80% tập train và mỗi người có ít sách "liên quan" hơn trong phần validation. Khi chọn tham số, điều quan trọng là **thứ hạng giữa các bộ tham số**, không phải giá trị tuyệt đối.

## 5. Ví dụ trước và sau

Cột implicit lấy từ mô hình cuối `20261106_125827` (collection gợi ý đang active), theo thứ tự xếp hạng.

| | ALS explicit (cũ, 04–05/11) | ALS implicit (mới, 06/11) |
|---|---|---|
| User 1 | Calvin and Hobbes (7 cuốn khác nhau), The Hate U Give, The Revenge of the Baby-Sat, Humans of New York | The Guernsey Literary and Potato Peel Pie Society (0,84), The Great Gatsby (0,84), The Catcher in the Rye (0,79), The Diary of a Young Girl (0,75), Cutting for Stone (0,75), Animal Farm… |
| User 2 | The Divan, Calvin and Hobbes (5 cuốn), The Hate U Give, The Revenge of the Baby-Sat, Humans of New York, Just Mercy… | The Last Lecture, To Kill a Mockingbird, The Five People You Meet in Heaven, Animal Farm, The 7 Habits of Highly Effective People, The Alchemist, … Freakonomics |
| User 3 | Holy Bible, Preach My Gospel, The Divan, Book of Mormon… | Animal Farm, Fahrenheit 451, Romeo and Juliet, The Great Gatsby, Night, The Catcher in the Rye, The Grapes of Wrath, … Macbeth |

Với mô hình mới, mỗi người nhận một danh sách khác nhau, dù cả ba đều có Animal Farm: user 2 nghiêng về sách phi hư cấu và phát triển bản thân (The Last Lecture, 7 Habits, Freakonomics), user 3 nghiêng về tác phẩm kinh điển thường học ở trường. Đây không phải ba thể loại tách bạch.

![Top-10 của user 1 với ALS implicit (mô hình 20261106_125827)](img/app_implicit_user1.png)

## 6. Vì sao chọn ALS implicit

1. **Đúng mục tiêu của sản phẩm.** Người dùng chỉ thấy 10 cuốn, nên thước đo phải là chất lượng top-10 (Precision@10, Recall@10), không phải sai số số sao.
2. **Benchmark cũ tốt hơn trên đúng thước đo đó.** Precision@10 0,204 so với 0,008 (explicit) và 0,078 (sách phổ biến); không áp dụng cho pipeline hiện tại.
3. **Vẫn cá nhân hóa.** 1.224 đầu sách được gợi ý so với 39 của baseline; danh sách khác nhau theo từng người.
4. **Không đổi kiến trúc.** Vẫn là ALS trong Spark MLlib, cùng cách ghi vào MongoDB và cùng app. Chỉ đổi tham số mô hình và nhãn một cột hiển thị.
5. **Chọn tham số đúng quy trình.** Chọn trên validation, test chỉ dùng một lần, nên con số 0,204 không bị thổi phồng do leakage.

## 7. Đánh đổi và giới hạn

- **Không còn đoán số sao.** Điểm của mô hình implicit là mức phù hợp, không phải dự đoán số sao; RMSE không áp dụng cho mô hình này.
- **Nghiêng về sách nổi tiếng hơn.** Ví dụ Animal Farm có trong danh sách của cả user 1, 2 và 3, The Great Gatsby trong danh sách của user 1 và 3, và độ phủ thấp hơn explicit một chút (1.224 so với 1.370). Đây là đặc điểm thường gặp của mô hình implicit; có thể giảm bằng cách thêm hình phạt độ phổ biến khi xếp hạng (chưa làm).
- **Mới thử 4 bộ tham số** (rank 20 hoặc 50, alpha 1 hoặc 10). Có thể còn bộ tốt hơn, ví dụ rank lớn hơn hoặc regParam khác.
- **Mô hình phục vụ được fit trên toàn bộ tập train (80% dữ liệu)**; chưa fit lại trên 100% dữ liệu trước khi ghi gợi ý (xem phần Kết luận).
- **Đánh giá online trong app không phải chất lượng offline.** `online_eval` có tính vòng tròn nếu người dùng chọn sách từ chính top-10; chỉ dùng để xác nhận pipeline, không thay cho Precision@10.
- **Fold-in chỉ tính lại vector user** trên item factors đã cố định. Sách mới và user mới vẫn cần batch train; dưới ngưỡng rating tích cực thì app dùng batch list.

## 8. Chạy lại

```bash
docker compose exec spark python batch/train_als.py                     # explicit + baseline (RMSE, Precision@10)
docker compose exec spark python batch/train_als.py --implicit --ranks 20,50 --regs 0.1 --alphas 1,10
docker compose exec spark python batch/write_recs.py --books data/goodbooks/books.csv          # mặc định: als_implicit
docker compose exec spark python scripts/foldin_eval.py --eval-users 1000
docker compose restart app                                              # để Streamlit nạp lại code nếu vừa sửa app.py
```

Để so ngưỡng 3 và 4, chạy cùng lệnh train lần lượt với `docker compose exec -e ALS_MIN_RATING=3 ...` và
`docker compose exec -e ALS_MIN_RATING=4 ...`, rồi đối chiếu report. `foldin_eval.py` phát lại một nửa của test split
và đánh giá trên nửa còn lại; đây là mô phỏng offline, không phải người dùng thật hay cam kết độ trễ production.

Quay về mô hình cũ: `docker compose exec spark python batch/write_recs.py --books data/goodbooks/books.csv --model als`.

## 9. Trả lời ngắn khi bị hỏi

> Ban đầu nhóm dùng ALS explicit, mô hình đoán số sao. RMSE tốt hơn baseline, nhưng khi đo đúng thứ người dùng thấy là top-10, Precision@10 chỉ 0,008, thua cả việc gợi ý sách phổ biến. Lý do là mô hình đẩy lên những cuốn ít người chấm nhưng toàn 5 sao, nên ai cũng nhận gần như cùng một danh sách Calvin and Hobbes. Nhóm chuyển sang ALS implicit, học xem người dùng có chọn đọc cuốn nào, và chọn tham số theo Precision@10 trên tập validation. Trên tập test, Precision@10 lên 0,204, gấp 2,6 lần baseline và khoảng 26 lần explicit, mà vẫn gợi ý hơn 1.200 đầu sách khác nhau. Đổi lại, mô hình không còn đoán số sao và hơi nghiêng về sách nổi tiếng.
