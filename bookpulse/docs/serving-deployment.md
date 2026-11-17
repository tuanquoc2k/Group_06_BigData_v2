# Thiết kế serving và triển khai

Tài liệu này ghi lựa chọn kiến trúc cho báo cáo. Cassandra và cloud bên dưới là **thiết kế**, chưa được triển khai hoặc benchmark trong project.

## Các mô hình NoSQL

| Mô hình | Hợp với | Áp dụng cho BookPulse |
|---|---|---|
| Key-value | Đọc/ghi theo một khóa, trạng thái nhỏ hoặc cache | Có thể cache top-10 theo `user_id`; chưa cần thêm Redis vì app đọc snapshot MongoDB trực tiếp. |
| Document | Đọc một đối tượng đã gom theo khóa, có trường lồng nhau và truy vấn phụ theo index | MongoDB lưu danh sách gợi ý theo user; app đọc theo `_id`, sau đó lọc sách đã rate. |
| Wide-column | Ghi lớn, phân vùng theo access pattern và truy vấn theo partition/clustering key | Cassandra là phương án nếu cần mở rộng ghi rating/event theo `user_id` hoặc thời gian; cần thiết kế partition để tránh hot partition. |
| Graph | Truy vấn nhiều bước qua quan hệ user–book–author–genre | Chưa phù hợp luồng chính: request đọc danh sách đã xếp hạng, không traversal quan hệ. |

## MongoDB và Cassandra theo workload

| Tiêu chí | MongoDB hiện tại | Cassandra (phương án thiết kế) |
|---|---|---|
| Access pattern | Ghi snapshot theo user; đọc theo `_id`; index phụ cho `user_seen`; cập nhật thống kê và job state | Tối ưu khi truy vấn biết partition key; có thể phân vùng ratings theo user. Mỗi kiểu truy vấn cần bảng thiết kế riêng. |
| Dạng dữ liệu | Document lồng nhau, index phụ, upsert và cập nhật pipeline phù hợp các collection hiện dùng | Wide-column, ghi phân tán tốt; không phù hợp join hoặc truy vấn tùy ý, không thay trực tiếp update pipeline hiện tại. |
| Nhất quán / CAP | CAP chỉ có ý nghĩa khi có phân vùng mạng trong triển khai phân tán. MongoDB standalone hiện tại dừng khi node/service lỗi; chưa đo khả năng chịu partition. Replica set có thể yêu cầu majority cho ghi. | Tùy consistency level và replication factor; có thể điều chỉnh theo thao tác. Không thể gán một lựa chọn CAP nếu chưa nêu cấu hình và quorum. |
| Nhận định | Giữ MongoDB vì đáp ứng snapshot theo user, index phụ và job state với ít thành phần vận hành. | Chỉ cân nhắc nếu tải ghi/phân tán vượt nhu cầu demo và đã xác định query/partition key; repo chưa có benchmark so sánh. |

Môi trường hiện tại là một máy với MongoDB standalone; các nhận định CAP trên là lý thuyết theo kiến trúc, không phải kết quả thử nghiệm HA. Kafka hiện cũng là một broker. Outage test kiểm tra phục hồi trong cấu hình local, không chứng minh độ sẵn sàng của cụm.

## Phác thảo triển khai cloud

Một hướng triển khai có thể dùng Kafka managed, Spark trên dịch vụ batch/container theo lịch, object storage tương thích S3 cho dữ liệu và checkpoint, MongoDB managed replica set cho serving, cùng app sau private network/load balancer. Batch ALS có thể chạy theo lịch rồi tắt compute; streaming worker chạy thường trực. Cần bổ sung IAM/secret manager, TLS, cảnh báo lag/health và backup/restore.

Đây là kiến trúc mục tiêu, chưa được triển khai, chưa đo chi phí, thông lượng hay độ trễ trên cloud; không dùng làm bằng chứng vận hành.
