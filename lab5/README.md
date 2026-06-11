# Lab 5 - Hệ thống đếm số lượng người hiện diện trong camera sử dụng Apache Kafka
Sinh viên thực hiện: 23521104 - Vũ Đình Nhật
## Giới thiệu
Bài lab xây dựng một hệ thống đếm số lượng người trong khung hình theo kiến trúc nhiều thành phần, trong đó:
- **Camera Server** nhận dữ liệu từ camera hoặc video đầu vào.
- **Processing Server** thực hiện nhận diện người bằng mô hình YOLOv8.
- **Storage Server** lưu kết quả nhận diện và cung cấp API truy vấn.
- **Kafka** đóng vai trò message broker trong kiến trúc streaming, thể hiện ngữ cảnh xử lý dữ liệu lớn.

Kiến trúc tổng quát:

```text
[Camera / Video]
      |
      v
Camera Server -> Kafka topic: raw-frames -> Processing Server -> Kafka topic: detection-results -> Storage Server/API
```

---

## 1. Triển khai server trên Google Colab

Phần triển khai trên Colab được dùng để chạy pipeline end-to-end trong môi trường có GPU, phù hợp cho việc demo nhanh và kiểm tra khả năng xử lý của hệ thống.

### Thành phần liên quan
- Notebook chính: `notebooks/full_pipeline_colab.ipynb`
- Source code sử dụng trong notebook:
  - `camera_server/`
  - `processing_server/`
  - `storage_server/`
  - `utils/`

### Mô tả sơ bộ
Trong notebook Colab, hệ thống thực hiện các bước chính:
1. Cài đặt thư viện cần thiết.
2. Khởi động Kafka.
3. Nạp source code từ repository.
4. Nhận video đầu vào từ thư mục `videos/`.
5. Chạy các server trong cùng runtime.
6. Lưu kết quả nhận diện vào `results/sample_output.json` và `results/annotated_frame.jpg`.
---

## 2. Triển khai ở local

Phần triển khai local được dùng để chạy hệ thống trên máy cá nhân, phục vụ việc kiểm thử từng thành phần và demo trực tiếp với webcam hoặc video.

### Thành phần liên quan
- Source code chính:
  - `camera_server/producer.py`
  - `processing_server/consumer.py`
  - `processing_server/detector.py`
  - `storage_server/consumer.py`
  - `storage_server/api.py`
- File cấu hình/chạy local:
  - `docker-compose.yml`
- Script xem trực tiếp camera/video có bounding box:
  - `live_viewer.py`

### Mô tả sơ bộ
Khi chạy local, hệ thống được chia thành các phần:
1. **Docker** dùng để khởi động Kafka và MongoDB.
2. **Processing Server** nhận frame từ Kafka và chạy YOLOv8 để phát hiện người.
3. **Storage Server** lưu kết quả và cung cấp API tại `http://localhost:8000/docs`.
4. **Live Viewer** dùng để mở webcam hoặc video và hiển thị bounding box trực tiếp trên màn hình.

### Kết quả minh họa
- Hình 4: Docker containers của Kafka và MongoDB khi chạy local.
- Hình 5: Terminal chạy Processing Server và Storage Server.
- Hình 6: Cửa sổ preview webcam/video với bounding box và số lượng người.
- Hình 7: API docs hoặc kết quả trả về từ endpoint `/latest` và `/stats`.

![Minh họa kết quả hệ thống đếm người](https://private-user-images.githubusercontent.com/146966505/606442597-a3410e96-39de-4785-93df-265a7980aa50.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3ODExODUyNjksIm5iZiI6MTc4MTE4NDk2OSwicGF0aCI6Ii8xNDY5NjY1MDUvNjA2NDQyNTk3LWEzNDEwZTk2LTM5ZGUtNDc4NS05M2RmLTI2NWE3OTgwYWE1MC5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwNjExJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDYxMVQxMzM2MDlaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT0wM2M2NThjNWZlZGZkY2Y5NThhMGUxZDNlOGY0MzNmOWQ4YTk5ZGYxYWJjZjUwMzE5NTkwMzJjZDIzZTQ2YWViJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCZyZXNwb25zZS1jb250ZW50LXR5cGU9aW1hZ2UlMkZwbmcifQ.jCQOQ3Mnfa3zmZVWD3dpua3CbpM_Fm7P-3aceYD8eO4)

![Log back-end khi chạy hệ thống](https://private-user-images.githubusercontent.com/146966505/606442852-72927324-2cae-4b88-9c1a-9ac00d6799b6.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3ODExODUyNjksIm5iZiI6MTc4MTE4NDk2OSwicGF0aCI6Ii8xNDY5NjY1MDUvNjA2NDQyODUyLTcyOTI3MzI0LTJjYWUtNGI4OC05YzFhLTlhYzAwZDY3OTliNi5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwNjExJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDYxMVQxMzM2MDlaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT0wNjBlYzExNzVjZmE3ODFiNGVlMzdlOTc3YTBlMjQ0YmRkNTVjZmE4M2ZhZWI4MGY4MzIzZGQ1N2M3YzlmODEyJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCZyZXNwb25zZS1jb250ZW50LXR5cGU9aW1hZ2UlMkZwbmcifQ.aMnfG4K-rY1iE9Cm6RywOQlRVPbo3yu-66UJrtaQO9k)
---

## Kết quả đầu ra
Các file kết quả mẫu của hệ thống nằm tại:
- `results/sample_output.json`
- `results/annotated_frame.jpg`


