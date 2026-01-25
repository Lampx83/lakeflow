
````markdown
# Project Overview

Hệ thống được thiết kế để chạy **toàn bộ bằng Docker**, đảm bảo tính nhất quán môi trường, dễ triển khai và dễ mở rộng. Người dùng **không cần cài đặt trực tiếp Python hay các thư viện phụ thuộc** trên máy host.

---

## Yêu cầu hệ thống

- Docker >= 20.x  
- Docker Compose >= 2.x  

Kiểm tra:
```bash
docker --version
docker compose version
````

---

## Cấu trúc triển khai (tổng quan)

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── env.example
├── ...
```

---

## Thiết lập môi trường

### 1. Tạo file biến môi trường

Sao chép file mẫu:

```bash
cp env.example .env.local
```

Chỉnh sửa `.env` theo cấu hình mong muốn (port, database, API key, … nếu có).

---

## Chạy hệ thống bằng Docker

### 2. Build và khởi động toàn bộ hệ thống

```bash
docker compose up --build
```

Hoặc chạy ở chế độ nền:

```bash
docker compose up -d --build
```

Docker sẽ:

* Build image từ `Dockerfile`
* Cài đặt dependencies từ `requirements.txt`
* Khởi động toàn bộ service được định nghĩa trong `docker-compose.yml`

---

## Dừng hệ thống

```bash
docker compose down
```

Dừng và xóa toàn bộ container, network (không xóa image).

---

## Xem log hệ thống

```bash
docker compose logs -f
```

Hoặc theo từng service:

```bash
docker compose logs -f <service_name>
```

---

## Rebuild khi có thay đổi code

```bash
docker compose down
docker compose up --build
```

---

## Ghi chú

* Không chạy trực tiếp ứng dụng bằng `python main.py` trên host
* Mọi thao tác phát triển, test, deploy đều thực hiện **thông qua Docker**
* Khuyến nghị dùng Docker ngay cả trong môi trường development

---

## Triển khai (Deployment)

Hệ thống có thể triển khai trực tiếp trên:

* VPS
* Server vật lý
* Cloud (AWS / GCP / Azure)

Chỉ cần:

```bash
docker compose up -d --build
```