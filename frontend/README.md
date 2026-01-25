# EDUAI – Streamlit Frontend (Control & Test UI)

## 1. Mục đích

Đây là **Streamlit Control UI** phục vụ cho:

- Test và debug Backend API
- Vận hành pipeline dữ liệu (DEV / nội bộ)
- Kiểm tra semantic search, ingestion, embedding
- Duyệt Data Lake theo các zone (000 → 500)

⚠️ **Không phải frontend sản phẩm**  
⚠️ **Không dùng cho end-user hoặc production**

---

## 2. Kiến trúc tổng quan

```text
[ Streamlit UI ]  →  [ FastAPI Backend ]  →  [ Qdrant Vector DB ]
        8501                8011                   6333
````

Frontend **không xử lý nghiệp vụ**, chỉ đóng vai trò:

* Gọi API
* Hiển thị kết quả
* Trigger pipeline (DEV)

---

## 3. Yêu cầu hệ thống

### 3.1. Bắt buộc

* Docker
* Docker Compose
* Backend EDUAI đã cấu hình đầy đủ

### 3.2. Các service liên quan

Frontend **phụ thuộc** vào:

* `eduai-backend`
* `qdrant`

Do đó **không chạy frontend độc lập**.

---

## 4. Chạy frontend bằng Docker Compose (khuyến nghị)

### 4.1. Cấu trúc liên quan

```text
project-root/
├── docker-compose.yml
├── .env
├── backend/
└── frontend/
    └── streamlit/
        ├── Dockerfile
        ├── requirements.txt
        ├── app.py
        └── README.md
```

---

### 4.2. File `.env` (ví dụ)

Đặt tại **project root**:

```env
# =========================
# EDUAI ENV
# =========================

EDUAI_MODE=DEV

# Backend API
API_BASE_URL=http://eduai-backend:8011

# Data path (trong container)
EDUAI_DATA_BASE_PATH=/data

# Qdrant
QDRANT_HOST=eduai-qdrant
QDRANT_PORT=6333
```

---

### 4.3. Khởi động toàn bộ hệ thống

Tại **project root**:

```bash
docker-compose up --build
```

Hoặc chạy nền:

```bash
docker-compose up -d --build
```

---

### 4.4. Truy cập frontend

Mở trình duyệt:

```
http://localhost:8501
```

Giao diện chính:

* Login
* Semantic Search
* Pipeline Runner (DEV)
* Data Lake Explorer

---

## 5. Chạy frontend ở chế độ DEV (không khuyến nghị)

⚠️ Chỉ dùng khi debug UI, **không dùng cho pipeline thật**

### 5.1. Cài dependency

```bash
cd frontend/streamlit
pip install -r requirements.txt
```

### 5.2. Chạy Streamlit

```bash
cd frontend/c
export $(grep -v '^#' ../../.env.local | xargs)
streamlit run app.py
```

⚠️ Lưu ý:

* Backend **phải chạy trước**
* Không mount được `/data` như Docker
* Một số chức năng Data Lake sẽ không hoạt động

---

## 6. Chức năng chính của UI

### 6.1. Login

* Username: `admin`
* Password: `admin123`
* Nhận JWT token để gọi API

---

### 6.2. Semantic Search

* Nhập query ngôn ngữ tự nhiên
* Truy vấn Qdrant
* Hiển thị chunk, score, metadata

---

### 6.3. Pipeline Runner (DEV ONLY)

Chỉ hiển thị khi:

```env
EDUAI_MODE=DEV
```

Bao gồm:

* 000 – Inbox ingestion
* 200 – File staging
* 300 – Processing
* 400 – Embedding
* 401 – Qdrant indexing

⚠️ **Tuyệt đối không bật ở production**

---

### 6.4. Data Lake Explorer

Duyệt trực tiếp các zone:

* `000_inbox`
* `100_raw`
* `200_staging`
* `300_processed`
* `400_embeddings`
* `500_catalog`

Hỗ trợ preview:

* `.json`
* `.txt`

---

## 7. Lưu ý bảo mật & vận hành

* UI **không có phân quyền**
* Token hiển thị rõ trên màn hình
* Chỉ dùng trong:

  * Môi trường DEV
  * Mạng nội bộ
  * Người vận hành kỹ thuật

