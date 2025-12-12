  # GCOS107 — Khai thác thông tin (gcos107_khai_thac_thong_tin)

  Mục tiêu: Thu thập và xử lý tin tức từ VnExpress để xây dựng chỉ mục tra cứu/trích xuất thông tin và phục vụ giao diện ứng dụng để truy vấn/kết quả.

  ## Nội dung chính của project
  - crawl.py — script thu thập dữ liệu (crawler / scraper) và lưu tin tức vào thư mục vnexpress_data.
  - build_index.py — script xây dựng chỉ mục (index) từ dữ liệu đã thu thập (dùng cho việc tìm kiếm, truy vấn gần đúng,...).
  - app.py — ứng dụng web để tương tác với dữ liệu/điểm tra kết quả (chạy server hoặc mở giao diện).
  - vnexpress_data/ — thư mục chứa dữ liệu tin tức (JSON/CSV hoặc file văn bản) do crawl.py tạo ra.
  - stopwords/ — danh sách stopwords (từ dừng) phục vụ tiền xử lý tiếng Việt.

  ## Cài đặt nhanh
  1. Clone repo
  ```bash
  git clone https://github.com/TinhLogic/gcos107_khai_thac_thong_tin.git
  cd gcos107_khai_thac_thong_tin
  ```

  2. Cài đặt các gói (yêu cầu)
  - Python 3.13.9
  - Thư viện gợi ý (tùy script): requests, beautifulsoup4, lxml, pandas, scikit-learn, numpy, tqdm,...

  ```bash
  pip install -r requirements.txt
  ```

  ## Cách dùng
  1. Thu thập dữ liệu (crawl)
  ```bash
  # chạy script crawl để thu thập tin tức và lưu vào vnexpress_data/
  python crawl.py
  ```
  - Kiểm tra nội dung trong vnexpress_data/ để xác nhận các file tin tức (ví dụ: JSON, CSV hoặc file text).
  - Nếu crawl.py có tham số (số trang, category, output path), hãy chạy `python crawl.py --help` để xem cách dùng.

  2. Xử lý & xây dựng chỉ mục
  ```bash
  # chạy script build_index để tiền xử lý dữ liệu và lưu chỉ mục
  python build_index.py
  ```
  - Script này thường thực hiện: đọc vnexpress_data/*, tiền xử lý (loại bỏ stopwords, chuẩn hóa), vector hóa với TF-IDF và lưu ra folder index.
  - Kiểm tra thư mục hoặc file được tạo (ví dụ: fidf_matrix.pkl, vectorizer.pkl, documents.pkl).

  3. Chạy ứng dụng giao diện / API
  ```bash
  # chạy server/API
  streamlit run app.py
  ```
  - Mở trình duyệt tới http://localhost:8501 để thử tìm kiếm/truy vấn.