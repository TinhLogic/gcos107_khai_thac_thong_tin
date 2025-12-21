# build_index.py - Chạy 1 lần để tạo chỉ mục TF-IDF
import os
import json
import pickle
from tqdm import tqdm
from underthesea import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
import re

# Đường dẫn dữ liệu và chỉ mục
DATA_DIR = "vnexpress_data"
INDEX_DIR = "index"
os.makedirs(INDEX_DIR, exist_ok=True)

# Load stopwords
with open("stopwords/vietnamese-stopwords.txt", "r", encoding="utf-8") as f:
    STOPWORDS = set(f.read().splitlines())

def preprocess(text):
    # Tách từ + lowercase + bỏ stopword + lọc ký tự lạ
    text = text.lower()
    # thay thế các ký tự lạ thành space
    text = re.sub(r"[^a-zA-Z0-9À-ỹ\s]", " ", text)
    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) > 1]
    return " ".join(tokens)

print("Bắt đầu đọc dữ liệu và tiền xử lý...")
documents = []
metadata = []  # lưu url, title, date, category để hiển thị kết quả

# Duyệt qua tất cả các file JSON trong thư mục dữ liệu
for root, dirs, files in os.walk(DATA_DIR):
    for file in tqdm(files, desc=f"Đang xử lý {os.path.basename(root)}"):
        if file.endswith(".json"):
            path = os.path.join(root, file)
            try:
                # Đọc dữ liệu bài viết
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                clean_text = preprocess(data["content"])

                # Bỏ qua bài quá ngắn
                if len(clean_text.split()) < 10:
                    continue
                documents.append(clean_text)
                metadata.append(
                    {
                        "title": data["title"],
                        "url": data["url"],
                        "date": data["date"],
                        "category": os.path.basename(root),
                    }
                )
            except:
                continue

print(f"Đã xử lý xong {len(documents)} tài liệu hợp lệ")
print("Đang xây dựng ma trận TF-IDF...")
# Tạo ma trận TF-IDF
# Sử dụng n-gram (1,2) và giới hạn số đặc trưng
# Bạn có thể điều chỉnh max_features tùy theo nhu cầu
vectorizer = TfidfVectorizer(
    max_features=50000,
    ngram_range=(1, 2)
)

# Tạo ma trận TF-IDF
tfidf_matrix = vectorizer.fit_transform(documents)

print(f"TF-IDF matrix shape: {tfidf_matrix.shape}")
# Lưu ma trận và vectorizer
with open(os.path.join(INDEX_DIR, "tfidf_matrix.pkl"), "wb") as f:
    pickle.dump(tfidf_matrix, f)
with open(os.path.join(INDEX_DIR, "vectorizer.pkl"), "wb") as f:
    pickle.dump(vectorizer, f)
with open(os.path.join(INDEX_DIR, "documents.pkl"), "wb") as f:
    pickle.dump(metadata, f)

print("HOÀN TẤT! Chỉ mục đã được lưu vào thư mục index.")
print("Giờ bạn chỉ cần chạy: streamlit run app.py")
