# app.py - Giao diện tìm kiếm web
import streamlit as st
import pickle
import re
from sklearn.metrics.pairwise import cosine_similarity
from underthesea import word_tokenize

# Load chỉ mục
@st.cache_resource
def load_index():
    with open("index/tfidf_matrix.pkl", "rb") as f:
        tfidf_matrix = pickle.load(f)
    with open("index/vectorizer.pkl", "rb") as f:
        vectorizer = pickle.load(f)
    with open("index/documents.pkl", "rb") as f:
        metadata = pickle.load(f)
    return tfidf_matrix, vectorizer, metadata

tfidf_matrix, vectorizer, metadata = load_index()

# Stopwords
with open("stopwords/vietnamese-stopwords.txt", "r", encoding="utf-8") as f:
  STOPWORDS = set(f.read().splitlines())
  print(f"{len(STOPWORDS)} stopwords load")

# Xử lý truy vấn
def preprocess(text):
  # Tách từ + lowercase + bỏ stopword + lọc ký tự lạ
    text = text.lower()
    # thay thế các ký tự lạ thành space
    text = re.sub(r"[^a-zA-Z0-9À-ỹ\s]", " ", text)
    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) > 1]
    return " ".join(tokens)

def search(query, top_k=10):
    if not query.strip():
        return []
    clean_query = preprocess(query)
    query_vec = vectorizer.transform([clean_query])
    cosine_sim = cosine_similarity(query_vec, tfidf_matrix).flatten()
    top_indices = cosine_sim.argsort()[-top_k:][::-1]
    results = []
    for idx in top_indices:
        if cosine_sim[idx] > 0:
            results.append({
                "score": round(float(cosine_sim[idx]), 4),
                "title": metadata[idx]["title"],
                "url": metadata[idx]["url"],
                "date": metadata[idx]["date"],
                "category": metadata[idx]["category"]
            })
    return results

# === GIAO DIỆN STREAMLIT ===
st.set_page_config(page_title="Tìm kiếm tin tức VnExpress - TF-IDF", layout="wide")

st.title("Hệ thống Tìm kiếm Tin tức Tiếng Việt")
st.markdown("**Đề tài Truy hồi Thông tin - Dựa trên Mô hình Không gian Vector (TF-IDF)**")

query = st.text_input("Nhập từ khóa tìm kiếm (ví dụ: bầu cử Mỹ, cao tốc Bắc Nam, bóng đá Việt Nam...)", 
                      placeholder="Gõ ở đây rồi nhấn Enter")

if query:
    with st.spinner("Đang tìm kiếm..."):
        results = search(query, top_k=15)
    
    st.markdown(f"**Tìm thấy {len(results)} kết quả cho:** `{query}`")
    
    for i, r in enumerate(results, 1):
        with st.expander(f"{i}. {r['title']} ({r['score']})"):
            st.caption(f"**Chuyên mục:** {r['category'].upper()} | **Ngày:** {r['date']}")
            st.markdown(f"[Đọc bài gốc]({r['url']})", unsafe_allow_html=True)
else:
    st.info("Nhập từ khóa ở ô trên để bắt đầu tìm kiếm")
    st.markdown("""
    ### Ví dụ gợi ý bạn thử:
    - `phạm nhật vượng`
    - `bão yagi`
    - `u23 việt nam`
    - `ô tô điện vinfast`
    - `chứng khoán hôm nay`
    """)