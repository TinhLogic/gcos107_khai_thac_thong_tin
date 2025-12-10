# app_elasticsearch.py - Tìm kiếm bằng Elasticsearch (cosine + highlight + siêu nhanh)
import streamlit as st
import time
from elasticsearch import Elasticsearch
from underthesea import word_tokenize

es = Elasticsearch("http://localhost:9200")

def search(query, size=15):
    if not query.strip():
        return []
    
    # Preprocess query giống lúc index
    tokens = word_tokenize(query.lower())
    clean_tokens = [t for t in tokens if len(t) > 1]
    search_query = " ".join(clean_tokens)
    
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": search_query,
                "fields": ["content"],  # title quan trọng gấp 3
                "type": "best_fields",
                # "fuzziness": "AUTO"
            }
        },
        "highlight": {
            "fields": {
                "content": {"fragment_size": 150, "number_of_fragments": 2},
                "title": {}
            },
            "pre_tags": ["<b style='color:red'>"], "post_tags": ["</b>"]
        }
    }
    
    res = es.search(index="vnexpress_articles", body=body)
    return res['hits']['hits']

# Giao diện
st.set_page_config(page_title="Tìm kiếm VnExpress - Elasticsearch", layout="wide")
st.title("Hệ thống Tìm kiếm Tin tức – Elasticsearch")
st.markdown("**TF-IDF + Cosine Similarity + Highlight** • Đồ án Truy hồi Thông tin")

query = st.text_input("Tìm kiếm mọi thứ...")

if query:
    start = time.time()
    with st.spinner("Đang tìm trong 3000+ bài báo..."):
        results = search(query)

    elapsed = (time.time() - start) * 1000   # ms
    
    st.success(f"Tìm thấy {len(results)} kết quả trong {elapsed:.2f} ms")
    
    for hit in results:
        source = hit['_source']
        highlight = hit.get('highlight', {})
        
        title = highlight.get('title', [source['title']])[0]
        snippet = highlight.get('content', [source['content_raw'][:200] + '...'])[0]
        
        st.markdown(f"### [{title}]({source['url']})", unsafe_allow_html=True)
        st.caption(f"**{source['category'].title()}** • {source['date']}")
        st.markdown(f"**Snippet:** {snippet}", unsafe_allow_html=True)
        st.markdown("---")
else:
    st.info("Gõ gì cũng ra nấy")