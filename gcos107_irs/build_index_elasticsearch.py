import os
import json
import re
from tqdm import tqdm
from underthesea import word_tokenize
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Kết nối ES
esClient = Elasticsearch("http://localhost:9200", verify_certs=False)
print("Kết nối Elasticsearch:", esClient.ping())

vnexpress_articles_index = "vnexpress_articles"

# Xóa index cũ nếu có
try:
    esClient.indices.delete(index=vnexpress_articles_index)
except:
    pass

# Tạo index mới (analyzer đơn giản)
settings = {
    "settings": {
        "analysis": {
            "analyzer": {
                "vn_text": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "vn_text"},
            "content": {"type": "text", "analyzer": "vn_text"},
            "content_raw": {"type": "text"},
            "url": {"type": "keyword"},
            "category": {"type": "keyword"},
            "date": {"type": "keyword"}
        }
    }
}

esClient.indices.create(index=vnexpress_articles_index, body=settings)
print("Đã tạo index vnexpress_articles với analyzer vn_text")

# Load stopwords
with open("stopwords/vietnamese-stopwords.txt", "r", encoding="utf-8") as f:
    STOPWORDS = set(f.read().splitlines())

def preprocess(text):
    text = text.lower()
    text = re.sub(r"[^a-zA-Z0-9À-ỹ\s]", " ", text)

    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) > 1]

    return " ".join(tokens)

# Đẩy dữ liệu
actions = []

for root, _, files in os.walk("vnexpress_data"):
    for file in tqdm(files, desc=f"Đang xử lý {os.path.basename(root)}"):
        if file.endswith(".json"):      # <-- FIX INDENT
            path = os.path.join(root, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    doc = json.load(f)

                clean_text = preprocess(doc["content"])
                if len(clean_text.split()) < 10:
                    continue

                actions.append(
                    {
                        "_index": vnexpress_articles_index,
                        "_source": {
                            "title": doc["title"],
                            "content": clean_text,
                            "content_raw": doc["content"],
                            "url": doc["url"],
                            "category": os.path.basename(root),
                            "date": doc["date"],
                        },
                    }
                )

            except Exception as e:
                print("Lỗi đọc file:", path, e)
                continue

        if len(actions) >= 500:
            bulk(esClient, actions)
            actions = []

        esClient.indices.refresh()

# Đẩy phần còn lại
if actions:
    bulk(esClient, actions)

esClient.indices.refresh(index=vnexpress_articles_index)

print(
    f"HOÀN TẤT! Đã lập chỉ mục {esClient.cat.count(index=vnexpress_articles_index, format='json')[0]['count']} bài báo."
)
