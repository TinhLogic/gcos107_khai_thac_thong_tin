
docker run -d --name elasticsearch \
  -p 9200:9200 -p 5601:5601 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0

Các thư viện:
  - python==3.13.9
  - requests
  - beautifulsoup4 
  - lxml 
  - underthesea 
  - scikit-learn 
  - streamlit 
  - pandas 
  - numpy 
  - tqdm
  - elasticsearch==8.15.0

Stopwords được sử dụng trong dự án này: https://github.com/phamtheanhphu/IRS_Course/blob/master/data/stopwords/vietnamese-stopwords.txt
