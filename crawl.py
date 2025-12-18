# import thư viện cần thiết
import requests
from bs4 import BeautifulSoup
import os
import time
import json
import re
import unicodedata
from urllib.parse import urljoin
import random

# Cấu hình http request headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Connection": "keep-alive",
}

# cấu đường dẫn thư mực lưu dữ liệu
BASE_DIR = "vnexpress_data"

# Tạo thư mục lưu dữ liệu nếu chưa tồn tại
os.makedirs(BASE_DIR, exist_ok=True)

# định nghĩa các chuyên mục và giới hạn bài cần crawl
CATEGORIES = {
    # "the-gioi": {"url": "https://vnexpress.net/the-gioi", "limit": 300},
    # "thoi-su": {"url": "https://vnexpress.net/thoi-su", "limit": 300},
    # "kinh-doanh": {"url": "https://vnexpress.net/kinh-doanh", "limit": 300},
    # "the-thao": {"url": "https://vnexpress.net/the-thao", "limit": 300},
    # "giai-tri": {"url": "https://vnexpress.net/giai-tri", "limit": 300},
    # "phap-luat": {"url": "https://vnexpress.net/phap-luat", "limit": 300},
    # "suc-khoe": {"url": "https://vnexpress.net/suc-khoe", "limit": 300},
    # "giao-duc": {"url": "https://vnexpress.net/giao-duc", "limit": 300},
    "du-lich": {"url": "https://vnexpress.net/du-lich", "limit": 300},
    "khoa-hoc-cong-nghe": {
        "url": "https://vnexpress.net/khoa-hoc-cong-nghe",
        "limit": 300,
    },
    "doi-song": {"url": "https://vnexpress.net/doi-song", "limit": 300},
    "xe": {"url": "https://vnexpress.net/oto-xe-may", "limit": 300},
}

# Xử lý định dạng chuỗi văn bản
def format_string(txt):
    if not txt:
        return ""

    # 1. Chuẩn hóa unicode tiếng Việt (tránh mất dấu)
    txt = unicodedata.normalize("NFC", txt)

    # 2. Thay các dấu -, –, — thành 1 space (không dính từ)
    txt = re.sub(r"\s*[-–—]\s*", " ", txt)

    # 3. Giữ lại chữ cái tiếng Việt, số và khoảng trắng
    #   Lưu ý: \w không dùng được vì giữ _
    txt = re.sub(r"[^0-9a-zA-ZÀ-ỹ\s]", " ", txt)

    # 4. Gom nhiều space thành 1
    txt = re.sub(r"\s+", " ", txt).strip()

    return txt

# Lấy các links bài viết từ các chuyên mục
def get_article_links(category_url, max_pages=20):
    links = set()

    # Duyệt qua các trang trong chuyên mục
    for page in range(1, max_pages + 1):
        url = category_url if page == 1 else f"{category_url}-p{page}"
        print(f"Crawling page: {url}")

        try:
            # Gửi yêu cầu HTTP GET
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                continue
            
            # Phân tích HTML
            soup = BeautifulSoup(r.text, "html.parser")
            found = 0

            # Tìm tất cả thẻ <a> trong các thẻ bài viết
            items = soup.select("article.item-news a, div.item-news a")

            # Lấy link từ các thẻ <a>
            for a in items:
                href = a.get("href", "")
                if not href:
                    continue
                
                # Tạo link đầy đủ
                full_url = urljoin("https://vnexpress.net", href)

                # Chỉ lấy bài báo thật, không lấy trang html khác hoặc video
                if full_url.endswith(".html") and "/video/" not in full_url:
                    links.add(full_url)
                    found += 1
            
            time.sleep(random.uniform(1.2, 2.0))

        except Exception as e:
            print(f"Lỗi: {e}")

    return list(links)

# CRAWL Bài viết
def crawl_article(url):
    try:
        # Gửi yêu cầu HTTP GET
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None

        # Phân tích HTML
        soup = BeautifulSoup(r.text, "html.parser")

        # ================== TITLE ==================
        title = soup.find("h1", class_="title-detail")
        title = title.get_text(strip=True) if title else ""

        # ================== DATE ==================
        date = soup.find("span", class_="date")
        date = date.get_text(strip=True) if date else ""

        # ================== DESCRIPTION ==================
        desc = soup.find("p", class_="description")
        desc = desc.get_text(" ", strip=True) if desc else ""
        desc = format_string(desc)

        # ================== CONTENT ==================
        content_parts = []
        article_block = soup.find("article", class_="fck_detail") or soup.find(
            "div", class_="fck_detail"
        )

        # Lấy tất cả các đoạn văn trong bài
        if article_block:
            for p in article_block.find_all("p"):
                txt = p.get_text(" ", strip=True)
                if txt:
                    content_parts.append(txt)

        # Gắn tất cả các content thành chuỗi
        full_content = " ".join([title + " " + desc] + content_parts)

        # Định dạng chuỗi văn bản
        full_content = format_string(full_content)

        # Loại bài rỗng
        if len(full_content) < 200:
            return None

        return {
            "url": url,
            "title": title,
            "date": date,
            "description": desc,
            "content": full_content.strip(),
        }

    except Exception as e:
        print(f"Lỗi crawl bài {url}: {e}")
        return None

# ================== MAIN ==================
def main():
    total = 0

    for cat, info in CATEGORIES.items():
        print(f"BẮT ĐẦU CRAWL: {cat.upper()} — mục tiêu {info['limit']} bài")

        # Folder lưu
        cat_dir = os.path.join(BASE_DIR, cat)
        os.makedirs(cat_dir, exist_ok=True)

        # Lấy danh sách bài
        links = get_article_links(info["url"], max_pages=20)
        print(f"Tổng cộng lấy được {len(links)} link thô")

        # Lấy nhiều gấp đôi để lọc bài lỗi (slice lấy từ phẩn tử đầu tiên đến phần tử thứ limit * 2)
        links = links[: info["limit"] * 2]

        count = 0
        # Duyệt từng link bài viết
        for idx, link in enumerate(links, 1):
            if count >= info["limit"]:
                break
            
            # Hiển thị tiến trình
            print(f"[{count + 1}/{info['limit']}] {link}")

            # Crawl bài viết
            data = crawl_article(link)
            if data:
                # Lưu dữ liệu ra file JSON
                filename = os.path.join(cat_dir, f"{count + 1:04d}.json")
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                # Thông báo lưu dữ liệu thành công
                print("OK")
                count += 1
                total += 1
            else:
                print("Skip (bài rỗng hoặc lỗi)")

            time.sleep(random.uniform(1.5, 2.8))

    print("\nHOÀN THÀNH! Đã crawl tổng cộng:", total, "bài báo")

if __name__ == "__main__":
    main()
