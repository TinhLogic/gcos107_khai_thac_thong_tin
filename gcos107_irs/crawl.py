import requests
from bs4 import BeautifulSoup
import os
import time
import json
import re
from urllib.parse import urljoin
import random

# ================== CONFIG ==================
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Connection": "keep-alive",
}

BASE_DIR = "vnexpress_data"
os.makedirs(BASE_DIR, exist_ok=True)

CATEGORIES = {
    "the-gioi": {"url": "https://vnexpress.net/the-gioi", "limit": 500},
    "thoi-su": {"url": "https://vnexpress.net/thoi-su", "limit": 500},
    "kinh-doanh": {"url": "https://vnexpress.net/kinh-doanh", "limit": 500},
    "the-thao": {"url": "https://vnexpress.net/the-thao", "limit": 500},
    "giai-tri": {"url": "https://vnexpress.net/giai-tri", "limit": 300},
    "phap-luat": {"url": "https://vnexpress.net/phap-luat", "limit": 500},
    "suc-khoe": {"url": "https://vnexpress.net/suc-khoe", "limit": 300},
    "giao-duc": {"url": "https://vnexpress.net/giao-duc", "limit": 300},
    "du-lich": {"url": "https://vnexpress.net/du-lich", "limit": 300},
    "khoa-hoc-cong-nghe": {
        "url": "https://vnexpress.net/khoa-hoc-cong-nghe",
        "limit": 300,
    },
    "doi-song": {"url": "https://vnexpress.net/doi-song", "limit": 300},
    "xe": {"url": "https://vnexpress.net/oto-xe-may", "limit": 300},
}

def format_string(txt):
    if len(txt) <= 0:
        return ""

    # Chuẩn hóa dấu gạch ngang thành " - "
    txt = re.sub(r"\s*[-–—]\s*", " - ", txt)

    # Nếu crawl bị dính chữ như "MỹPhi" → tách chữ HOA
    txt = re.sub(r"[^a-zA-Z0-9À-ỹ\s]", " ", txt)

    # Gom nhiều khoảng trắng lại còn 1
    txt = re.sub(r"\s+", " ", txt).strip()

    return txt


# ================== LẤY LINK (CHUẨN 2025) ==================
def get_article_links(category_url, max_pages=20):
    links = set()

    for page in range(1, max_pages + 1):
        url = category_url if page == 1 else f"{category_url}-p{page}"
        print(f"Crawling page: {url}")

        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            found = 0

            # VNExpress 2024–2025 selector mới
            items = soup.select("article.item-news a, div.item-news a")

            for a in items:
                href = a.get("href", "")
                if not href:
                    continue

                full_url = urljoin("https://vnexpress.net", href)

                # Chỉ lấy bài báo thật
                if full_url.endswith(".html") and "/video/" not in full_url:
                    links.add(full_url)
                    found += 1

            print(f"Trang {page}: +{found} link | Tổng: {len(links)}")
            time.sleep(random.uniform(1.2, 2.0))

        except Exception as e:
            print(f"Lỗi: {e}")

    return list(links)


# ================== CRAWL BÀI VIẾT ==================
def crawl_article(url):
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # ================== TITLE ==================
        title = soup.find("h1", class_="title-detail")
        title = title.get_text(strip=True) if title else ""

        # ================== DATE ==================
        date = soup.find("span", class_="date")
        date = date.get_text(strip=True) if date else ""

        # ================== DESCRIPTION ==================
        desc = soup.find("p", class_="description")
        desc = desc.get_text(strip=True) if desc else ""
        desc = format_string(desc)

        # ================== CONTENT ==================
        content_parts = []
        article_block = soup.find("article", class_="fck_detail") or soup.find(
            "div", class_="fck_detail"
        )

        if article_block:
            for p in article_block.find_all("p"):
                txt = p.get_text(strip=True)
                if txt:
                    content_parts.append(txt)

        # Gắn tất cả các content thành chuỗi
        full_content = " ".join([title + " " + desc] + content_parts)

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
        links = get_article_links(info["url"], max_pages=1)
        print(f"Tổng cộng lấy được {len(links)} link thô")

        # Lấy nhiều gấp đôi để lọc bài lỗi (slice lấy từ phẩn tử đầu tiên đến phần tử thứ limit * 2)
        links = links[: info["limit"] * 2]

        count = 0
        for idx, link in enumerate(links, 1):
            if count >= info["limit"]:
                break

            print(f"[{count + 1}/{info['limit']}] {link}")

            data = crawl_article(link)
            if data:
                filename = os.path.join(cat_dir, f"{count + 1:04d}.json")
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                print("OK")
                count += 1
                total += 1
            else:
                print("Skip (bài rỗng hoặc lỗi)")

            time.sleep(random.uniform(1.5, 2.8))

    print("\nHOÀN THÀNH! Đã crawl tổng cộng:", total, "bài báo")

if __name__ == "__main__":
    main()
