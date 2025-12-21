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
import hashlib

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
    "phap-luat": {"url": "https://vnexpress.net/phap-luat", "limit": 100},
    # "suc-khoe": {"url": "https://vnexpress.net/suc-khoe", "limit": 300},
    # "giao-duc": {"url": "https://vnexpress.net/giao-duc", "limit": 300},
    # "du-lich": {"url": "https://vnexpress.net/du-lich", "limit": 300},
    # "khoa-hoc-cong-nghe": {"url": "https://vnexpress.net/khoa-hoc-cong-nghe", "limit": 300},
    # "doi-song": {"url": "https://vnexpress.net/doi-song", "limit": 300},
    # "xe": {"url": "https://vnexpress.net/oto-xe-may", "limit": 300},
    #"bat-dong-san": {"url": "https://vnexpress.net/bat-dong-san", "limit": 5},
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

def slugify_title(title: str, max_len: int = 120) -> str:
    """
    'Tiêu đề có dấu' -> 'tieu-de-co-dau'
    - viết thường
    - bỏ dấu tiếng Việt
    - chỉ giữ a-z, 0-9
    - cách nhau bởi '-'
    - giới hạn độ dài để tránh path quá dài
    """
    if not title:
        return ""

    title = title.strip().lower()

    # bỏ dấu: NFD rồi loại bỏ combining marks
    title = unicodedata.normalize("NFD", title)
    title = "".join(ch for ch in title if unicodedata.category(ch) != "Mn")
    title = title.replace("đ", "d")

    # ký tự không phải chữ/số -> space
    title = re.sub(r"[^a-z0-9]+", " ", title).strip()

    # space -> '-'
    title = re.sub(r"\s+", "-", title)

    # tránh tên rỗng + cắt ngắn an toàn
    title = title.strip("-")
    if max_len and len(title) > max_len:
        title = title[:max_len].rstrip("-")

    return title

def md5_text(s: str) -> str:
    """Tính MD5 cho chuỗi Unicode (UTF-8)."""
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def load_hashes(hash_file: str) -> set:
    """Load tập MD5 đã lưu từ file (nếu chưa có thì trả set rỗng)."""
    if not os.path.exists(hash_file):
        return set()
    try:
        with open(hash_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
        return set()
    except Exception:
        return set()

def save_hashes(hash_file: str, hashes_set: set) -> None:
    """Lưu tập MD5 ra file."""
    with open(hash_file, "w", encoding="utf-8") as f:
        json.dump(sorted(list(hashes_set)), f, ensure_ascii=False, indent=2)

def sync_hashes_from_existing_files(cat_dir: str, hash_file: str) -> set:
    """
    Đồng bộ hashes.json dựa trên các file bài đã có trong cat_dir.
    - Nếu hashes.json chưa có: tạo mới.
    - Nếu hashes.json có nhưng thiếu: bổ sung.
    Trả về set seen_hashes (đã sync).
    """
    seen_hashes = load_hashes(hash_file)
    updated = False

    # Quét tất cả file .json trong cat_dir (trừ hashes.json)
    for fname in os.listdir(cat_dir):
        if not fname.endswith(".json"):
            continue
        if fname == "hashes.json":
            continue

        fpath = os.path.join(cat_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                obj = json.load(f)

            content = obj.get("content", "")
            if not content:
                # file cũ bị lỗi/không có content -> bỏ qua
                continue

            h = md5_text(content)
            if h not in seen_hashes:
                seen_hashes.add(h)
                updated = True

        except Exception:
            # file hỏng json -> bỏ qua
            continue

    # Nếu hashes.json chưa tồn tại hoặc có cập nhật thì ghi lại
    if (not os.path.exists(hash_file)) or updated:
        save_hashes(hash_file, seen_hashes)

    return seen_hashes

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
        article_block = soup.find("article", class_="fck_detail") or soup.find("div", class_="fck_detail")

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

        # File lưu MD5 content để chống trùng (giữa các lần chạy)
        hash_file = os.path.join(cat_dir, "hashes.json")
        seen_hashes = sync_hashes_from_existing_files(cat_dir, hash_file)

        print(f"Đã sync hashes: {len(seen_hashes)} content hash trong {cat}")

        # Lấy danh sách bài
        links = get_article_links(info["url"], max_pages=2)
        print(f"Tổng cộng lấy được {len(links)} link thô")

        # Lấy nhiều gấp đôi để lọc bài lỗi
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
                # 1) Đặt tên file theo slug tiêu đề
                slug = slugify_title(data.get("title", ""))
                if not slug:
                    print("Skip (không có title để đặt tên file)")
                    time.sleep(random.uniform(1.5, 2.8))
                    continue

                filename = os.path.join(cat_dir, f"{slug}.json")

                # Nếu trùng tên file -> bỏ qua (đúng yêu cầu trước đó)
                if os.path.exists(filename):
                    print(f"Skip (trùng tên file): {slug}.json")
                    time.sleep(random.uniform(1.5, 2.8))
                    continue

                # 2) Tính MD5 của content đã xử lý để check trùng
                content_processed = data.get("content", "")
                if not content_processed:
                    print("Skip (content rỗng)")
                    time.sleep(random.uniform(1.5, 2.8))
                    continue

                content_hash = md5_text(content_processed)

                # Nếu trùng content -> bỏ qua
                if content_hash in seen_hashes:
                    print("Skip (trùng content theo MD5)")
                    time.sleep(random.uniform(1.5, 2.8))
                    continue
             
                # 3) Lưu file JSON
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                # 4) Ghi nhận hash đã lưu + persist ra hashes.json
                seen_hashes.add(content_hash)
                save_hashes(hash_file, seen_hashes)

                print("OK")
                count += 1
                total += 1
            else:
                print("Skip (bài rỗng hoặc lỗi)")

            time.sleep(random.uniform(1.5, 2.8))

    print("\nHOÀN THÀNH! Đã crawl tổng cộng:", total, "bài báo")

if __name__ == "__main__":
    main()
