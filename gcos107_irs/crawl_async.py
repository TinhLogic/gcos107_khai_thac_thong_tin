import requests
from bs4 import BeautifulSoup
import os
import json
import re
import time
import unicodedata
import random
from urllib.parse import urljoin

import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector

# ================== CONFIG ==================
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Connection": "keep-alive",
}

BASE_DIR = "vnexpress_data"
os.makedirs(BASE_DIR, exist_ok=True)

CATEGORIES = {
    "the-thao": {"url": "https://vnexpress.net/the-thao", "limit": 300},
    "giai-tri": {"url": "https://vnexpress.net/giai-tri", "limit": 300},
    "phap-luat": {"url": "https://vnexpress.net/phap-luat", "limit": 300},
    "suc-khoe": {"url": "https://vnexpress.net/suc-khoe", "limit": 300},
    "giao-duc": {"url": "https://vnexpress.net/giao-duc", "limit": 300},
    "du-lich": {"url": "https://vnexpress.net/du-lich", "limit": 300},
    "khoa-hoc-cong-nghe": {"url": "https://vnexpress.net/khoa-hoc-cong-nghe", "limit": 300},
    "doi-song": {"url": "https://vnexpress.net/doi-song", "limit": 300},
    "xe": {"url": "https://vnexpress.net/oto-xe-may", "limit": 300},
}

# ================== FORMAT STRING ==================
def format_string(txt):
    if not txt:
        return ""

    txt = unicodedata.normalize("NFC", txt)
    txt = re.sub(r"\s*[-–—]\s*", " ", txt)
    txt = re.sub(r"[^0-9a-zA-ZÀ-ỹ\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()

    return txt


# ============================================================
#                   ASYNC GET LINKS
# ============================================================

async def fetch_page_links(session, url, sem):
    """Lấy link bài từ 1 trang category."""
    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []

                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")

                items = soup.select("article.item-news a, div.item-news a")

                links = []
                for a in items:
                    href = a.get("href", "")
                    if not href:
                        continue

                    full_url = urljoin("https://vnexpress.net", href)
                    if full_url.endswith(".html") and "/video/" not in full_url:
                        links.append(full_url)

                return links

        except:
            return []


async def async_get_article_links(category_url, max_pages=20):
    """Lấy link bài async."""
    timeout = ClientTimeout(total=20)
    connector = TCPConnector(limit=30)
    sem = asyncio.Semaphore(10)  # tránh spam quá mạnh

    async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:

        tasks = []
        for page in range(1, max_pages + 1):
            url = category_url if page == 1 else f"{category_url}-p{page}"
            tasks.append(fetch_page_links(session, url, sem))

        print(f"Đang lấy link async từ {max_pages} trang...")

        results = await asyncio.gather(*tasks)

    # Gộp & loại trùng
    link_set = set()
    for lst in results:
        for link in lst:
            link_set.add(link)

    print(f"LẤY LINK XONG — Tổng: {len(link_set)} link\n")
    return list(link_set)


# ============================================================
#                   ASYNC CRAWL BÀI VIẾT
# ============================================================

async def async_crawl_article(session, url, sem):
    async with sem:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return None

                text = await resp.text()
                soup = BeautifulSoup(text, "html.parser")

                # TITLE
                title = soup.find("h1", class_="title-detail")
                title = title.get_text(strip=True) if title else ""

                # DATE
                date = soup.find("span", class_="date")
                date = date.get_text(strip=True) if date else ""

                # DESCRIPTION
                desc = soup.find("p", class_="description")
                desc = desc.get_text(" ", strip=True) if desc else ""
                desc = format_string(desc)

                # CONTENT
                content_parts = []
                article_block = soup.find("article", class_="fck_detail") or soup.find(
                    "div", class_="fck_detail"
                )

                if article_block:
                    for p in article_block.find_all("p"):
                        txt = p.get_text(" ", strip=True)
                        if txt:
                            content_parts.append(txt)

                full_content = " ".join([title + " " + desc] + content_parts)
                full_content = format_string(full_content)

                if len(full_content) < 200:
                    return None

                return {
                    "url": url,
                    "title": title,
                    "date": date,
                    "description": desc,
                    "content": full_content,
                }

        except:
            return None


# ============================================================
#                   XỬ LÝ MỖI CATEGORY
# ============================================================

async def process_category(cat, info):
    print(f"\n=========== BẮT ĐẦU CRAWL {cat.upper()} ===========")

    cat_dir = os.path.join(BASE_DIR, cat)
    os.makedirs(cat_dir, exist_ok=True)

    # ---- Lấy link async ----
    links = await async_get_article_links(info["url"], max_pages=20)
    links = links[: info["limit"] * 2]

    print(f"Total raw links: {len(links)}")

    timeout = ClientTimeout(total=15)
    connector = TCPConnector(limit=30)
    sem = asyncio.Semaphore(20)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:
        tasks = [async_crawl_article(session, link, sem) for link in links]
        results = await asyncio.gather(*tasks)

    # Lọc bài hợp lệ
    articles = [r for r in results if r]
    articles = articles[: info["limit"]]

    print(f"Saved articles: {len(articles)}")

    # Lưu file tuần tự: 0001.json → 0300.json
    for i, article in enumerate(articles, start=1):
        filename = os.path.join(cat_dir, f"{i:04d}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(article, f, ensure_ascii=False, indent=2)

    return len(articles)


# ============================================================
#                   MAIN
# ============================================================
async def main_async():
    total = 0
    for cat, info in CATEGORIES.items():
        count = await process_category(cat, info)
        total += count
        await asyncio.sleep(1)

    print("\nHOÀN THÀNH! Tổng số bài đã crawl:", total)


if __name__ == "__main__":
    asyncio.run(main_async())
