import requests
from bs4 import BeautifulSoup
import re
import time

BASE = "https://www.petrolimex.com.vn"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def page_url(page: int) -> str:
    # Trang 1 dùng URL gốc, các trang sau có dạng /2.html, /3.html, ...
    if page == 1:
        return f"{BASE}/ndi/thong-cao-bao-chi.html"
    return f"{BASE}/ndi/thong-cao-bao-chi/{page}.html"

def get_last_page(soup: BeautifulSoup) -> int:
    """Đọc số trang cuối cùng từ thanh phân trang."""
    nums = []
    for a in soup.select("a"):
        text = a.get_text(strip=True)
        if text.isdigit():
            nums.append(int(text))
    return max(nums) if nums else 1

def crawl_all():
    results = []
    seen_links = set()

    # Lấy trang 1 trước để biết tổng số trang
    res = requests.get(page_url(1), headers=HEADERS, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")
    last_page = get_last_page(soup)
    print(f"Tổng số trang phát hiện được: {last_page}")
    last_page = 12
    for page in range(1, last_page + 1):
        url = page_url(page)
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
        except requests.RequestException as e:
            print(f"Lỗi khi tải trang {page}: {e}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")

        # Chỉ lấy các thẻ <a> nằm trong khu vực danh sách bài viết (heading h3),
        # để tránh dính link ở menu, footer, "Tin nổi bật", v.v.
        for h in soup.select("h3 a, h2 a"):
            title = h.get_text(strip=True)
            href = h.get("href")
            if not href:
                continue
            if not href.startswith("http"):
                href = BASE + href

            if re.search(r"điều chỉnh giá.*xăng dầu", title, re.IGNORECASE):
                if href not in seen_links:
                    seen_links.add(href)
                    results.append((title, href))
                    print(f"Đã tìm thấy: {title} - {href}")

        time.sleep(0.5)  # lịch sự với server, tránh bị chặn

    return results