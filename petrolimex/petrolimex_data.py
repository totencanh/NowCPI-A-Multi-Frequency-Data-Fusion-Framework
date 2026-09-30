import os
import urllib.request
from crawl_petrolimex import *
import re
import time
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from PIL import Image, ImageOps, ImageFilter
import pytesseract
import pandas as pd

# --- Tự động đảm bảo có gói ngôn ngữ tiếng Việt cho Tesseract ---------------
# Không phụ thuộc vào việc `apt install tesseract-ocr-vie` có đặt đúng chỗ hay
# không (trên Colab, path tessdata hay lệch bản/khác /usr/share/...). Ta tải
# thẳng file vie.traineddata từ repo chính thức của Tesseract về một thư mục
# cục bộ, rồi trỏ TESSDATA_PREFIX vào đó.
_TESSDATA_DIR = os.path.join(os.getcwd(), "tessdata")


def ensure_vietnamese_tessdata():
    os.makedirs(_TESSDATA_DIR, exist_ok=True)
    vie_path = os.path.join(_TESSDATA_DIR, "vie.traineddata")
    if not os.path.exists(vie_path):
        print("Đang tải gói ngôn ngữ tiếng Việt cho Tesseract (chỉ chạy 1 lần)...")
        url = "https://github.com/tesseract-ocr/tessdata/raw/main/vie.traineddata"
        urllib.request.urlretrieve(url, vie_path)
        print("Đã tải xong:", vie_path)
    os.environ["TESSDATA_PREFIX"] = _TESSDATA_DIR


ensure_vietnamese_tessdata()
# -----------------------------------------------------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

BASE = "https://www.petrolimex.com.vn"

# Các mặt hàng cần bóc giá, viết theo nhiều cách có thể xuất hiện trong OCR
ITEM_PATTERNS = {
    "RON95_V":   r"ron\s*95[\-\s]*v\b",
    "RON95_III": r"ron\s*95[\-\s]*iii\b",
    "E10_RON95": r"e\s*10.*ron\s*95",
    "E5_RON92":  r"e\s*5.*ron\s*92|ron\s*92",
    "DO_001S":   r"(điêzen|dau|dầu)\D{0,10}0[.,]?00?1\s*s",
    "DO_005S":   r"(điêzen|dau|dầu)\D{0,10}0[.,]?0?5\s*s",
    "DAU_HOA":   r"dầu\s*hỏa|dau\s*hoa",
    "MAZUT_35S": r"mazút|mazut",
}



# Ảnh KHÔNG phải bảng giá (logo, icon cờ, avatar...) -> loại trừ theo path
IMG_BLOCKLIST_PATTERNS = [
    r"petrolimex-s256", r"icon-vn", r"icon-en", r"/images/",
    r"thumbnailwebps",  # ảnh thumbnail cho meta og:image, không phải ảnh trong bài
]

def _is_excluded(src: str) -> bool:
    return any(re.search(p, src, re.IGNORECASE) for p in IMG_BLOCKLIST_PATTERNS)


def normalize_url(src: str) -> str:
    """Chuẩn hoá URL ảnh: xử lý protocol-relative (//...) và relative (/...)."""
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/"):
        return BASE + src
    return src


def find_price_image(article_url: str, debug: bool = False) -> str | None:
    res = requests.get(article_url, headers=HEADERS, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    all_imgs = []
    for img in soup.select("img"):
        # một số ảnh lazy-load dùng data-src / data-original thay vì src
        src = img.get("src") or img.get("data-src") or img.get("data-original") or ""
        if src:
            all_imgs.append(normalize_url(src))

    if debug:
        print(f"  [debug] Tổng số ảnh tìm thấy: {len(all_imgs)}")
        for s in all_imgs:
            print(f"  [debug]   - {s}")

    # Bước 1: mọi ảnh trong thư mục /jpgs/ (đây là nơi Petrolimex đặt ảnh bảng giá)
    candidates = [s for s in all_imgs if "/jpgs/" in s.lower()]
    if candidates:
        # Nếu có nhiều ảnh /jpgs/ (vd: bảng giá bán lẻ + bảng Quỹ BOG),
        # ảnh đầu tiên xuất hiện trong bài thường là bảng giá bán lẻ.
        return candidates[0]

    # Bước 2: fallback - ảnh nội dung trên files.petrolimex.com.vn, loại bỏ logo/icon
    fallback = [
        s for s in all_imgs
        if "files.petrolimex.com.vn" in s and not _is_excluded(s)
    ]
    if fallback:
        return fallback[0]

    return None


def preprocess_image(img: Image.Image) -> Image.Image:
    """Tiền xử lý ảnh để tăng độ chính xác OCR: grayscale, phóng to, tăng tương phản."""
    img = img.convert("L")  # grayscale
    # Phóng to 2x giúp Tesseract đọc số rõ hơn
    w, h = img.size
    img = img.resize((w * 2, h * 2), Image.LANCZOS)
    img = ImageOps.autocontrast(img)
    img = img.filter(ImageFilter.SHARPEN)
    return img


def ocr_price_table(image_url: str) -> str:
    res = requests.get(image_url, headers=HEADERS, timeout=15)
    res.raise_for_status()
    img = Image.open(BytesIO(res.content))
    img = preprocess_image(img)

    # lang="vie" cần gói tesseract-ocr-vie đã cài
    # --psm 6: coi ảnh là một khối văn bản đồng nhất (phù hợp bảng)
    text = pytesseract.image_to_string(img, lang="vie", config="--psm 6")
    return text


def parse_ocr_text(text: str) -> dict:
    """Bóc số từ text OCR theo từng dòng, khớp với tên mặt hàng đã biết."""
    result = {key: None for key in ITEM_PATTERNS}
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    for line in lines:
        low = line.lower()
        # Tìm tất cả số có 4-6 chữ số liên tiếp (sau khi bỏ dấu . , khoảng trắng)
        raw_numbers = re.findall(r"[\d][\d.,\s]{3,}\d", line)
        numbers = []
        for raw in raw_numbers:
            cleaned = re.sub(r"[^\d]", "", raw)
            if cleaned.isdigit() and 10000 <= int(cleaned) <= 99999:
                numbers.append(int(cleaned))
        if not numbers:
            continue

        for key, pattern in ITEM_PATTERNS.items():
            if re.search(pattern, low):
                # Lấy số đầu tiên tìm được trên dòng này làm giá Vùng 1
                result[key] = numbers[0]
                break

    return result


def build_dataset(links_data):
    records = []
    for title, url in links_data:
        date_match = re.search(r"ngày\s+(\d{1,2})[./](\d{1,2})[./](\d{4})", title, re.IGNORECASE)
        apply_date = None
        if date_match:
            d, m, y = date_match.groups()
            apply_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

        print(f"Đang xử lý: {title}")
        try:
            img_url = find_price_image(url, debug=False)
            if not img_url:
                # Thử lại với debug=True để in ra toàn bộ ảnh, giúp chẩn đoán
                print("  -> Không tìm thấy ảnh bảng giá theo pattern chuẩn/fallback. Đang debug...")
                img_url = find_price_image(url, debug=True)
                if not img_url:
                    print("  -> Vẫn không tìm thấy. Bỏ qua bài này (xem log [debug] ở trên).")
                    continue

            text = ocr_price_table(img_url)
            prices = parse_ocr_text(text)
            prices["Apply_Date"] = apply_date
            prices["URL"] = url
            prices["Image_URL"] = img_url
            records.append(prices)

            print(f"  -> OCR xong: {prices}")

        except Exception as e:
            print(f"  -> Lỗi: {e}")

        time.sleep(0.5)

    df = pd.DataFrame(records)
    cols = ["Apply_Date"] + list(ITEM_PATTERNS.keys()) + ["URL", "Image_URL"]
    df = df.reindex(columns=cols)
    df = df.sort_values("Apply_Date").reset_index(drop=True)
    return df


if __name__ == "__main__":
    data = crawl_all()
    output = "/app/output/"
    os.makedirs(output, exist_ok= True)
    df = build_dataset(data)
    df.to_csv(os.path.join(output, "petrolimex_prices_ocr.csv"), index=False, encoding="utf-8-sig")
    print("\n[XONG] Đã lưu petrolimex_prices_ocr.csv")
    print(df.head(10))