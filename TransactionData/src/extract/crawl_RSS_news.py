import json
import os
from datetime import datetime, timezone
from pathlib import Path
import trafilatura
import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("NEWSDATA_API_KEY")
BASE_URL = os.getenv(
    "NEWSDATA_BASE_URL",
    "https://newsdata.io/api/1"
)

path = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/news"

path_process = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/news/process"

def query_news():
    url = f"{BASE_URL}/latest"

    params = {
        "apikey": API_KEY,
        "country": "vn",
        "language": "vi",
        "category": "business",
    }

    MAX_ARTICLES = 200
    all_results = []

    while len(all_results)< MAX_ARTICLES:
        response = requests.get(
            url,
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        results = data.get("results", [])
        all_results.extend(results)

        if len(all_results) >= MAX_ARTICLES:
            all_results=all_results[:MAX_ARTICLES]
            break

        next_page = data.get("nextPage")

        if not next_page:
            break

        params["page"] = next_page

    return {
        "status": "success",
        "totalResults": len(all_results),
        "results": all_results,
    }
def save_raw_news(data):
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    raw_file = f"{path}/newsdata_{date}.json"
    
    with open(
        raw_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
        )


def crawl_article(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=20,
        )

        if response.status_code != 200:
            return None

        content = trafilatura.extract(
            response.text,
            include_comments=False,
            include_tables=False,
        )

        return content

    except requests.RequestException:
        return None
        
def process_raw_news():
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_file = f"{path}/newsdata_{date}.json"
    with open(raw_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    processed_data = []

    for article in raw_data.get("results", []):

        url = article.get("link")

        if not url:
            continue

        full_content  = crawl_article(url)

        processed_article = {
            # =========================
            # Thông tin bài viết
            # =========================
            "article_id": article.get("article_id"),
            "title": article.get("title"),
            "description": article.get("description"),
            "content": article.get("content"),
            "full_content": full_content,
            "keywords": article.get("keywords") or [],
            "link": url,

            # =========================
            # Thời gian
            # =========================
            "pub_date": article.get("pubDate"),
            "pub_date_tz": article.get("pubDateTZ"),
            "fetched_at": article.get("fetched_at"),

            # =========================
            # Nguồn
            # =========================
            "source_id": article.get("source_id"),
            "source_name": article.get("source_name"),
            "source_priority": article.get("source_priority"),
            "source_url": article.get("source_url"),

            # =========================
            # Phân loại
            # =========================
            "language": article.get("language"),
            "country": article.get("country") or [],
            "category": article.get("category") or [],
            "datatype": article.get("datatype"),

                
        }

        processed_data.append(processed_article)

    return processed_data


def save_processed_news(data):
    
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    process_file = f"{path_process}/newsdata_processed_{date}.json"

    with open(process_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def crawl_RSS_news():
    # 1. Query NewsData
    raw_data = query_news()

    # 2. Save raw JSON
    save_raw_news(raw_data)

    # 3. Read raw → crawl URL → create processed data
    processed_data = process_raw_news()

    # 4. Save processed JSON
    save_processed_news(processed_data)
    
crawl_RSS_news()

