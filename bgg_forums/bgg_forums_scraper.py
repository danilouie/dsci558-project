#!/usr/bin/env python3
"""
BoardGameGeek Scraper (Forums)
Scrapes forum threads and posts, cleans text, and
writes one JSON line per game to forums.jsonl.

Forums to scrape:
    - https://boardgamegeek.com/forum/8/bgg/recommendations
    - https://boardgamegeek.com/forum/34/bgg/gaming-with-kids
    - https://boardgamegeek.com/forum/35/bgg/games-in-the-classroom
    - https://boardgamegeek.com/forum/13/bgg/trades
    - https://boardgamegeek.com/forum/10/bgg/hot-deals
"""

import requests
import xml.etree.ElementTree as ET
import json
import time
import re
import argparse
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("forums.jsonl")
API_FORUM  = "https://boardgamegeek.com/xmlapi2/forum"
API_THREAD = "https://boardgamegeek.com/xmlapi2/thread"

DELAY       = 2.0
RETRY_DELAY = 10.0
MAX_RETRIES = 5

FORUMS = {
    "recommendations": 8,
    "gaming-with-kids": 34,
    "games-in-the-classroom": 35,
    "hot-deals": 10,
    "trades": 13
}

def make_headers(api_token):
    headers = {
        "User-Agent": "bgg_forums (Danielle Louie, louiedan@usc.edu)",
        "Accept": "application/xml"
    }
    if api_token:
        headers['Authorization'] = f'Bearer {api_token}'
    return headers

def fetch_xml(url, params, headers):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
        except requests.RequestException as e:
            print(f"    [api] request error: {e} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
            continue

        if r.status_code == 200:
            time.sleep(DELAY)
            return r.text
        elif r.status_code == 202:
            print(f"    [api] 202 queued — waiting {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            print(f"    [api] HTTP {r.status_code} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

    print(f"    [api] gave up after {MAX_RETRIES}")
    return None

def clean_text(text):
    if not text:
        return ""
    
    text = re.sub(r'\n+', " ", text)
    text = re.sub(r'\s+', " ", text)
    return text.strip()

def fetch_threads(forum_id, page, headers):
    params = {"id": forum_id, "page": page}
    xml_text = fetch_xml(API_FORUM, params, headers)

    if not xml_text:
        return []
    
    try:
        root = ET.fromstring(xml_text)
        threads = []

        for t in root.findall('.//thread'):
            threads.append({
                "id": t.get("id"),
                "subject": t.get("subject"),
                "author": t.get("author"),
                "numarticles": t.get("numarticles")
            })
        return threads
    except ET.ParseError as e:
        print(f"    [xml] parse error: {e}")
        return []

def fetch_articles(thread_id, headers):
    params = {"id": thread_id}
    xml_text = fetch_xml(API_THREAD, params, headers)
    
    if not xml_text:
        return []
    
    try:
        root = ET.fromstring(xml_text)
        articles_container = root.find('articles')

        articles_container = root.find('articles')
        
        if articles_container is None:
            return []
        
        articles = []
        for a in articles_container.findall('article'):
            subject = a.find("subject")
            body = a.find("body")
            articles.append({
                "id": a.get("id"),
                "username": a.get("username"),
                "postdate": a.get("postdate"),
                "subject": subject.text if subject is not None else None,
                "body": body.text if body is not None else None,
            })
        
        return articles
    except ET.ParseError as e:
        print(f"    [xml] parse error: {e}")
        return []
    
def convert_to_qa_pairs(thread, articles, category):
    qa_pairs = []

    question = clean_text(thread["subject"])  # ✅ Fixed: was "subjec"
    q_author = thread["author"]
    thread_id = thread["id"]
    
    # skip first question bc it's the body to the main question
    for a in articles[1:]:
        response_body = clean_text(a["body"])

        if not response_body:
            continue

        qa_pair = {
            "category": category,
            "question": question,
            "response": response_body,
            "response_date": a["postdate"],
            "metadata": {
                "thread_id": thread_id,
                "question_author": q_author,
                "response_author": a["username"],
                "article_id": a["id"]
            }
        }

        qa_pairs.append(qa_pair)

    return qa_pairs

def scrape_forum(forum_id, category, max_pages, headers, output_file):
    print(f"\n  Forum: {category} (ID: {forum_id})")
    
    all_threads = []
    page = 1

    # fetch all thread listings until we run out
    while True:
        print(f"    Fetching page {page}...")
        threads = fetch_threads(forum_id, page, headers)

        if not threads:
            print(f"    No threads on page {page} — stopping")
            break
        
        all_threads.extend(threads)
        print(f"    Found {len(threads)} threads")
        
        if max_pages and page >= max_pages:
            print(f"    Reached max pages limit ({max_pages})")
            break
        
        page += 1

    print(f"    Total threads: {len(all_threads)}")
    print(f"    Fetching posts from each thread...")

    qa_count = 0

    with output_file.open("a", encoding="utf-8") as f:
        for i, thread in enumerate(all_threads, 1):
            thread_subject = thread["subject"][:50] if thread["subject"] else "Untitled"

            if i % 10 == 0 or i == len(all_threads):
                print(f"    [{i}/{len(all_threads)}] {thread_subject}...")

            articles = fetch_articles(thread["id"], headers)

            if not articles:
                continue

            qa_pairs = convert_to_qa_pairs(thread, articles, category)
            
            for qa in qa_pairs:
                f.write(json.dumps(qa, ensure_ascii=False) + "\n")
                qa_count += 1
    
    print(f"    Q&A pairs from {category}: {qa_count}")
    return qa_count
    
def main():
    parser = argparse.ArgumentParser(
        description="Scrape BGG forums to JSONL (category, question, response, date)"
    )
    parser.add_argument('--token', required=True, help='BGG API token')
    parser.add_argument('--forums', nargs='+', choices=list(FORUMS.keys()),
                       help='Forums to scrape (default: all)')
    parser.add_argument('--pages', type=int, default=None,
                       help='Max pages per forum (default: unlimited, scrape all pages)')
    parser.add_argument('--since-year', type=int, default=None,
                       help='Only scrape threads with activity since this year (e.g., 2020)')
    parser.add_argument('--output', type=str, default='forums.jsonl',
                       help='Output JSONL file (default: forums.jsonl)')
    parser.add_argument('--separate-files', action='store_true',
                       help='Create separate JSONL file for each forum (e.g., recommendations.jsonl, hot-deals.jsonl)')
    parser.add_argument('--append', action='store_true',
                       help='Append to existing file (default: overwrite)')
    args = parser.parse_args()

    # select forums
    if args.forums:
        forums_to_scrape = {name: FORUMS[name] for name in args.forums}
    else:
        forums_to_scrape = FORUMS
    
    output_file = Path(args.output)
    
    if not args.append:
        output_file.write_text('', encoding='utf-8')
        print(f"Created new file: {output_file}")
    else:
        print(f"Appending to: {output_file}")
    
    headers = make_headers(args.token)
    
    print("\n=== BGG Forum Scraper ===")
    print(f"Forums to scrape: {len(forums_to_scrape)}")
    print(f"Pages per forum: {'unlimited' if args.pages is None else args.pages}")
    print(f"Output: {output_file}")
    
    total_qa = 0
    
    for category, forum_id in forums_to_scrape.items():
        qa_count = scrape_forum(forum_id, category, args.pages, headers, output_file)
        total_qa += qa_count
    
    print(f"\n{'='*60}")
    print(f"Done! {total_qa} Q&A pairs written to {output_file}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()