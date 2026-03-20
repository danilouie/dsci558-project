#!/usr/bin/env python3
"""
Debug script to test BGG API responses and check structure
"""

import requests
import xml.etree.ElementTree as ET
import sys

API_TOKEN = sys.argv[1] if len(sys.argv) > 1 else None

if not API_TOKEN:
    print("Usage: python debug_api.py YOUR_TOKEN")
    sys.exit(1)

headers = {
    "User-Agent": "bgg_forums_debug",
    "Accept": "application/xml",
    "Authorization": f"Bearer {API_TOKEN}"
}

# Test 1: fetch a thread from recommendations forum
print("="*60)
print("Test 1: Fetching threads from recommendations forum (ID: 8)")
print("="*60)

r = requests.get("https://boardgamegeek.com/xmlapi2/forum", 
                 params={"id": 8, "page": 1}, 
                 headers=headers)

if r.status_code == 200:
    root = ET.fromstring(r.text)
    threads = root.findall('.//thread')
    print(f"✅ Found {len(threads)} threads")
    
    if threads:
        # get first thread details
        first_thread = threads[0]
        thread_id = first_thread.get('id')
        thread_subject = first_thread.get('subject')
        thread_articles = first_thread.get('numarticles')
        
        print(f"\nFirst thread:")
        print(f"  ID: {thread_id}")
        print(f"  Subject: {thread_subject[:60]}...")
        print(f"  Articles: {thread_articles}")
        
        # Test 2: fetch articles from that thread
        print(f"\n{'='*60}")
        print(f"Test 2: Fetching articles from thread {thread_id}")
        print(f"{'='*60}")
        
        r2 = requests.get("https://boardgamegeek.com/xmlapi2/thread",
                         params={"id": thread_id},
                         headers=headers)
        
        if r2.status_code == 200:
            root2 = ET.fromstring(r2.text)
            
            # Print raw XML snippet
            print("\nRaw XML (first 500 chars):")
            print(r2.text[:500])
            print("\n")
            
            thread_elem = root2.find('.//thread')
            
            if thread_elem is not None:
                articles = thread_elem.findall('.//article')
                print(f"✅ Found {len(articles)} articles in thread")
                
                if articles:
                    for i, article in enumerate(articles[:3], 1):
                        print(f"\nArticle {i}:")
                        print(f"  ID: {article.get('id')}")
                        print(f"  Author: {article.get('username')}")
                        print(f"  Date: {article.get('postdate')}")
                        
                        subject_elem = article.find('subject')
                        body_elem = article.find('body')
                        
                        subject = subject_elem.text if subject_elem is not None else None
                        body = body_elem.text if body_elem is not None else None
                        
                        print(f"  Subject: {subject}")
                        print(f"  Body: {body[:100] if body else 'None'}...")
                else:
                    print("❌ No articles found (empty list)")
            else:
                print("❌ No <thread> element found in XML")
        else:
            print(f"❌ HTTP {r2.status_code}")
            print(r2.text[:500])
    else:
        print("❌ No threads found")
else:
    print(f"❌ HTTP {r.status_code}")
    print(r.text[:500])

print(f"\n{'='*60}")
print("Diagnosis complete")
print(f"{'='*60}")