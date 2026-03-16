"""
BoardGameGeek Scraper
Reads IDs from boardgames_ranks.csv, batch-fetches details from the XML API2,
and writes one JSON line per game to games.jsonl.
"""

import requests
import xml.etree.ElementTree as ET
import json
import time
import re
import csv
import argparse
from pathlib import Path

# Config
API_TOKEN   = "85cbfc9b-e2f9-4b82-b76c-1eaabb547586"

CSV_FILE    = Path(__file__).parent.parent / "boardgames_ranks.csv"
OUTPUT_FILE = Path(__file__).parent / "games.jsonl"
API_THING   = "https://boardgamegeek.com/xmlapi2/thing"

BATCH_SIZE  = 20
DELAY       = 2.0
RETRY_DELAY = 10.0
MAX_RETRIES = 5

# create authenticated session using API token
def get_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "bgg_games (Danielle Louie, louiedan@usc.edu)",
        "Authorization": f"Bearer {API_TOKEN}",
    })
    print("Session ready with API token.")
    return session

# loading csv
def load_csv(ranked_only=False, no_expansions=False):
    games = []
    with CSV_FILE.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if no_expansions and row.get("is_expansion", "0") == "1":
                continue
            rank = int(row["rank"]) if row["rank"].strip().isdigit() else None
            if ranked_only and rank is None:
                continue
            games.append({
                "bgg_id":              row["id"].strip(),
                "name":                row["name"].strip(),
                "year":                int(row["yearpublished"]) if row["yearpublished"].strip().lstrip("-").isdigit() else None,
                "rank":                rank,
                "geek_rating":         float(row["bayesaverage"]) if row["bayesaverage"].strip()        else None,
                "avg_rating":          float(row["average"])      if row["average"].strip()             else None,
                "num_voters":          int(row["usersrated"])      if row["usersrated"].strip().isdigit() else None,
                "is_expansion":        row.get("is_expansion",        "0") == "1",
                "abstracts_rank":      int(row["abstracts_rank"])      if row.get("abstracts_rank",      "").strip().isdigit() else None,
                "cgs_rank":            int(row["cgs_rank"])            if row.get("cgs_rank",            "").strip().isdigit() else None,
                "childrensgames_rank": int(row["childrensgames_rank"]) if row.get("childrensgames_rank", "").strip().isdigit() else None,
                "familygames_rank":    int(row["familygames_rank"])    if row.get("familygames_rank",    "").strip().isdigit() else None,
                "partygames_rank":     int(row["partygames_rank"])     if row.get("partygames_rank",     "").strip().isdigit() else None,
                "strategygames_rank":  int(row["strategygames_rank"])  if row.get("strategygames_rank",  "").strip().isdigit() else None,
                "thematic_rank":       int(row["thematic_rank"])       if row.get("thematic_rank",       "").strip().isdigit() else None,
                "wargames_rank":       int(row["wargames_rank"])       if row.get("wargames_rank",       "").strip().isdigit() else None,
            })
    return games

# fetch XML from API
def fetch_batch_xml(ids, session):
    params = {"id": ",".join(ids), "stats": 1, "type": "boardgame"}
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(API_THING, params=params, timeout=30)
        except requests.RequestException as e:
            print(f"[api] request error: {e} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
            continue

        if r.status_code == 200:
            return r.text
        elif r.status_code == 202:
            print(f"[api] 202 queued — waiting {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            print(f"[api] HTTP {r.status_code} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

    print(f"[api] gave up on batch starting with {ids[0]}")
    return None

# parsing XML
def parse_best_players(poll):
    best_counts = []
    for results in poll.findall("results"):
        num = results.get("numplayers", "")
        best_votes = 0
        for result in results.findall("result"):
            if result.get("value") == "Best":
                try:
                    best_votes = int(result.get("numvotes", 0))
                except ValueError:
                    pass
        if best_votes > 0:
            best_counts.append((num, best_votes))

    if not best_counts:
        return None, None

    max_votes = max(v for _, v in best_counts)
    winners = [n for n, v in best_counts if v == max_votes]
    nums = []
    for w in winners:
        try:
            nums.append(int(re.sub(r'[^\d]', '', w)))
        except ValueError:
            pass
    if not nums:
        return None, None
    return min(nums), max(nums)


def parse_item(item, csv_lookup):
    bgg_id = item.get("id")
    game = dict(csv_lookup.get(bgg_id, {"bgg_id": bgg_id}))

    name_el = item.find("name[@type='primary']")
    if name_el is not None:
        game["name"] = name_el.get("value", "").strip()

    if not game.get("year"):
        year_el = item.find("yearpublished")
        if year_el is not None:
            try:
                game["year"] = int(year_el.get("value", ""))
            except ValueError:
                pass

    desc_el = item.find("description")
    if desc_el is not None and desc_el.text:
        game["description"] = desc_el.text.strip()

    for field, tag in [("min_players", "minplayers"), ("max_players", "maxplayers")]:
        el = item.find(tag)
        if el is not None:
            try:
                game[field] = int(el.get("value", ""))
            except ValueError:
                pass

    poll = item.find("poll[@name='suggested_numplayers']")
    if poll is not None:
        game["best_min_players"], game["best_max_players"] = parse_best_players(poll)
    else:
        game["best_min_players"] = None
        game["best_max_players"] = None

    for field, tag in [("min_playtime", "minplaytime"), ("max_playtime", "maxplaytime")]:
        el = item.find(tag)
        if el is not None:
            try:
                game[field] = int(el.get("value", ""))
            except ValueError:
                pass

    age_el = item.find("minage")
    if age_el is not None:
        try:
            game["min_age"] = int(age_el.get("value", ""))
        except ValueError:
            pass

    ratings = item.find("statistics/ratings")
    if ratings is not None:
        el = ratings.find("averageweight")
        if el is not None:
            try:
                game["complexity"] = float(el.get("value", ""))
            except ValueError:
                pass

    game["categories"] = [
        l.get("value") for l in item.findall("link[@type='boardgamecategory']")
        if l.get("value")
    ]
    game["mechanisms"] = [
        l.get("value") for l in item.findall("link[@type='boardgamemechanic']")
        if l.get("value")
    ]

    return game

# main
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ranked-only",   action="store_true", help="Skip unranked games")
    parser.add_argument("--no-expansions", action="store_true", help="Skip expansions")
    args = parser.parse_args()

    session = get_session()

    print("\n=== Step 1: Loading CSV ===")
    games = load_csv(ranked_only=args.ranked_only, no_expansions=args.no_expansions)
    print(f"  {len(games)} games loaded from {CSV_FILE}")

    csv_lookup = {g["bgg_id"]: g for g in games}
    all_ids = [g["bgg_id"] for g in games]

    # Resume: skip IDs already written to output file
    done_ids = set()
    if OUTPUT_FILE.exists():
        with OUTPUT_FILE.open(encoding="utf-8") as f:
            for line in f:
                try:
                    done_ids.add(json.loads(line)["bgg_id"])
                except (json.JSONDecodeError, KeyError):
                    pass
        if done_ids:
            print(f"  Resuming — {len(done_ids)} already written, skipping them")
            all_ids = [i for i in all_ids if i not in done_ids]

    total_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n===Step 2: Fetching details ({len(all_ids)} games, {total_batches} batches)===")

    written = len(done_ids)
    with OUTPUT_FILE.open("a", encoding="utf-8") as f:
        for i in range(0, len(all_ids), BATCH_SIZE):
            batch = all_ids[i: i + BATCH_SIZE]
            xml_text = fetch_batch_xml(batch, session)

            if xml_text:
                try:
                    root = ET.fromstring(xml_text)
                    for item in root.findall("item"):
                        f.write(json.dumps(parse_item(item, csv_lookup), ensure_ascii=False) + "\n")
                        written += 1
                except ET.ParseError as e:
                    print(f"    [api] XML parse error: {e}")

            batch_num = i // BATCH_SIZE + 1
            if batch_num % 25 == 0 or batch_num == total_batches:
                print(f"  [api] batch {batch_num}/{total_batches} — {written} games written")

            time.sleep(DELAY)

    print(f"\nDone! {written} games written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()