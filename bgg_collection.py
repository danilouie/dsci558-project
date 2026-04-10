"""
BoardGameGeek Collection Fetcher — Multi-User
Reads usernames from users.txt (one per line), shuffles the list, then
fetches each user's BGG collection via the XML API2. Skips any user whose
output file already exists (resume-friendly). Writes one JSON line per item
to <username>_collection.jsonl.

Usage examples:
    python bgg_collection.py
    python bgg_collection.py --users-file other_users.txt --own 1 --stats
    python bgg_collection.py --wishlist 1 --no-expansions
    python bgg_collection.py --minrating 7 --stats
"""

import requests
import xml.etree.ElementTree as ET
import json
import time
import argparse
from pathlib import Path
# ── Config ──────────────────────────────────────────────────────────────────
API_TOKEN   = "83bdd67a-5016-4155-940a-b3ee4e9d73c1"

OUTPUT_DIR     = Path(__file__).parent
USERS_FILE     = Path(__file__).parent / "users.txt"
API_COLLECTION = "https://boardgamegeek.com/xmlapi2/collection"

RETRY_DELAY = 5.0
MAX_RETRIES = 8   # BGG often returns 202 for collection; retry generously


# ── Session ──────────────────────────────────────────────────────────────────
def get_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "bgg_collection_fetcher",
        "Authorization": f"Bearer {API_TOKEN}",
    })
    return session


# ── Fetch ────────────────────────────────────────────────────────────────────
def fetch_collection_xml(params: dict, session: requests.Session) -> str | None:
    """
    Hits the /collection endpoint with retry logic.
    BGG queues large collections and returns 202 until ready.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(API_COLLECTION, params=params, timeout=30)
        except requests.RequestException as e:
            print(f"  [net] request error: {e} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
            continue

        if r.status_code == 200:
            return r.text
        elif r.status_code == 202:
            wait = min(15, RETRY_DELAY + attempt * 2)
            print(f"  [api] 202 — BGG is preparing the collection (attempt {attempt}/{MAX_RETRIES}), waiting {wait}s...")
            time.sleep(wait)
        else:
            print(f"  [api] HTTP {r.status_code} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

    print("[api] Gave up waiting for collection response.")
    return None


# ── Parse ────────────────────────────────────────────────────────────────────
def _int(val: str | None) -> int | None:
    try:
        return int(val) if val is not None else None
    except ValueError:
        return None

def _float(val: str | None) -> float | None:
    try:
        return float(val) if val is not None else None
    except ValueError:
        return None

def _text(el, path: str) -> str | None:
    node = el.find(path)
    return node.text.strip() if node is not None and node.text else None


def parse_item(item: ET.Element) -> dict:
    game: dict = {}

    game["bgg_id"]      = item.get("objectid")
    game["subtype"]     = item.get("subtype")
    game["collid"]      = item.get("collid")

    # Primary name
    name_el = item.find("name")
    if name_el is not None:
        game["name"]      = name_el.text.strip() if name_el.text else None
        game["sort_index"] = _int(name_el.get("sortindex"))

    game["year"]        = _int(_text(item, "yearpublished"))
    game["image"]       = _text(item, "image")
    game["thumbnail"]   = _text(item, "thumbnail")

    # User status flags
    status = item.find("status")
    if status is not None:
        game["own"]          = status.get("own")
        game["prev_owned"]   = status.get("prevowned")
        game["for_trade"]    = status.get("fortrade")
        game["want"]         = status.get("want")
        game["want_to_play"] = status.get("wanttoplay")
        game["want_to_buy"]  = status.get("wanttobuy")
        game["wishlist"]     = status.get("wishlist")
        game["wishlist_priority"] = status.get("wishlistpriority")
        game["preordered"]   = status.get("preordered")
        game["last_modified"]= status.get("lastmodified")

    # Plays
    game["num_plays"] = _int(_text(item, "numplays"))

    # Comment / private info
    game["comment"]       = _text(item, "comment")
    game["private_comment"] = _text(item, "privatecomment")

    # Stats block (only present when stats=1)
    stats = item.find("stats")
    if stats is not None:
        game["min_players"]  = _int(stats.get("minplayers"))
        game["max_players"]  = _int(stats.get("maxplayers"))
        game["min_playtime"] = _int(stats.get("minplaytime"))
        game["max_playtime"] = _int(stats.get("maxplaytime"))
        game["num_owned"]    = _int(stats.get("numowned"))

        ratings = stats.find("rating")
        if ratings is not None:
            game["user_rating"]    = _float(ratings.get("value")) if ratings.get("value") not in (None, "N/A") else None

            avg = ratings.find("average")
            game["avg_rating"]     = _float(avg.get("value"))    if avg is not None else None

            bay = ratings.find("bayesaverage")
            game["geek_rating"]    = _float(bay.get("value"))    if bay is not None else None

            stddev = ratings.find("stddev")
            game["rating_stddev"]  = _float(stddev.get("value")) if stddev is not None else None

            median = ratings.find("median")
            game["rating_median"]  = _float(median.get("value")) if median is not None else None

            # Sub-type ranks (e.g. overall, strategy, thematic …)
            ranks = []
            for rank_el in ratings.findall("ranks/rank"):
                ranks.append({
                    "type":       rank_el.get("type"),
                    "name":       rank_el.get("name"),
                    "friendly_name": rank_el.get("friendlyname"),
                    "value":      _int(rank_el.get("value")) if rank_el.get("value") not in (None, "Not Ranked") else None,
                    "bayes_avg":  _float(rank_el.get("bayesaverage")),
                })
            game["ranks"] = ranks

    return game


# ── Helpers ──────────────────────────────────────────────────────────────────
def load_usernames(users_file: Path) -> list[str]:
    """Read usernames from a text file, one per line. Ignores blank lines and # comments."""
    if not users_file.exists():
        raise FileNotFoundError(f"Users file not found: {users_file}")
    names = []
    with users_file.open(encoding="utf-8") as f:
        for line in f:
            name = line.strip()
            if name and not name.startswith("#"):
                names.append(name)
    return names


# ── Main ─────────────────────────────────────────────────────────────────────
def build_params(username: str, args) -> dict:
    """Map CLI args to BGG API query parameters for a given username."""
    p: dict = {"username": username}

    # Numeric / flag filters — only add when explicitly set
    flag_map = {
        "own":          "own",
        "rated":        "rated",
        "played":       "played",
        "comment":      "comment",
        "trade":        "trade",
        "want":         "want",
        "wishlist":     "wishlist",
        "preordered":   "preordered",
        "wanttoplay":   "wanttoplay",
        "wanttobuy":    "wanttobuy",
        "prevowned":    "prevowned",
        "hasparts":     "hasparts",
        "wantparts":    "wantparts",
    }
    for attr, param in flag_map.items():
        val = getattr(args, attr, None)
        if val is not None:
            p[param] = val

    optional_map = {
        "wishlist_priority": "wishlistpriority",
        "id":                "id",
        "minrating":         "minrating",
        "rating":            "rating",
        "minbggrating":      "minbggrating",
        "bggrating":         "bggrating",
        "minplays":          "minplays",
        "maxplays":          "maxplays",
        "modifiedsince":     "modifiedsince",
        "collid":            "collid",
    }
    for attr, param in optional_map.items():
        val = getattr(args, attr, None)
        if val is not None:
            p[param] = val

    if args.no_expansions:
        p["subtype"]        = "boardgame"
        p["excludesubtype"] = "boardgameexpansion"
    elif args.expansions_only:
        p["subtype"] = "boardgameexpansion"

    if args.stats:
        p["stats"] = 1

    if args.brief:
        p["brief"] = 1

    return p


def main():
    parser = argparse.ArgumentParser(description="Fetch BGG collections for multiple users")

    # Users file
    parser.add_argument("--users-file", default="users.txt",
                        help=f"Path to text file with one BGG username per line (default: users.txt)")

    # Output directory
    parser.add_argument("--output-dir", default="user",
                        help="Directory for output .jsonl files (default: same folder as script)")

    # API flags
    parser.add_argument("--stats",           action="store_true", help="Include rating/ranking stats")
    parser.add_argument("--brief",           action="store_true", help="Abbreviated results")
    parser.add_argument("--no-expansions",   action="store_true", help="Exclude expansions")
    parser.add_argument("--expansions-only", action="store_true", help="Only expansions")

    # 0/1 filter flags
    for flag in ["own", "rated", "played", "comment", "trade", "want",
                 "wishlist", "preordered", "wanttoplay", "wanttobuy",
                 "prevowned", "hasparts", "wantparts"]:
        parser.add_argument(f"--{flag}", type=int, choices=[0, 1], default=None)

    # Value filters
    parser.add_argument("--wishlist-priority", type=int, choices=range(1, 6), dest="wishlist_priority")
    parser.add_argument("--id",            help="Comma-separated BGG item IDs to filter")
    parser.add_argument("--minrating",     type=float)
    parser.add_argument("--rating",        type=float, help="Max personal rating")
    parser.add_argument("--minbggrating",  type=float)
    parser.add_argument("--bggrating",     type=float, help="Max BGG rating")
    parser.add_argument("--minplays",      type=int)
    parser.add_argument("--maxplays",      type=int)
    parser.add_argument("--modifiedsince", help="YY-MM-DD or 'YY-MM-DD HH:MM:SS'")
    parser.add_argument("--collid",        help="Restrict to a specific collection id")

    args = parser.parse_args()

    users_file = Path(args.users_file) if args.users_file else USERS_FILE
    out_dir    = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Load & shuffle usernames ──────────────────────────────────────────────
    print(f"\n=== Loading usernames from {users_file} ===")
    try:
        usernames = load_usernames(users_file)
    except FileNotFoundError as e:
        print(e)
        return

    print(f"  {len(usernames)} usernames found")
    # ── Skip users whose output file already exists ───────────────────────────
    pending = []
    skipped = []
    for username in usernames:
        out_file = out_dir / f"{username}_collection.jsonl"
        if out_file.exists():
            skipped.append(username)
        else:
            pending.append(username)

    if skipped:
        print(f"  Skipping {len(skipped)} already-fetched user(s): {', '.join(skipped)}")
    print(f"  {len(pending)} user(s) to fetch\n")

    if not pending:
        print("Nothing to do — all users already have output files.")
        return

    # ── Fetch ─────────────────────────────────────────────────────────────────
    session = get_session()

    for idx, username in enumerate(pending, 1):
        out_file = out_dir / f"{username}_collection.jsonl"
        params   = build_params(username, args)

        print(f"[{idx}/{len(pending)}] Fetching collection for '{username}' → {out_file.name}")

        xml_text = fetch_collection_xml(params, session)
        if not xml_text:
            print(f"  [!] No data received for '{username}', skipping.")
            continue

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            print(f"  [!] XML parse error for '{username}': {e}, skipping.")
            continue

        errors = root.find("errors/error/message")
        if errors is not None:
            print(f"  [!] BGG error for '{username}': {errors.text}, skipping.")
            continue

        items = root.findall("item")
        print(f"  {len(items)} items returned")

        written = 0
        with out_file.open("w", encoding="utf-8") as f:
            for item in items:
                f.write(json.dumps(parse_item(item), ensure_ascii=False) + "\n")
                written += 1

        print(f"  ✓ {written} items written to {out_file.name}")

        # Polite delay between users (skip after last one)
        if idx < len(pending):
            time.sleep(1.0)
    
    print(f"\n=== All done! {len(pending)} user(s) processed. ===")


if __name__ == "__main__":
    main()