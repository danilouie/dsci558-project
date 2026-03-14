"""
Board Game Quest (boardgamequest.com) game review scraper.

Discovers review URLs from category listing pages (game-reviews and
digital-board-game-reviews), then scrapes each review page for title,
author, date, score, body, hits, and misses.
"""

import json
import re
import scrapy
from bgq_reviews.items import BgqReviewItem


def _is_review_url(url: str) -> bool:
    """True if URL looks like a single review article (not category/author/feed)."""
    url = url.split("?")[0].split("#")[0].rstrip("/")
    if "/category/" in url or "/author/" in url or "/feed" in url:
        return False
    if "/page/" in url:
        return False
    # Review posts typically have -review in the path
    return "-review" in url or url.endswith("/review")


def _game_name_from_title(title: str) -> str:
    """e.g. 'Shallow Regrets Review' -> 'Shallow Regrets'."""
    if not title:
        return ""
    for suffix in (" Review", " Digital Review", " Preview"):
        if title.endswith(suffix):
            return title[: -len(suffix)].strip()
    return title.strip()


# Single regex for section headers (case-insensitive); only one named group matches per match
_SECTION_REGEX = re.compile(
    r"(?P<gameplay_overview>Gameplay\s+Overview\s*:)"
    r"|(?P<game_experience>Game(?:play)?\s+Experience\s*:)"
    r"|(?P<final_thoughts>Final\s+Thoughts\s*:)"
    r"|(?P<final_score>Final\s+Score\s*:)",
    re.IGNORECASE,
)


def _parse_body_sections(body: str) -> dict:
    """
    Split body into intro, gameplay_overview, game_experience, final_thoughts,
    and extract final_score line + description. Returns a dict with keys
    intro, gameplay_overview, game_experience, final_thoughts, final_score_line,
    final_score_description.
    """
    result = {
        "intro": "",
        "gameplay_overview": "",
        "game_experience": "",
        "final_thoughts": "",
        "final_score_line": "",
        "final_score_description": "",
    }
    if not (body or "").strip():
        return result

    last_end = 0
    last_section = "intro"
    intro_parts = []

    for m in _SECTION_REGEX.finditer(body):
        start, end = m.start(), m.end()
        chunk = body[last_end:start].strip()
        if last_section == "intro":
            intro_parts.append(chunk)
        else:
            existing = result.get(last_section) or ""
            result[last_section] = (existing + "\n\n" + chunk).strip() if existing else chunk

        if m.lastgroup == "gameplay_overview":
            last_section = "gameplay_overview"
        elif m.lastgroup == "game_experience":
            last_section = "game_experience"
        elif m.lastgroup == "final_thoughts":
            last_section = "final_thoughts"
        elif m.lastgroup == "final_score":
            # Everything from "Final Score:" until "Hits:" or end is the final score block
            rest = body[end:].strip()
            hits_pos = re.search(r"\n\s*Hits\s*:", rest, re.IGNORECASE)
            end_final = hits_pos.start() if hits_pos else len(rest)
            final_block = rest[:end_final].strip()
            result["final_score_line"] = ("Final Score: " + final_block).strip()
            # Description is the part after "X stars - " or "X Stars - "
            desc_m = re.search(
                r"[\d.]+(?:\s*[sS]tars?)?\s*[-–—]\s*(.+)",
                final_block,
                re.DOTALL,
            )
            result["final_score_description"] = desc_m.group(1).strip() if desc_m else ""
            break
        last_end = end

    result["intro"] = "\n\n".join(intro_parts).strip()
    return result


class BgqReviewsSpider(scrapy.Spider):
    name = "bgq_reviews"
    allowed_domains = ["boardgamequest.com", "www.boardgamequest.com"]

    # Games-by-rating has hundreds of review links in one page; category pages add more + pagination
    start_urls = [
        "https://www.boardgamequest.com/games-by-rating/",
        "https://www.boardgamequest.com/category/game-reviews/",
        "https://www.boardgamequest.com/category/digital-board-game-reviews/",
    ]

    def parse(self, response):
        # Which category we're in (for tagging)
        if "digital-board-game-reviews" in response.url:
            category = "digital-board-game-reviews"
        elif "games-by-rating" in response.url:
            category = "game-reviews"  # page mixes board + digital; tag as game-reviews
        else:
            category = "game-reviews"

        # Collect review article URLs from this page (all links that look like review posts)
        seen = set()
        for href in response.css("a[href*='boardgamequest.com']::attr(href)").getall():
            raw = response.urljoin(href).split("#")[0]
            # Normalize to trailing slash so server serves real page (avoids broken meta-refresh)
            if "?" in raw:
                base, qs = raw.split("?", 1)
                full_url = base.rstrip("/") + "/?" + qs
            else:
                full_url = raw.rstrip("/") + "/"
            if full_url in seen:
                continue
            if not _is_review_url(full_url):
                continue
            seen.add(full_url)
            yield scrapy.Request(
                full_url,
                callback=self.parse_review,
                meta={"category": category},
            )

        # Pagination: follow rel="next" (WordPress category pages)
        next_url = response.css("link[rel='next']::attr(href)").get()
        if next_url:
            yield response.follow(next_url, callback=self.parse)
        elif seen and re.search(r"/category/[^/]+/page/(\d+)/?$", response.url):
            # Fallback: we found links on this page but no rel=next; try next page number
            match = re.search(r"/category/[^/]+/page/(\d+)/?$", response.url)
            if match:
                n = int(match.group(1))
                base = re.sub(r"/page/\d+/?$", "", response.url).rstrip("/")
                yield response.follow(f"{base}/page/{n + 1}/", callback=self.parse)

    def parse_review(self, response):
        category = response.meta.get("category", "game-reviews")
        item = BgqReviewItem()
        url = response.url.split("?")[0].split("#")[0]
        item["url"] = url.rstrip("/") + "/"  # canonical form with trailing slash
        item["category"] = category

        # Try to get structured data from JSON-LD (Review schema)
        body_text = None
        body_from_json = False
        for script in response.css("script[type='application/ld+json']::text").getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    data = [data]
                for node in data if isinstance(data, list) else [data]:
                    if node.get("@type") == "Review" and "reviewBody" in node:
                        body_text = node.get("reviewBody") or ""
                        if body_text:
                            body_text = body_text.replace("\\r\\n", "\n").replace("\r\n", "\n")
                            body_from_json = True
                        author = node.get("author")
                        if isinstance(author, dict):
                            item["author"] = author.get("name", "").strip()
                            item["author_url"] = author.get("sameAs") or ""
                        elif isinstance(author, str):
                            item["author"] = author.strip()
                            item["author_url"] = ""
                        date_pub = node.get("datePublished") or ""
                        if date_pub:
                            item["published_date"] = date_pub
                        rating = node.get("reviewRating") or node.get("ratingValue")
                        if isinstance(rating, dict) and "ratingValue" in rating:
                            item["score"] = str(rating.get("ratingValue", ""))
                        elif rating is not None:
                            item["score"] = str(rating)
                        break
            except (json.JSONDecodeError, TypeError):
                continue

        # Title from HTML (and game name)
        title = response.css("h1.entry-title::text").get()
        if title:
            title = title.strip()
        item["title"] = title or ""
        item["game_name"] = _game_name_from_title(item["title"])

        # If we didn't get body from JSON-LD, use entry-content
        if not body_from_json or not body_text:
            body_sel = response.css(".entry-content, .post-content, article .entry-content")
            body_text = " ".join(body_sel.css("::text").getall()) if body_sel else ""
            body_text = " ".join(body_text.split())

        # Keep full body unchanged so you can re-run parsing later if needed
        item["body"] = (body_text or "").strip()

        # Author/date fallback from HTML
        if not item.get("author"):
            author_sel = response.css(".author a::text, a[rel='author']::text, .posted-by a::text")
            item["author"] = (author_sel.get() or "").strip()
        if not item.get("author_url"):
            item["author_url"] = response.css(".author a::attr(href), a[rel='author']::attr(href)").get() or ""
        if not item.get("published_date"):
            item["published_date"] = (
                response.css("time::attr(datetime)").get()
                or response.css("time::text").get()
                or ""
            )
            if isinstance(item["published_date"], str):
                item["published_date"] = item["published_date"].strip()

        # Score / Final Score line from body if not from JSON-LD
        if not item.get("score") and item["body"]:
            m = re.search(
                r"Final Score:\s*([^\n]+?)(?:\s*Hits:|\s*$)",
                item["body"],
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                item["score_raw"] = ("Final Score: " + m.group(1).strip()).strip()
                # Normalize to number or "X stars"
                raw = m.group(1).strip()
                item["score"] = raw.split("–")[0].split("-")[0].strip() if raw else ""
            else:
                item["score_raw"] = ""
                item["score"] = ""
        else:
            item["score_raw"] = ("Final Score: " + str(item.get("score", ""))) if item.get("score") else ""

        # Hits and Misses from body (bullet lists after "Hits:" / "Misses:")
        item["hits"] = []
        item["misses"] = []
        if item["body"]:
            hits_m = re.search(
                r"Hits:\s*(.*?)(?=Misses:|$)",
                item["body"],
                re.IGNORECASE | re.DOTALL,
            )
            if hits_m:
                block = hits_m.group(1).strip()
                item["hits"] = [
                    s.strip().strip("•").strip()
                    for s in re.split(r"[\n•]", block)
                    if s.strip()
                ]
            misses_m = re.search(
                r"Misses:\s*(.*?)(?=RELATED|$)",
                item["body"],
                re.IGNORECASE | re.DOTALL,
            )
            if misses_m:
                block = misses_m.group(1).strip()
                item["misses"] = [
                    s.strip().strip("•").strip()
                    for s in re.split(r"[\n•]", block)
                    if s.strip()
                ]

        # Parse body into structured sections; on failure keep body and leave structured fields empty
        item["intro"] = ""
        item["gameplay_overview"] = ""
        item["game_experience"] = ""
        item["final_thoughts"] = ""
        item["final_score_description"] = ""
        try:
            sections = _parse_body_sections(item["body"] or "")
            item["intro"] = sections.get("intro") or ""
            item["gameplay_overview"] = sections.get("gameplay_overview") or ""
            item["game_experience"] = sections.get("game_experience") or ""
            item["final_thoughts"] = sections.get("final_thoughts") or ""
            item["final_score_description"] = sections.get("final_score_description") or ""
            if sections.get("final_score_line"):
                item["score_raw"] = sections["final_score_line"]
        except Exception:
            # Body is already set; structured fields stay empty so you can parse later
            pass

        yield item
