# Board Game Quest (BGQ) review scraper

Scrapes all game reviews from [Board Game Quest](https://www.boardgamequest.com/):

- **Game Reviews** (`/category/game-reviews/`)
- **Digital Board Game Reviews** (`/category/digital-board-game-reviews/`)

## Output (JSONL)

Each line is one review with:

| Field | Description |
|-------|-------------|
| `url` | Canonical review URL |
| `title` | e.g. "Shallow Regrets Review" |
| `game_name` | e.g. "Shallow Regrets" |
| `author` | Reviewer name |
| `author_url` | Link to author page |
| `published_date` | ISO or raw date string |
| `score` | Numeric score, e.g. "2" or "4.5" |
| `score_raw` | Full line, e.g. "Final Score: 3.5 Stars - ..." |
| `final_score_description` | Subtitle after "X stars - " (one-line summary) |
| `intro` | Text before the first section header |
| `gameplay_overview` | "Gameplay Overview:" section body |
| `game_experience` | "Game Experience:" / "Gameplay Experience:" section body |
| `final_thoughts` | "Final Thoughts:" section body |
| `hits` | List of pros (bullet points) |
| `misses` | List of cons (bullet points) |
| `body` | Full review text (unchanged) |
| `category` | "game-reviews" or "digital-board-game-reviews" |

## Run

```bash
# From repo root
pip install -r bgq_reviews/requirements.txt
cd bgq_reviews
scrapy crawl bgq_reviews -o reviews.jsonl
```

Crawl is polite (delay, concurrency limits, obeys `robots.txt`). To limit how many review pages are fetched (e.g. for testing), use:

```bash
scrapy crawl bgq_reviews -o reviews.jsonl -s CLOSESPIDER_ITEMCOUNT=50
```
