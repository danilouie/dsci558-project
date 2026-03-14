# dsci558-project
Creating a KG for board games

## Files

### `bgg_games`
Spider for crawling https://boardgamegeek.com/browse/boardgame/ to obtain board game information (output: `games_info.jsonl`).
- From project root: `cd bgg_games && scrapy crawl bgg_games -o games_info.jsonl`

### `bgq_reviews`
Spider for scraping **all game reviews** from https://www.boardgamequest.com/ (Game Reviews + Digital Board Game Reviews). Outputs one JSON record per review with title, game name, author, date, score, body, hits, and misses.
- **Setup:** `pip install -r bgq_reviews/requirements.txt`
- **Run (from project root):**  
  `cd bgq_reviews && scrapy crawl bgq_reviews -o reviews.jsonl`  
  Or to a directory:  
  `cd bgq_reviews && scrapy crawl bgq_reviews -o ../data/bgq_reviews.jsonl`
- The crawler follows category pagination and respects `robots.txt` with polite delays.
