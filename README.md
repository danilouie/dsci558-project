# dsci558-project

Creating a KG for board games

## Files
*Each webscraping component is currently in their own branch.*
- `bgg_game_scraper.py`: scrapes game info from BoardGameGeek using XML and API token $\rightarrow$ outputs in `games.jsonl`

- `bgg_forums_scraper.py`: scrapes forum threads and articles from BoardGameGeek using XML and API $\rightarrow$ outputs in 5 files separated by forum category
  - `recommendations.jsonl`
  - `gaming-with-kids.jsonl`
  - `games-in-the-classroom.jsonl`
  - `hot-deals.jsonl`
  - `trades.jsonl`

- `bgo_download.py`: scrapes price history from BoardGameOracle $\rightarrow$ outputs in `bgo.zip`

- `bgq_scrapper.py`: scrapes reviews from BoardGameQuest $\rightarrow$ outputs in `reviews.jsonl`
    


