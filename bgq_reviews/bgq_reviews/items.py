# Define here the models for your scraped items
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BgqReviewItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()           # e.g. "Shallow Regrets Review"
    game_name = scrapy.Field()       # e.g. "Shallow Regrets" (derived from title)
    author = scrapy.Field()
    author_url = scrapy.Field()
    published_date = scrapy.Field()  # ISO or raw string
    score = scrapy.Field()           # e.g. "2" or "4.5"
    score_raw = scrapy.Field()       # full line e.g. "Final Score: 2 stars"
    final_score_description = scrapy.Field()  # text after "X stars - " (subtitle)
    hits = scrapy.Field()            # list of hit bullets
    misses = scrapy.Field()          # list of miss bullets
    # Structured sections (parsed from body)
    intro = scrapy.Field()           # text before first section header
    gameplay_overview = scrapy.Field()
    game_experience = scrapy.Field()  # "Game Experience" or "Gameplay Experience"
    final_thoughts = scrapy.Field()
    body = scrapy.Field()            # full review text, always kept raw (parse later if needed)
    category = scrapy.Field()        # "game-reviews" or "digital-board-game-reviews"
