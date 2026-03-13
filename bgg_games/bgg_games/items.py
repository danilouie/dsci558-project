# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BggGamesItem(scrapy.Item):
    bgg_id = scrapy.Field()
    rank = scrapy.Field()
    name = scrapy.Field()
    year = scrapy.Field()
    description = scrapy.Field()
    min_players = scrapy.Field()
    max_players = scrapy.Field()
    best_min_players = scrapy.Field()
    best_max_players = scrapy.Field()
    min_playtime = scrapy.Field()
    max_playtime = scrapy.Field()
    min_age = scrapy.Field()
    complexity = scrapy.Field()
    geek_rating = scrapy.Field()
    avg_rating = scrapy.Field()
    num_voters = scrapy.Field() 
    categories = scrapy.Field()
    mechanisms = scrapy.Field()
    pass
