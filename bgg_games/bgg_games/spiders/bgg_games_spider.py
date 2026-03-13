"""
BoardGameGeek Game Info Scraper 
    
Uses Scrapy-Playwright to handle JavaScript-rendered content
"""

from bgg_games.items import BggGamesItem
import scrapy
import re

class BggGamesSpider(scrapy.Spider):
    name = "bgg_games"
    allowed_domains = ["boardgamegeek.com"]  
    
    def start_requests(self):
        # Start with Playwright for the browse page
        yield scrapy.Request(
            url="https://boardgamegeek.com/browse/boardgame",
            meta={"playwright": True},
            callback=self.parse
        )
    
    def parse(self, response):     
        # id attribute starts with 'row_'
        rows = response.css("tr[id^='row_']")
        
        for r in rows:
            item = BggGamesItem()

            game_url = r.css("div[id^='results_objectname'] a.primary::attr(href)").get()

            rank = r.css("td.collection_rank a::text").get()
            if rank:
                item["rank"] = rank.strip()

            ratings = r.css("td.collection_bggrating::text").getall()
            if len(ratings) >= 3:
                geek_rating = ratings[0].strip()
                if geek_rating:
                    item["geek_rating"] = geek_rating.strip()

                avg_rating = ratings[1].strip()
                if avg_rating:
                    item["avg_rating"] = avg_rating.strip()

                num_voters = ratings[2].strip()
                if num_voters:
                    item["num_voters"] = num_voters.strip()

            if game_url:
                yield response.follow(
                    game_url, 
                    callback=self.parse_game, 
                    meta={"item": item, "playwright": True}  
                )

        # crawling to next page
        next_page = response.css("a[title='next page']::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page, 
                callback=self.parse,
                meta={"playwright": True}  
            )

    def parse_game(self, response):
        item = response.meta["item"]

        item["bgg_id"] = response.url.split("/")[4]

        name = response.css("a[ui-sref='geekitem.overview'] span[itemprop='name']::text").get() 
        if name:
            item["name"] = name.strip()

        year = response.css("span.game-year.ng-binding::text").get()
        if year:
            item["year"] = year.strip()
            # remove parenthesis (ex: (2018) -> 2018)
            year_clean = re.sub(r'[^\d]', '', item["year"])
            item["year"] = int(year_clean)

        description = response.css("span[itemprop='description']::text").get()
        if description:
            item["description"] = description.strip()

        min_players = response.css("meta[itemprop='minValue']::attr(content)").get()
        if min_players:
            item["min_players"] = int(min_players)

        max_players = response.css("meta[itemprop='maxValue']::attr(content)").get()
        if max_players:
            item["max_players"] = int(max_players)
            
        best_num_players = response.css("span[item-poll-button='numplayers'] button span.ng-binding::text").getall()

        best_num_players = [text.strip() for text in best_num_players if text.strip()]

        best_range = None
        for text in best_num_players:
            if 'Best:' in text:
                best_range = text.replace('Best:', '').replace('—', '').strip()
                break

        if best_range:
            self.logger.info(f"DEBUG: Found best_range = '{best_range}'")
            
            # check for range (e.g., "3–4" or "3-4")
            has_range = False
            for dash in ['–', '-', '—', '−']:  # Try different dash types
                if dash in best_range:
                    best_parts = best_range.split(dash)
                    try:
                        item["best_min_players"] = int(best_parts[0].strip())
                        item["best_max_players"] = int(best_parts[1].strip())
                        has_range = True
                        self.logger.info(f"DEBUG: Range found - min: {item['best_min_players']}, max: {item['best_max_players']}")
                        break
                    except (ValueError, IndexError) as e:
                        self.logger.warning(f"Could not parse range '{best_range}': {e}")
                        continue
    
            # single number
            if not has_range:
                try:
                    best_val = int(best_range.strip())
                    item["best_min_players"] = best_val
                    item["best_max_players"] = best_val
                    self.logger.info(f"DEBUG: Single value found: {best_val}")
                except ValueError as e:
                    self.logger.warning(f"Could not parse single value '{best_range}': {e}")
                    item["best_min_players"] = None
                    item["best_max_players"] = None
        else:
            item["best_min_players"] = None
            item["best_max_players"] = None

        playtime = response.css("p.gameplay-item-primary span.ng-binding::text").getall()
        playtime_values = [val.strip() for val in playtime if val.strip().isdigit()]

        if len(playtime_values) >= 2:
            item["min_playtime"] = int(playtime_values[0])  
            item["max_playtime"] = int(playtime_values[1]) 
        elif len(playtime_values) == 1:
            item["min_playtime"] = int(playtime_values[0])
            item["max_playtime"] = int(playtime_values[0])

        min_age = response.css("span[itemprop='suggestedMinAge']::text").get()
        if min_age:
            item["min_age"] = int(min_age)

        # score is out of 5
        complexity = response.css("span[item-poll-button='boardgameweight'] span.ng-binding::text").get()
        if complexity:
            item["complexity"] = float(complexity.strip())

        credits_url = response.css("a[ui-sref='geekitem.credits']::attr(href)").get()
        if credits_url:
            yield response.follow(
                credits_url, 
                callback=self.parse_credits, 
                meta={"item": item, "playwright": True}  
            )
        else:
            yield item
    
    def parse_credits(self, response):
        item = response.meta["item"]
    
        categories = response.xpath("//span[@id='fullcredits-boardgamecategory']/ancestor::li//a[@class='ng-binding']/text()").getall()
        if categories:
            item["categories"] = [cat.strip() for cat in categories]
            
        # Extract mechanisms
        mechanisms = response.xpath("//span[@id='fullcredits-boardgamemechanic']/ancestor::li//a[@class='ng-binding']/text()").getall()
        if mechanisms:
            item["mechanisms"] = [mech.strip() for mech in mechanisms]
    
        yield item