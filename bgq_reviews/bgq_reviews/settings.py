# Scrapy settings for bgq_reviews project
# https://docs.scrapy.org/en/latest/topics/settings.html

BOT_NAME = "bgq_reviews"

SPIDER_MODULES = ["bgq_reviews.spiders"]
NEWSPIDER_MODULE = "bgq_reviews.spiders"

# Crawl responsibly: identify bot and obey robots.txt
USER_AGENT = "bgq_reviews (+https://github.com/dsci558-project; educational scraping)"
ROBOTSTXT_OBEY = True

# Polite crawling: limit concurrency and add delay
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle to adapt to server response
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2
AUTOTHROTTLE_MAX_DELAY = 8
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# Export encoding
FEED_EXPORT_ENCODING = "utf-8"

# Item pipelines
ITEM_PIPELINES = {
    "bgq_reviews.pipelines.BgqReviewsPipeline": 300,
}
