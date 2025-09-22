import scrapy
import csv

class GoldSpider(scrapy.Spider):
    name = "gold"
    start_urls = ["https://datahub.io/core/gold-prices/r/monthly.csv"]

    def parse(self, response):
        lines = response.text.splitlines()
        reader = csv.DictReader(lines)

        for row in reader:
            yield {
                "date": row["Date"],
                "price_usd": row["Price"]
            }
