import scrapy

class GutenbergSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["gutenberg.org"]
    start_urls = [
        "https://www.gutenberg.org/ebooks/search/?sort_order=downloads"
    ]

    def parse(self, response):
        # Mỗi book trong thẻ li.booklink
        for book in response.css("li.booklink"):
            title = book.css("span.title::text").get()
            author = book.css("span.subtitle::text").get()
            downloads_text = book.css("span.extra::text").get()
            
            # Chuyển "12345 downloads" -> số nguyên
            downloads = None
            if downloads_text:
                try:
                    downloads = int(downloads_text.strip().split()[0].replace(',', ''))
                except:
                    downloads = None

            yield {
                "title": title.strip() if title else None,
                "author": author.strip() if author else None,
                "downloads": downloads,
            }

        # Phân trang
        next_page = response.css("a[title='Go to the next page of results.']::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
