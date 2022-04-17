import inspect
import logging

from scrapy import Spider
from scrapy.utils.misc import walk_modules

from .brand import Brand
from .utils import ScrapeMode


class RnRSpider(Spider):
    allowed_domains = ['amazon.in', 'amazon.com', 'amazon.ae', 'amazon.ca',
                       'flipkart.com', 'myntra.com', 'ajio.com', 'nykaa.com', 'nykaafashion.com']
    name = 'rnr_spider'
    marketplace = None
    brand = None
    brand_cls = None
    mode = None
    proxy = None
    review_capture_duration = 15
    scrape_reviews = None
    scrape_products = None
    category = None

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        urls = kwargs.pop('start_urls', None)
        self.start_urls = urls.split(',') if urls else None
        if not self.start_urls:
            raise AttributeError("Crawling could not start: 'start_urls' not found ")
        if not getattr(self, 'category', None):
            raise ValueError(f"{type(self).__name__} must have a category")
        if not getattr(self, 'brand', None):
            raise ValueError(f"{type(self).__name__} must have a brand")
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")
        if not getattr(self, 'mode', None):
            self.mode = ScrapeMode.Full
        else:
            self.mode = ScrapeMode(int(self.mode))
        if not getattr(self, 'review_capture_duration', None):
            self.review_capture_duration = 15
        else:
            self.review_capture_duration = int(self.review_capture_duration)
        if not getattr(self, 'scrape_reviews', None):
            self.scrape_reviews = True
        else:
            self.scrape_reviews = self.scrape_reviews in ['true', 'True', '1']
        if not getattr(self, 'scrape_products', None):
            self.scrape_products = True
        else:
            self.scrape_products = self.scrape_products in ['true', 'True', '1']
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = self.proxy in ['true', 'True', '1']

        self.brand_cls = Brand(category=self.category,brand=self.brand, marketplace=self.marketplace, mode=self.mode,
                               review_capture_duration=self.review_capture_duration,
                               proxy=self.proxy, scrape_reviews=self.scrape_reviews,
                               scrape_products=self.scrape_products)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'rnr_spider': self})

    def start_requests(self):
        start_urls = self.start_urls if self.mode is ScrapeMode.Full else self.start_urls[:1]
        return self.brand_cls.marketplace.start_request(start_urls)

    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')
