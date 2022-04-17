from scrapy import Spider
import logging
from .marketplace import AmazonIn, Flipkart, AmazonCom, Myntra, Nykaa, NykaaFashion, AmazonAe
from .base import MarketPlaceFactory


class BrokennessSpider(Spider):
    allowed_domains = ['amazon.in', 'amazon.com', 'amazon.ae', 'flipkart.com', 'myntra.com', 'ajio.com', 'nykaa.com',
                       'nykaafashion.com']
    name = 'brokenness_spider'
    marketplace = None
    brand = None
    marketplace_cls: MarketPlaceFactory = None
    proxy = None
    scrape_limit = None

    @staticmethod
    def __get_marketplace_cls(brand, marketplace, **kwargs) -> MarketPlaceFactory:
        if 'amazon.in' == marketplace:
            return AmazonIn(brand, **kwargs)
        elif 'flipkart.com' == marketplace:
            return Flipkart(brand, **kwargs)
        elif 'amazon.com' == marketplace:
            return AmazonCom(brand, **kwargs)
        elif 'amazon.ae' == marketplace:
            return AmazonAe(brand, **kwargs)
        elif 'myntra.com' == marketplace:
            return Myntra(brand, **kwargs)
        elif 'nykaa.com' == marketplace:
            return Nykaa(brand, **kwargs)
        elif 'nykaafashion.com' == marketplace:
            return NykaaFashion(brand, **kwargs)

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        urls = kwargs.pop('start_urls', None)
        self.start_urls = urls.split(',') if urls else None
        if not self.start_urls:
            raise AttributeError("Crawling could not start: 'start_urls' not found ")
        if not getattr(self, 'brand', None):
            raise ValueError(f"{type(self).__name__} must have a brand")
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")
        if not getattr(self, 'scrape_limit', None):
            self.scrape_limit = 1000
        else:
            self.scrape_limit = int(self.scrape_limit)
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = self.proxy in ['true', 'True', '1']

        self.marketplace_cls = self.__get_marketplace_cls(brand=self.brand, marketplace=self.marketplace,
                                                          proxy=self.proxy, scrape_limit=self.scrape_limit)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'brokenness_spider': self})

    def start_requests(self):
        return self.marketplace_cls.start_requests(self.start_urls)

    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')
