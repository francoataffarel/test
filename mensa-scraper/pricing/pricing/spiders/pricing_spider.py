import logging
from scrapy import Spider
from .base import MarketPlaceFactory
from .marketplace import Myntra, Flipkart, AmazonIn, AmazonCom, Nykaa, NykaaFashion, Ajio

class PricingSpider(Spider):
    allowed_domains = ['amazon.in', 'amazon.com', 'amazon.ae', 'amazon.ca',
                       'flipkart.com', 'myntra.com', 'ajio.com', 'nykaa.com', 'nykaafashion.com']
    name = 'pricing_spider'
    marketplace = None
    proxy = None
    marketplace_cls: MarketPlaceFactory = None
    scrape_limit=None
    category=None

    @staticmethod
    def __get_marketplace_cls(marketplace, category, **kwargs) -> MarketPlaceFactory:
        if 'myntra.com' == marketplace:
            return Myntra(category=category, **kwargs)
        elif "flipkart.com" == marketplace:
            return Flipkart(category=category, **kwargs)
        elif "amazon.in" == marketplace:
            return AmazonIn(category=category, **kwargs)
        elif "amazon.com" == marketplace:
            return AmazonCom(category=category, **kwargs)            
        elif "nykaafashion.com" == marketplace:
            return NykaaFashion(category=category, **kwargs)
        elif "nykaa.com" == marketplace:
            return Nykaa(category=category, **kwargs)
        elif "ajio.com" == marketplace:
            return Ajio(category=category, **kwargs)            

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        urls = kwargs.pop('start_urls', None)
        self.start_urls = urls.split(',') if urls else None

        if not self.start_urls:
            raise AttributeError("Crawling could not start: 'start_urls' not found ")
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")            
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = self.proxy in ['true', 'True', '1']
        if not getattr(self, 'category', None):
            raise ValueError(f"{type(self).__name__} must have a category")
        if not getattr(self, 'scrape_limit', None):
            self.scrape_limit = 300
        else:
            self.scrape_limit = int(self.scrape_limit)            
        
        self.marketplace_cls = self.__get_marketplace_cls(self.marketplace,
                                    start_urls=self.start_urls,
                                    proxy=self.proxy,
                                    category=self.category,
                                    scrape_limit=self.scrape_limit)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})

    def start_requests(self):
        return self.marketplace_cls.start_requests(self.start_urls)
    
    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')                