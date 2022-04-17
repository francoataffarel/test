import logging
from .base import MarketPlaceFactory
from .marketplace import Myntra, Flipkart, AmazonIn, Ajio, NykaaFashion, Nykaa
from scrapy import Spider


class ServiceabilitySpider(Spider):
    allowed_domains = ['amazon.in', 'flipkart.com', 'myntra.com', 'ajio.com', 'nykaafashion.com', "nykaa.com", "amazonaws.com"]
    name = 'serviceability_spider'
    marketplace = None
    proxy = None
    brand = None
    marketplace_cls = None
    competitor_of_brands= None   

    @staticmethod
    def __get_marketplace_cls(marketplace, brand, **kwargs) -> MarketPlaceFactory:
        if 'myntra.com' == marketplace:
            return Myntra(brand=brand, **kwargs)
        elif "flipkart.com" == marketplace:
            return Flipkart(brand=brand, **kwargs)
        elif "amazon.in" == marketplace:
            return AmazonIn(brand=brand, **kwargs)
        elif "ajio.com" == marketplace:
            return Ajio(brand=brand, **kwargs)
        elif "nykaafashion.com" == marketplace:
            return NykaaFashion(brand=brand, **kwargs)
        elif "nykaa.com" == marketplace:
            return Nykaa(brand=brand, **kwargs)

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        urls = kwargs.pop('start_urls', None)
        self.start_urls = urls.split(',') if urls else None
        pincodes = kwargs.pop('pincodes', None)
        self.pincodes = pincodes.split(',') if pincodes else None
        competitor_of_brands = kwargs.pop('competitor_of_brands', None)
        self.competitor_of_brands = competitor_of_brands.split(',') if competitor_of_brands else [ "NA" ]

        if not self.start_urls:
            raise AttributeError("Crawling could not start: 'start_urls' not found ")
        if not self.pincodes:
            raise AttributeError("Crawling could not start: 'pincodes' not found ")            
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")            
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = self.proxy in ['true', 'True', '1']
        if not getattr(self, 'brand', None):
            raise ValueError(f"{type(self).__name__} must have a brand")
        
        self.marketplace_cls = self.__get_marketplace_cls(self.marketplace, self.brand, 
                                    start_urls=self.start_urls, 
                                    pincodes=self.pincodes,
                                    proxy=self.proxy,
                                    competitor_of_brands=self.competitor_of_brands)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})

    def start_requests(self):
        return self.marketplace_cls.start_request()
    
    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')   