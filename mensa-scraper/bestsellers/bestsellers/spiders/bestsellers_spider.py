import scrapy
import logging
from .marketplace import AmazonIn, AmazonCom, AmazonAe, Myntra, Flipkart, Nykaa, NykaaFashion, Ajio


class Bestsellers(scrapy.Spider):
    name = 'bestsellers_spider'
    allowed_domains = ['amazon.in', 'amazon.com']
    brand = None
    marketplace = None
    marketplace_object = None
    proxy = None
    sku_ids = None

    @staticmethod
    def __marketplace_factory(domain, **kwargs):
        marketplace = {
            "amazon.in": AmazonIn,
            "amazon.com": AmazonCom,
            "amazon.ae": AmazonAe,
            "myntra.com": Myntra,
            "flipkart.com": Flipkart,
            "nykaa.com": Nykaa,
            "nykaafashion.com": NykaaFashion,
            "ajio.com": Ajio,
        }
        return marketplace[domain](**kwargs)

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        sku_ids = kwargs.pop('sku_ids', None)
        self.sku_ids = sku_ids.split(',') if sku_ids else None
        if not self.sku_ids:
            raise AttributeError("Crawling could not start: 'sku_ids' not found ")
        if not getattr(self, 'brand', None):
            raise ValueError(f"{type(self).__name__} must have a brand")
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = self.proxy in ['true', 'True', '1']
        marketplace = kwargs.pop('marketplace', None)
        if marketplace not in self.allowed_domains:
            raise ValueError(
                f"{type(self).__name__} must have a one of following marketplaces: {str(self.allowed_domains)}")
        else:
            self.marketplace_object = self.__marketplace_factory(domain=marketplace, brand=self.brand, proxy=self.proxy)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'bestsellers_spider': self})

    def start_requests(self):
        self.marketplace_object.print_config()
        return self.marketplace_object.start_requests(sku_ids=self.sku_ids)

    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')
