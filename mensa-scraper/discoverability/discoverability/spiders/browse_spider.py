import logging
from scrapy import Spider
from .base import disc_marketplace_factory


class Browse(Spider):
    allowed_domains = ['myntra.com', 'ajio.com']
    name = 'browse_spider'
    marketplace = None
    marketplace_object = None
    proxy = None
    scraping_items_limit = 200

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        if not getattr(self, 'primary_nav_url', None):
            raise ValueError(f"{type(self).__name__} must have a browse url: primary_nav_url")
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")
        if not getattr(self, 'offer_type', None):
            raise ValueError(f"{type(self).__name__} must have a offer_type")
        if not getattr(self, 'offer_title', None):
            raise ValueError(f"{type(self).__name__} must have a offer_title")
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = kwargs.pop('proxy', None) in ['true', 'True', '1']
        if not getattr(self, 'primary_nav_has_brand_name', None):
            self.primary_nav_has_brand_name = False
        else:
            self.primary_nav_has_brand_name = kwargs.pop('primary_nav_has_brand_name', None) in ['true', 'True', '1']
        if not getattr(self, 'secondary_nav_has_brand_name', None):
            self.secondary_nav_has_brand_name = False
        else:
            self.secondary_nav_has_brand_name = kwargs.pop('secondary_nav_has_brand_name', None) in ['true', 'True', '1']
        if not getattr(self, 'scraping_items_limit', None):
            self.scraping_items_limit = 200
        else:
            self.scraping_items_limit = int(kwargs.pop('scraping_items_limit', 200))
        marketplace = kwargs.pop('marketplace', None)
        if marketplace not in self.allowed_domains:
            raise ValueError(
                f"{type(self).__name__} must have a one of following marketplaces: {str(self.allowed_domains)}")
        else:
            self.marketplace_object = disc_marketplace_factory(domain=marketplace, proxy=self.proxy,
                                                               scraping_items_limit=self.scraping_items_limit,
                                                               primary_nav_has_brand_name=self.primary_nav_has_brand_name,
                                                               secondary_nav_has_brand_name=self.secondary_nav_has_brand_name,
                                                               **kwargs)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'browse_spider': self})

    def start_requests(self):
        self.marketplace_object.print_config()
        return self.marketplace_object.start_requests(nav_type=self.name)

    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')
