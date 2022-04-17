import logging
from scrapy import Spider
from .base import disc_marketplace_factory
from .utilities import SortOption


class Discoverability(Spider):
    allowed_domains = ['amazon.in', 'amazon.com', 'flipkart.com', 'myntra.com', 'ajio.com']
    name = 'discoverability_spider'
    marketplace = None
    marketplace_object = None
    keyword = None
    proxy = None
    sort = None
    scraping_items_limit = 200

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        if not getattr(self, 'keyword', None):
            raise ValueError(f"{type(self).__name__} must have a search keyword")
        else:
            self.keyword = kwargs.pop('keyword', None)
        if not getattr(self, 'marketplace', None):
            raise ValueError(f"{type(self).__name__} must have a marketplace")
        if not getattr(self, 'proxy', None):
            self.proxy = True
        else:
            self.proxy = self.proxy in ['true', 'True', '1']
        if not getattr(self, 'scraping_items_limit', None):
            self.scraping_items_limit = 200
        else:
            self.scraping_items_limit = int(self.scraping_items_limit)
        if not getattr(self, 'sort', None):
            self.sort = SortOption.relevance
        else:
            self.sort = SortOption[self.sort]
        marketplace = kwargs.pop('marketplace', None)
        if marketplace not in self.allowed_domains:
            raise ValueError(
                f"{type(self).__name__} must have a one of following marketplaces: {str(self.allowed_domains)}")
        else:
            self.marketplace_object = disc_marketplace_factory(domain=marketplace, proxy=self.proxy,
                                                               scraping_items_limit=self.scraping_items_limit)

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'discoverability_spider': self})

    def start_requests(self):
        self.marketplace_object.print_config()
        return self.marketplace_object.start_requests(nav_type=self.name, keyword=self.keyword, sort=self.sort)

    def parse(self, response, **kwargs):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')
