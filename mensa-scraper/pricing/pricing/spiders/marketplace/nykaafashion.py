import logging
import json
from datetime import datetime
from typing import List

from decouple import config
from scrapy import Request

from ..base import MarketPlaceFactory
from ..utils import ChannelType, replace_query_param, get_query_param
from ...items import PricingItem

class NykaaFashion(MarketPlaceFactory):
    smart_proxy = None
    scrape_limit = None
    category = None

    def __init__(self, category, **kwargs):
        super().__init__()
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None
        self.category = category

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_list)

    def extract_list(self, response, **kwargs) -> List[str]:
        self.logger.info(f"Fetching style list from {response.url}")
        response_json = json.loads(response.text)['response']
        products = response_json['products']
        products = [product for product in products if 'id' in product]
        for product in products:
            yield self.request(f"https://www.nykaafashion.com{product['actionUrl']}", self.extract_style)
        
        count_product = len(products)
        total_products = response_json['count']
        page = int(get_query_param(response.url, 'currentPage'))
        if (page + 1) * count_product <= self.scrape_limit and page * count_product < total_products:
            url = replace_query_param(response.url, 'currentPage', page + 1)
            yield self.request(url, self.extract_list)


    def extract_style(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching style from {response.url}")
        selection = response.selector.xpath("//script[@id='__PRELOADED_STATE__']/text()").get()
        selection = json.loads(selection.encode('ascii', 'ignore'))
        product = selection['details']['skuData']['product']
        skuid = product['id']
        name = product['subTitle']
        brand = product['title']
        product_href = response.url
        mrp = product['price']
        price = product['discountedPrice']
        discount = mrp - price
        discount_percentage = product['discount']
        mrp, price, discount, discount_percentage = str(mrp), str(price), str(discount), str(discount_percentage)
        available = product['isOutOfStock'] is 0
        extracted_date = datetime.today().strftime('%Y%m%d')
        
        yield PricingItem(skuid=skuid, category=self.category, brand=brand,
            title=name, product_href=product_href, mrp=mrp, price=price, discount=discount, 
            discount_percentage=discount_percentage, bank_offers="NA", coupon_offers="NA",
            extracted_date=extracted_date, available=available, marketplace=ChannelType.NykaaFashion)

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.NykaaFashion.name)
        return logging.LoggerAdapter(logger, {'pricing_spider': self})

    def request(self, url, callback, meta=None, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Category: " + self.category)
        self.logger.info("scrape_limit: " + str(self.scrape_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))