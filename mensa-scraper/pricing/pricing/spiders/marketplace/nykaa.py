import logging
import json
from datetime import datetime
from typing import List

from decouple import config
from scrapy import Request

from ..base import MarketPlaceFactory
from ..utils import ChannelType, replace_query_param
from ...items import PricingItem

class Nykaa(MarketPlaceFactory):
    smart_proxy = None
    scrape_limit = None
    category = None

    def __init__(self, category, **kwargs):
        super().__init__()
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None
        self.category = category

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Nykaa.name)
        return logging.LoggerAdapter(logger, {'pricing_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_list)
    
    def extract_list(self, response, **kwargs) -> List[str]:
        self.logger.info(f"Fetching style list from {response.url}")
        product_json = json.loads(response.text)
        products = product_json['response']['products']
        for product in products:
            url = f'https://www.nykaa.com/{product["slug"]}'
            yield self.request(url, self.extract_style)

        total_products = product_json['response']['total_found']
        offset = product_json['response']['offset']
        count_products = len(products)
        if count_products + offset <= self.scrape_limit and count_products + offset < total_products:
            url = replace_query_param(response.url, 'from', count_products + offset)
            yield self.request(url, self.extract_list)
        
    def extract_style(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching style from {response.url}")
        script_text = response.selector.xpath("//script[contains(text(),'window.__PRELOADED_STATE__')]/text()").get()
        script_text = script_text[len("window.__PRELOADED_STATE__ = "):]
        selector = json.loads(script_text)
        product_page = selector['productPage']['product']
        brand = product_page['brandName']
        skuid = product_page['id']
        mrp = product_page['mrp']
        price = product_page['offerPrice']
        discount = mrp - price
        discount_percentage = product_page['discount']
        mrp, price, discount, discount_percentage = str(mrp), str(price), str(discount), str(discount_percentage)
        available = product_page['inStock']
        extracted_date = datetime.today().strftime('%Y%m%d')
        product_href = response.url
        name = product_page['name']
        if len(product_page['offers']) >= 1:
            bank_offers_text = ','.join([offer['description'] for offer in product_page['offers']])
        else:
            bank_offers_text = "NA"    
        yield PricingItem(skuid=skuid, category=self.category, brand=brand,
            title=name, product_href=product_href, mrp=mrp, price=price, discount=discount, 
            discount_percentage=discount_percentage, bank_offers=bank_offers_text, coupon_offers="NA",
            extracted_date=extracted_date, available=available, marketplace=ChannelType.Nykaa)        
        

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