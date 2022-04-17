import logging
import json
from datetime import datetime
from typing import List

from decouple import config
from scrapy import Request

from ..base import MarketPlaceFactory
from ..utils import ChannelType, replace_query_param, get_query_param, has_query_param, remove_all_query_params
from ...items import PricingItem

class Myntra(MarketPlaceFactory):
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
        logger = logging.getLogger(ChannelType.Myntra.name)
        return logging.LoggerAdapter(logger, {'pricing_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.start_request_cookies_wrapper)

    def start_request_cookies_wrapper(self, response, **kwargs):
        filter_path = None
        if has_query_param(response.url, 'f'):
            filter_path = f'&f={get_query_param(response.url, "f")}'
        category_path = remove_all_query_params(response.url).split('/')[-1]
        url = f'https://www.myntra.com/gateway/v2/search/{category_path}?p=1&rows=50&o=0&plaEnabled=false'
        if filter_path:
            url = url + filter_path
        yield self.request(url, self.extract_list)

    def extract_list(self, response, **kwargs) -> List[str]:
        selection = json.loads(response.text)
        style_list = [f"https://www.myntra.com/gateway/v2/product/{product['productId']}"
                      for product in selection["products"]]
        
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        for url in style_list:
            yield self.request(url, self.extract_style)

        total_items = selection['totalCount']
        url = response.url
        page = int(get_query_param(url, 'p'))
        
        if total_items and page * 50 < total_items and page * 50 <= self.scrape_limit:
            url = replace_query_param(url, 'p', page + 1)
            url = replace_query_param(url, 'o', (page * 50) - 1)
            yield self.request(url, self.extract_list)

    def extract_style(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching style from {response.url}")
        product = json.loads(response.text)['style']
        title = product['name']
        mrp = int(product['mrp'])
        price = min(map(lambda item: item['sizeSellerData'][0]['discountedPrice'] if item['available'] and 
                len(item['sizeSellerData']) > 0 else mrp, product['sizes']))
        skuid = str(product['id'])
        discount = int(mrp) - int(price)
        discount_percentage = (int(mrp) - int(price))*100 // int(mrp)
        mrp, price, discount, discount_percentage = str(mrp), str(price), str(discount), str(discount_percentage)
        category = self.category
        brand = product['analytics']['brand']
        available = not product['flags']['outOfStock']
        product_href = f"https://www.myntra.com/{product['id']}"
        extracted_date = datetime.today().strftime('%Y%m%d')

        seller_id = product['buyButtonSellerOrder'][0]['sellerPartnerId'] if len(product['buyButtonSellerOrder']) > 0 else None
        if not seller_id:
            #item must not be available, hence no offers
            yield PricingItem(skuid=skuid, category=category, title=title, product_href=product_href, 
                mrp=mrp, price=price, discount=discount, discount_percentage=discount_percentage, 
                bank_offers="NA", coupon_offers="NA", brand=brand,
                extracted_date=extracted_date, available=available, marketplace=ChannelType.Myntra)
        else:
            pricing_item = PricingItem(skuid=skuid, category=category, title=title, product_href=product_href, 
                    mrp=mrp, price=price, discount=discount, discount_percentage=discount_percentage,
                    brand=brand, extracted_date=extracted_date, available=available, marketplace=ChannelType.Myntra)
            yield self.request(f"https://www.myntra.com/gateway/v2/product/{product['id']}/offers/{seller_id}", 
                    self.get_offers, 
                    pricing_item=pricing_item)
    
    def get_offers(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching offers from {response.url}")
        product = json.loads(response.text)['bestPrice']
        pricing_item = kwargs['pricing_item']
        if product['couponCode']:
            coupon_offer_text = f"{product['couponCode']}-{product['applicableOn']}-{product['couponDiscount']}"
            pricing_item['bank_offers']="NA"
            pricing_item['coupon_offers']=coupon_offer_text
        else:
            pricing_item['bank_offers']="NA"
            pricing_item['coupon_offers']="NA"
        yield pricing_item

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


