import json
from ..base import MarketPlaceFactory
from ...items import ServiceabilityItem
from decouple import config
import logging
from datetime import datetime, time
from ..utils import ChannelType
from scrapy import Request
from ..utils import get_query_param
from typing import List
import re
import calendar

class NykaaStyleList():
    
    def clean(self, selection, **kwargs) -> List[str]:
        size = min(len(selection['response']['products']), 31)
        return [selection['response']['products'][product]['product_url'] for product in range(size) if 'product_url' in selection['response']['products'][product]]

def get_delivery_date(message):
    # message = by 11th Jan
    message = message.split(" ")
    day = int(re.sub(r'\D', '', message[1]).strip())
    month = message[2]
    month = list(calendar.month_abbr).index(month)
    if datetime.today().month == 12 and month != datetime.today().month:
        #next year is the Delivery
        return datetime(day=day, month=month, year=datetime.today().year+1)
    else:
        return datetime(day=day, month=month, year=datetime.today().year)

class Nykaa(MarketPlaceFactory):
    smart_proxy = None
    start_urls = None
    pincodes = None
    brand=None
    competitor_of_brands=None
    
    def __init__(self, brand, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None
        self.start_urls = kwargs['start_urls']
        self.pincodes = kwargs['pincodes']
        self.brand = brand
        self.competitor_of_brands = kwargs['competitor_of_brands']   

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Nykaa.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})

    def create_style_list(self) -> NykaaStyleList:
        return NykaaStyleList()

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        style_list = self.create_style_list().clean(selection)
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        for url in style_list:
            yield self.request(url, self.extract_serviceability_list)

    def start_request(self):
        self.print_config()
        for url in self.start_urls:
            yield self.request(url, self.extract_style_list)

    
    def extract_serviceability_list(self, response, **kwargs):
        selection = response
        script_text = selection.selector.xpath("//script[contains(text(),'window.__PRELOADED_STATE__')]/text()").get()
        script_text = script_text[len("window.__PRELOADED_STATE__ = "):]
        product_json = json.loads(script_text)
        product_id = product_json["productPage"]["product"]["id"]
        product_href = response.url
        product_name = product_json['productPage']['product']['name']
        for pincode in self.pincodes:
            pid = product_id
            self.logger.info(f'Fetching for request: {product_href} and for pincode: {pincode}')
            api_url = f'https://www.nykaa.com/custom/index/enterpincode?pincode={pincode}&prod_id={pid}'
            yield self.request(api_url, self.extract_serviceability_item, product_href=product_href, product_name=product_name)

    def extract_serviceability_item(self, response, **kwargs):
        selection = json.loads(response.text)
        pincode = get_query_param(response.url, 'pincode')
        product_id = get_query_param(response.url, 'prod_id')
        self.logger.info(f'Processing for request: {kwargs["product_href"]} and for pincode: {pincode}')
        message = selection['message']
        message_status = selection['message_status']
        if selection['status'] == 1 and message_status != 'Dispatch':
            delivery_date = datetime.combine(get_delivery_date(message), time.min)
            extracted_date = datetime.combine(datetime.today(), time.min)
            days_for_delivery = delivery_date - extracted_date
            delivery_date = delivery_date.strftime('%Y%m%d')
            extracted_date = extracted_date.strftime('%Y%m%d')
            for cb in self.competitor_of_brands:
                yield ServiceabilityItem(product_href=kwargs['product_href'], 
                        product_name=kwargs['product_name'], pincode=pincode, skuId=product_id,
                        extracted_date=extracted_date, delivery_date=delivery_date, 
                        days_for_delivery=days_for_delivery.days, brand=self.brand, 
                        competitor_of_brands=cb, marketplace=ChannelType.Nykaa)

    def request(self, url, callback, meta=None, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
        self.logger.info("start_urls: " + str(self.start_urls))
        self.logger.info("pincodes: " + str(self.pincodes))
        self.logger.info("brand: " + str(self.brand))
        self.logger.info("competitor_of_brands: " + str(self.competitor_of_brands))
