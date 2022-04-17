import json
from ..base import MarketPlaceFactory
from ...items import ServiceabilityItem
from decouple import config
import logging
from datetime import datetime, time, tzinfo
from ..utils import ChannelType
from typing import List
from scrapy import Request
import calendar
import re

class NykaaFashionStyleList():

    def clean(self, selection, **kwargs) -> List[str]:
        size = min(len(selection['response']['products']),31)
        return [f"https://www.nykaafashion.com{selection['response']['products'][product]['actionUrl']}"
                for product in range(size) if 'actionUrl' in selection['response']['products'][product]]

def get_delivery_date(delivery_date_text) -> datetime:
    CLEANR = re.compile('<.*?>')
    delivery_date_text = re.sub(CLEANR, '', delivery_date_text)
    _, date_text = delivery_date_text.split(",")
    date_text = date_text.strip()
    day, month = date_text.split(" ")
    day = int(re.sub(r'\D', '', day))
    if month in list(calendar.month_name):
        month = list(calendar.month_name).index(month)
    else:
        month = list(calendar.month_abbr).index(month)
    if datetime.today().month == 12 and month != datetime.today().month:
        return datetime(day=day, month=month, year=datetime.today().year+1)
    else:
        return datetime(day=day, month=month, year=datetime.today().year)
    
class NykaaFashion(MarketPlaceFactory):
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
        logger = logging.getLogger(ChannelType.NykaaFashion.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})
    
    def create_style_list(self) -> NykaaFashionStyleList:
        return NykaaFashionStyleList()

    def start_request(self):
        self.print_config()
        for url in self.start_urls:
            yield self.request(url, self.extract_style_list)  

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        style_list = self.create_style_list().clean(selection)
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        for url in style_list:
            yield self.request(url, self.extract_serviceability_list) 
    
    def extract_serviceability_list(self, response, **kwargs):
        selection = response
        product_info = json.loads(selection.selector.xpath("//script[@id='__PRELOADED_STATE__']/text()").get())
        product_details = product_info["details"]["skuData"]["product"]
        delivery_date_text = product_details["deliveryDetails"]["text"]
        product_name = f'{product_details["title"]} {product_details["subTitle"]}'
        sku = product_details["sku"]
        delivery_date = get_delivery_date(delivery_date_text).strftime('%Y%m%d')
        for pincode in self.pincodes:
            request_payload = {
                'pincode': pincode
            }
            yield self.post_api_request(url=f'https://www.nykaafashion.com/rest/appapi/V1/service/pincode?sku={sku}', 
                callback=self.extract_serviceability_item, 
                body=json.dumps(request_payload),
                delivery_date=delivery_date, product_name=product_name, product_href=response.url,
                pincode=pincode, sku=sku)

    def extract_serviceability_item(self, response, **kwargs):
        selection = json.loads(response.text)
        isServiceable = selection['response']['isServicable']
        if isServiceable == 1:
            self.logger.info(f'Fetching from this request: {kwargs["product_href"]} for pincode: {kwargs["pincode"]}')
            delivery_date = datetime.combine(datetime.strptime(kwargs['delivery_date'], '%Y%m%d'), time.min)
            extracted_date = datetime.combine(datetime.today(), time.min)
            days_for_delivery = delivery_date - extracted_date
            delivery_date = delivery_date.strftime('%Y%m%d')
            extracted_date = extracted_date.strftime('%Y%m%d')
            for cb in self.competitor_of_brands:
                yield ServiceabilityItem(product_href=kwargs['product_href'], 
                            product_name=kwargs['product_name'], pincode=kwargs['pincode'], 
                            skuId=kwargs['sku'],
                            extracted_date=extracted_date, delivery_date=delivery_date, 
                            days_for_delivery=days_for_delivery.days, brand=self.brand, 
                            competitor_of_brands=cb, marketplace=ChannelType.NykaaFashion)            


    def post_api_request(self, url, callback, meta=None, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, method='POST', body=kwargs['body'], meta=meta, cb_kwargs=kwargs)
    
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
