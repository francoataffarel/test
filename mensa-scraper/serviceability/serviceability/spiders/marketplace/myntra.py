from http import cookies
import logging
import json
from typing import List
from ..base import MarketPlaceFactory
from decouple import config
from scrapy import Request
from datetime import datetime, time
from ...items import ServiceabilityItem
from ..utils import ChannelType

class MyntraStyleList():
    
    def clean(self, selection, **kwargs) -> List[str]:
        size = min(len(selection["products"]),31)
        style_list = [f"https://www.myntra.com/gateway/v2/product/{selection['products'][product]['productId']}" for product in
                      range(size)]
        return style_list

class Myntra(MarketPlaceFactory):
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

    def create_style_list(self) -> MyntraStyleList:
        return MyntraStyleList()

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Myntra.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        style_list = self.create_style_list().clean(selection)

        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        for url in style_list:
            yield self.request(url, self.extract_serviceability_list, product_href=kwargs['product_href'])
            
    def start_request(self):
        self.print_config()
        for url in self.start_urls:
            yield self.request(url, self.start_request_cookies_wrapper, product_href=url)

    def start_request_cookies_wrapper(self, response, **kwargs):
        product_id = response.url.split('/')[-1]
        api_start_urls = f'https://www.myntra.com/gateway/v2/search/{product_id}?p=1&rows=50&o=0&plaEnabled=false'
        yield self.request(api_start_urls, self.extract_style_list,product_href=kwargs["product_href"])

    def extract_serviceability_list(self, response, **kwargs):
        selection = json.loads(response.text)
        request_body = {}
        request_body['paymentMode'] = 'ALL'
        request_body['serviceType'] = 'FORWARD'
        request_body['shippingMethod'] = 'NORMAL'
        flags = selection['style']['flags']
        item = {}
        for key, value in flags.items():
            item[key] = value

        sizes_list = selection['style']['sizes']
        available_size = None
        for size in sizes_list:
            #get the first available size
            if size['available']:
                available_size = size
                break
        
        if available_size is not None:
            item['itemReferenceId'] = str(available_size['sizeSellerData'][0]['sellerPartnerId'])
            item['itemValue'] = available_size['sizeSellerData'][0]['discountedPrice']
            item['procurementTimeInDays'] = available_size['sizeSellerData'][0]['procurementTimeInDays']
            item['availableInWarehouses'] = available_size['sizeSellerData'][0]['warehouses']
            item['launchDate'] = selection['style']['serviceability']['launchDate']
            item['skuId'] = str(available_size['skuId'])
            item['articleType'] = selection['style']['analytics']['articleType']
            request_body['items'] = [item]
            
            for pincode in self.pincodes:
                request_body['pincode'] = pincode
                yield self.post_api_request('https://www.myntra.com/gateway/v2/serviceability/check', 
                                self.extract_serviceability_item, body=json.dumps(request_body), 
                                product_href=kwargs['product_href'], product_name=selection['style']['name'])
        
    def extract_serviceability_item(self, response, **kwargs):
        selection = json.loads(response.text)
        self.logger.info(f'Fetching from this request: {kwargs["product_href"]} for pincode: {selection["pincode"]}')
        if len(selection['itemServiceabilityEntries']) > 0:
            if len(selection['itemServiceabilityEntries'][0]['serviceabilityEntries']) > 0:
                date_epoch = selection['itemServiceabilityEntries'][0]['serviceabilityEntries'][0]['promiseDate']
                skuId=selection['itemServiceabilityEntries'][0]['skuId']
                pincode=selection['pincode']
                delivery_date=datetime.fromtimestamp(date_epoch/1000.0).strftime('%Y%m%d')
                extracted_date=datetime.today().strftime('%Y%m%d')
                delivery_date_datetime = datetime.fromtimestamp(date_epoch/1000.0)
                days_for_delivery = datetime.combine(delivery_date_datetime.date(), time.min) - datetime.combine(datetime.today().date(), time.min)
                for cb in self.competitor_of_brands:
                    yield ServiceabilityItem(product_href=kwargs['product_href'], product_name=kwargs['product_name'],
                                    pincode=pincode, skuId=skuId,
                                    extracted_date=extracted_date, delivery_date=delivery_date, 
                                    days_for_delivery=days_for_delivery.days, brand=self.brand, 
                                    competitor_of_brands=cb, marketplace=ChannelType.Myntra)

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