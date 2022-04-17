import json
from ..base import MarketPlaceFactory
from ...items import ServiceabilityItem
from decouple import config
import logging
from datetime import datetime, time
from ..utils import ChannelType
from typing import List
from scrapy import Request

class Ajio(MarketPlaceFactory):
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
        logger = logging.getLogger(ChannelType.Ajio.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})

    def start_request(self):
        self.print_config()
        for url in self.start_urls:
            pid = url.split('/')[-1]
            api_url = f'https://www.ajio.com/api/p/{pid}'
            yield self.request(api_url, self.extract_serviceability_list, product_href=url)

    def extract_serviceability_list(self, response, **kwargs):
        selection = json.loads(response.text)
        variantOptions = selection['variantOptions']
        product_name = selection['name']
        product_name = product_name.strip()
        available_variant = None
        for variant in variantOptions:
            if variant['stock']['stockLevelStatus'] != 'outOfStock':
                available_variant = variant
        
        if available_variant:
            variantCode = available_variant['code']
            for pincode in self.pincodes:
                url = f'https://login.web.ajio.com/api/edd/checkDeliveryDetails?productCode={variantCode}&postalCode={pincode}&quantity=1&IsExchange=false'
                yield self.request(url, self.extract_serviceability_item, product_href=kwargs['product_href'], product_name=product_name)
    
    def extract_serviceability_item(self, response, **kwargs):
        selection = json.loads(response.text)
        serviceability = selection['servicability']
        products = selection['productDetails']
        if serviceability and len(products) > 0:
            pincode = selection['pinCode']
            product_id = products[0]['productCode']
            self.logger.info(f'Fetching from this request: {product_id} for pincode: {pincode}')
            delivery_date_text = products[0]['eddUpper']
            delivery_date = datetime.strptime(delivery_date_text, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
            extracted_date = datetime.combine(datetime.today(), time.min)
            days_for_delivery = delivery_date - extracted_date
            delivery_date = delivery_date.strftime('%Y%m%d')
            extracted_date = extracted_date.strftime('%Y%m%d')
            for cb in self.competitor_of_brands:
                yield ServiceabilityItem(product_href=kwargs['product_href'], 
                            product_name=kwargs['product_name'], pincode=pincode, skuId=product_id,
                            extracted_date=extracted_date, delivery_date=delivery_date, 
                            days_for_delivery=days_for_delivery.days, brand=self.brand, 
                            competitor_of_brands=cb, marketplace=ChannelType.Ajio)


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

