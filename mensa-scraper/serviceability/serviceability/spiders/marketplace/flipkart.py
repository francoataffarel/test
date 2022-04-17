from ..base import MarketPlaceFactory
from ...items import ServiceabilityItem
from decouple import config
import logging
import calendar
from typing import List
from scrapy import Request, Selector
from ..utils import remove_all_query_params, replace_query_param, get_query_param
from ..utils import ChannelType
import re
import json
from scrapy.utils.project import get_project_settings

class FlipkartStyleList():
    
    def selectors(self, **kwargs) -> List[str]:
        return ["//div[string-length(@data-id) > 0]/div/a/@href"]

    def clean(self, selection, **kwargs) -> List[str]:
        fsn_product_href = re.compile(r"^/[a-zA-Z0-9-]*/p/itm[a-zA-Z0-9]*\?pid=[A-Z0-9]{16}&lid=[A-Z0-9]{25}")
        style_list = list(set(filter(fsn_product_href.match, selection.getall())))
        size = min(len(style_list),21)
        style_list = [f"https://www.flipkart.com{style_list[style]}" for style in range(size)]
        return style_list

class Flipkart(MarketPlaceFactory):
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
        settings=get_project_settings()
        self.api_url = settings.get('SELENIUM_LAMBDA_URL')

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Flipkart.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})
    
    def create_style_list(self) -> FlipkartStyleList:
        return FlipkartStyleList()

    def extract_style_list(self, response, **kwargs):
        selection = response
        for selector in self.create_style_list().selectors():
            selection = selection.selector.xpath(selector)

        style_list = self.create_style_list().clean(selection)
        for url in style_list:
            for pincode in self.pincodes:
                product_id = get_query_param(url, 'pid')
                url = remove_all_query_params(url)
                url = replace_query_param(url, 'pid', product_id)
                url = replace_query_param(url, 'pincode', pincode)
                yield self.request(url, self.extract_serviceability_list, pincode=pincode)        

    def start_request(self):
        self.print_config()
        for url in self.start_urls:
            yield(self.request(url, self.extract_style_list))
    
    def extract_serviceability_list(self, response, **kwargs):
        selection = response
        product_href= response.url
        self.logger.info(f'Fetching request for {product_href} and pincode: {kwargs["pincode"]}')
        if selection.selector.xpath('//div[@class="_16FRp0" and contains(text(),"Sold Out")]').get():
            self.logger.info(f'{product_href} is sold out')
            return            
        
        if selection.selector.xpath('//input[@class="cfnctZ"]').get() or selection.selector.xpath('//div[@class="_12cXX4"]').get():
            option = 'fashion'
        else:
            option = 'non-fashion'
        
        api_url = self.api_url
        api_url = replace_query_param(api_url,'marketplace', 'flipkart.com')
        api_url = replace_query_param(api_url,'project', 'serviceability')
        api_url = replace_query_param(api_url,'product_url', product_href)
        api_url = replace_query_param(api_url,'pincode', kwargs['pincode'])
        api_url = replace_query_param(api_url,'option', option)

        yield self.request(api_url, self.extract_serviceability_item)
        
    def extract_serviceability_item(self, response, **kwargs):
        response = json.loads(response.text)
        product_href = response['product_url']
        product_name = response['product_name']
        delivery_date = response['delivery_date']
        days_for_delivery = response['days_for_delivery']
        extracted_date = response['extracted_date']
        pincode = response['pincode']
        product_id=get_query_param(product_href, 'pid')
        self.logger.info(f'Processing for request: {product_id} and for pincode: {pincode}')
        for cb in self.competitor_of_brands:
            yield ServiceabilityItem(product_href=product_href, product_name=product_name, pincode=pincode, skuId=product_id,
                        extracted_date=extracted_date, delivery_date=delivery_date, 
                        days_for_delivery=days_for_delivery, brand=self.brand, 
                        competitor_of_brands=cb, marketplace=ChannelType.Flipkart)

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