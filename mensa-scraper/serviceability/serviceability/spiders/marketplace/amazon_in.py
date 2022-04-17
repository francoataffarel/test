from itertools import product
from math import pi
from ..base import MarketPlaceFactory
from ...items import ServiceabilityItem
from decouple import config
from typing import List
import logging
import json
from ..utils import ChannelType
from scrapy import Request, Selector
from ..utils import remove_all_query_params, replace_query_param, has_query_param, get_query_param
from scrapy.utils.project import get_project_settings
import random
import re

class AmazonInStyleList():
    
    def selectors(self, **kwargs) -> List[str]:
        return ["//span[@data-component-type='s-search-results']/div/div[string-length(@data-asin) > 0 and not(contains(@class, 'AdHolder'))]"]

    def clean(self, selection, **kwargs) -> List[dict]:
        asin_product_href = re.compile("^/[a-zA-Z0-9-_]*/dp/[A-Z0-9]{10}")
        result = []
        size = min(len(selection), 21)
        for i in range(size):
            item = Selector(text=selection[i].get())
            product_href = item.xpath('//h2/a/@href').get()
            if asin_product_href.match(product_href):
                product_href = f"https://www.amazon.in{product_href}"
                result.append(product_href)
        return result


class AmazonIn(MarketPlaceFactory):
    smart_proxy = None
    start_urls = None
    pincodes = None
    brand=None
    competitor_of_brands=None
    product_href = []

    def __init__(self, brand, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None
        self.start_urls = kwargs['start_urls']
        self.pincodes = kwargs['pincodes']
        self.brand = brand
        self.competitor_of_brands = kwargs['competitor_of_brands']
        settings=get_project_settings()
        self.api_url = settings.get('SELENIUM_LAMBDA_URL')
    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.AmazonIn.name)
        return logging.LoggerAdapter(logger, {'serviceability_spider': self})

    def create_style_list(self) -> AmazonInStyleList:
        return AmazonInStyleList()

    def request(self, url, callback, **kwargs):
        meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def extract_style_list(self, response, **kwargs):
        selection = response
        for selector in self.create_style_list().selectors():
            selection = selection.selector.xpath(selector)
        style_list = self.create_style_list().clean(selection)
        requests_url = []
        for url in style_list:
            for pincode in self.pincodes:
                new_url = remove_all_query_params(url)
                if has_query_param(url, 'th'):
                    new_url = replace_query_param(new_url, 'th', get_query_param(url, 'th'))
                if has_query_param(url, 'psc'):    
                    new_url = replace_query_param(new_url, 'psc', get_query_param(url, 'psc'))
                new_url = replace_query_param(new_url, 'pincode', pincode)
                requests_url.append(new_url)
        #To skip amazon bot requests_url, else same product will be called again and again
        random.shuffle(requests_url)
        for url in requests_url:
            yield self.request(url, self.extract_serviceability_list)

    def start_request(self):
        self.print_config()
        for url in self.start_urls:
            yield(self.request(url, self.extract_style_list))

    def extract_serviceability_list(self, response, **kwargs):
        selection = response
        product_href = response.url
        pincode = get_query_param(product_href, 'pincode')
        self.logger.info(f'Fetching request for {product_href} and pincode: {pincode}')
        if selection.selector.xpath('//div[@id="availability"]/span[1][contains(text(), "Currently unavailable.")]').get():
            return

        api_url = self.api_url
        print(api_url)
        api_url = replace_query_param(api_url,'marketplace', 'amazon.in')
        api_url = replace_query_param(api_url,'project', 'serviceability')
        api_url = replace_query_param(api_url,'product_url', product_href)
        api_url = replace_query_param(api_url,'pincode', pincode)
        yield self.request(api_url, self.extract_serviceability_item)

    def extract_serviceability_item(self, response, **kwargs):
        response = json.loads(response.text)
        product_href = response['product_url']
        product_name = response['product_name']
        delivery_date = response['delivery_date']
        days_for_delivery = response['days_for_delivery']
        extracted_date = response['extracted_date']
        pincode = response['pincode']
        product_id = re.match('(.*)/dp/([A-Z0-9]{10})', product_href).group(2)
        self.logger.info(f'Processing request for {product_href} and pincode: {pincode}')
        for cb in self.competitor_of_brands:
            yield ServiceabilityItem(product_href=product_href, product_name=product_name, pincode=pincode, skuId=product_id,
                        extracted_date=extracted_date, delivery_date=delivery_date, 
                        days_for_delivery=days_for_delivery, brand=self.brand, 
                        competitor_of_brands=cb, marketplace=ChannelType.AmazonIn)
    
    def request(self, url, callback, meta=None, dont_filter=False, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, dont_filter=dont_filter, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
        self.logger.info("start_urls: " + str(self.start_urls))
        self.logger.info("pincodes: " + str(self.pincodes))
        self.logger.info("brand: " + str(self.brand))
        self.logger.info("competitor_of_brands: " + str(self.competitor_of_brands))