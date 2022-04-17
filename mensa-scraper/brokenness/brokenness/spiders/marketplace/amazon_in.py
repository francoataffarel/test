import logging
import re
from datetime import datetime
from typing import List
from urllib.parse import urlparse, parse_qsl

from decouple import config
from scrapy import Request

from ..base import MarketPlaceFactory
from ..uitilies import ChannelType, replace_query_param
from ...items import BrokennessItem


class AmazonIn(MarketPlaceFactory):
    brand = None
    smart_proxy = None
    scrape_limit = None
    marketplace_url = None
    channel = None

    def __init__(self, brand, **kwargs):
        super().__init__()
        self.brand = brand
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None
        self.marketplace_url = 'https://www.amazon.in'
        self.channel = ChannelType.AmazonIn

    @property
    def logger(self):
        logger = logging.getLogger(self.channel.value)
        return logging.LoggerAdapter(logger, {'brokenness_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_list)

    def extract_list(self, response, **kwargs):
        selection = response.selector.xpath("//span[@data-component-type='s-search-results']"
                                            "/div/div[string-length(@data-asin) > 0]//h2/a/@href")
        asin_product_href = re.compile("^/[a-zA-Z0-9-_]*/dp/[A-Z0-9]{10}")
        style_list = [style.split('?', 1)[0] for style in selection.getall()]
        style_list = list(set(filter(asin_product_href.match, style_list)))
        style_list = [f"{self.marketplace_url}{style}" for style in style_list]
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")

        for url in style_list:
            yield self.request(url, self.extract_parent_style)

        next_page = response.selector.css('ul.a-pagination').css('li.a-last').xpath('a/@href').get()
        if next_page is not None:
            url = f"{self.marketplace_url}{next_page}"
            page = int(dict(parse_qsl(urlparse(url)[4])).get('page', 1))
            if page <= self.scrape_limit / 48:
                yield self.request(url, self.extract_list)

    def extract_parent_style(self, response, **kwargs):
        self.logger.info(f"Fetching patent style from {response.url}")
        product_href = response.url
        product_name = response.selector.xpath("//span[contains(concat(' ',normalize-space(@class),' '),"
                                               "' product-title-word-break ')]//text()").get().strip()
        parent_asin = re.match('(.*)/dp/([A-Z0-9]{10})', response.url).group(2)
        out_of_stock = response.selector.xpath("//div[@id='outOfStock']").get()
        extracted_date = datetime.today().strftime('%Y%m%d')

        size_selection = response.selector.xpath("//select[@id='native_dropdown_selected_size_name']"
                                                 "/option[not(@id ='native_size_name_-1')]")
        variant_selection = response.selector.xpath("//div[contains(@id,'variation_')]//li")

        ####
        # Case 1: The given product has both size and variant options, then variant_name will be {variant}-{size}
        # Case 2: The given product has only size options, then variant_name will be {size}
        # Case 3: The given product has only variant options, then variant_name will be {variant}
        # Case 4: The given product has no options for size or variant, then variant_name will be {OneSize}
        ####

        if len(size_selection) != 0 and len(variant_selection) != 0:
            # First we will loop over variants, then for each variant, we will loop over sizes.
            for gen in self.__iterate_variants(response.url, variant_selection, self.extract_variant_style, parent_asin, product_name):
                yield gen
        elif len(size_selection) != 0:
            for gen in self.__iterate_sizes(response.url, size_selection, parent_asin, product_name):
                yield gen
        elif len(variant_selection) != 0:
            for gen in self.__iterate_variants(response.url, variant_selection, self.extract_style, parent_asin, product_name):
                yield gen
        else:
            total_skus = ["One Size"]
            available_skus = total_skus if not out_of_stock else []
            yield BrokennessItem(brand=self.brand, channel=self.channel, name=product_name, skuid=parent_asin,
                                 product_href=product_href, total_skus=total_skus, available_skus=available_skus,
                                 score=0.0, extracted_date=extracted_date)

    def extract_variant_style(self, response, **kwargs):
        parent_asin = response.meta['parent_asin']
        self.logger.info(f"Fetching variant from {response.url} for parent {parent_asin}")
        variant_asin = re.match('(.*)/dp/([A-Z0-9]{10})', response.url).group(2)
        size_selection = response.selector.xpath("//select[@id='native_dropdown_selected_size_name']"
                                                 "/option[not(@id ='native_size_name_-1')]")

        for item in size_selection:
            size_asin = re.search(r"([\d]{1}),([A-Z0-9]{10})", item.xpath("@value").get()).group(2)
            size_name = item.xpath("text()").get().strip()
            size_href = response.url.replace(variant_asin, size_asin)
            size_href = replace_query_param(size_href, 'th', '1')
            size_href = replace_query_param(size_href, 'psc', '1')
            yield self.request(size_href, self.extract_style, meta={
                "parent_asin": parent_asin, "variant_name": response.meta['variant_name'] + " - " + size_name,
                "product_href": response.meta['product_href'], "product_name": response.meta['product_name']
            }, dont_filter=True)

    def extract_style(self, response, **kwargs):
        parent_asin = response.meta['parent_asin']
        self.logger.info(f"Fetching style from {response.url} for parent {parent_asin}")
        variant_name = response.meta['variant_name']
        product_href = response.meta['product_href']
        product_name = response.meta['product_name']
        extracted_date = datetime.today().strftime('%Y%m%d')

        out_of_stock = response.selector.xpath("//div[@id='outOfStock']").get()
        total_skus = [variant_name]
        available_skus = [variant_name] if not out_of_stock else []

        # Special case for Amazon
        # Yielding single item for every size, Please refer brokenness/brokenness/pipelines.py for next steps
        yield BrokennessItem(brand=self.brand, channel=self.channel, name=product_name, skuid=parent_asin,
                             product_href=product_href, total_skus=total_skus, available_skus=available_skus,
                             score=0.0, extracted_date=extracted_date)

    def __iterate_sizes(self, url, size_selection, asin, name):
        for item in size_selection:
            size_asin = re.search(r"([\d]{1}),([A-Z0-9]{10})", item.xpath("@value").get()).group(2)
            size_name = item.xpath("text()").get().strip()
            size_href = url.replace(asin, size_asin)
            size_href = replace_query_param(size_href, 'th', '1')
            size_href = replace_query_param(size_href, 'psc', '1')
            yield self.request(size_href, self.extract_style, meta={
                "parent_asin": asin, "variant_name": size_name,
                "product_href": url, "product_name": name
            }, dont_filter=True)

    def __iterate_variants(self, url, variant_selection, callback_func, asin, name):
        for item in variant_selection:
            size_asin = item.xpath("@data-defaultasin").get()
            variant_name = item.xpath("@title").get().strip()
            variant_name = variant_name.replace("Click to select ", "")
            size_href = url.replace(asin, size_asin)
            size_href = replace_query_param(size_href, 'th', '1')
            size_href = replace_query_param(size_href, 'psc', '1')
            yield self.request(size_href, callback_func, meta={
                "parent_asin": asin, "variant_name": variant_name,
                "product_href": url, "product_name": name,
            })

    def request(self, url, callback, meta=None, dont_filter=False):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, dont_filter=dont_filter)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("scrape_limit: " + str(self.scrape_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
