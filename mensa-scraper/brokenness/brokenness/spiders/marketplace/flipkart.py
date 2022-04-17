import logging
import re
from datetime import datetime
from typing import List
from urllib.parse import urlparse, parse_qsl

from decouple import config
from scrapy import Request

from ..base import MarketPlaceFactory
from ..uitilies import ChannelType
from ...items import BrokennessItem


class Flipkart(MarketPlaceFactory):
    brand = None
    smart_proxy = None
    scrape_limit = None

    def __init__(self, brand, **kwargs):
        super().__init__()
        self.brand = brand
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Flipkart.name)
        return logging.LoggerAdapter(logger, {'brokenness_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_list)

    def extract_list(self, response, **kwargs):
        selection = response.selector.xpath("//div[string-length(@data-id) > 0]/div/a/@href")
        fsn_product_href = re.compile(r"^/[a-zA-Z0-9-]*/p/itm[a-zA-Z0-9]*\?pid=[A-Z0-9]{16}&lid=[A-Z0-9]{25}")
        style_list = list(set(filter(fsn_product_href.match, selection.getall())))
        style_list = [f"https://www.flipkart.com{style}" for style in style_list]
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")

        for url in style_list:
            yield self.request(url, self.extract_style)

        next_pages = response.selector.xpath("//nav/a/@href").getall()
        for next_page in next_pages:
            url = f"https://www.flipkart.com{next_page}"
            page = int(dict(parse_qsl(urlparse(url)[4])).get('page', 1))
            if page <= self.scrape_limit/40:
                yield self.request(url, self.extract_list)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = response.selector.xpath("//div[@id='container']")
        skuid = dict(parse_qsl(urlparse(response.url)[4]))['pid']
        name = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' aMaAEs "
                               "')]//h1//span[contains(concat(' ',normalize-space(@class),' '),"
                               "' B_NuCI ')]/text()").get()
        product_href = response.url
        extracted_date = datetime.today().strftime('%Y%m%d')
        total_skus = selection.xpath("//ul[@class='_1q8vHb']/li[contains(@id, 'swatch')]/a/text()").getall()
        available_skus = selection.xpath("//ul[@class='_1q8vHb']/li[contains(@id, 'swatch')]"
                                         "/a[not(contains(@class, 'qaAL3J')) and "
                                         "not(contains(@class, '_1ynmf9'))]/text()").getall()

        count_total_skus = len(total_skus)
        count_available_skus = len(available_skus)

        if count_total_skus > 0:
            score = round((count_total_skus - count_available_skus) / count_total_skus, 2)
            total_skus = ', '.join(total_skus)
            available_skus = ', '.join(available_skus)
        else:
            # When the product doesn't have any variants but is in stock
            score = 0.0
            total_skus = ''
            available_skus = ''

        yield BrokennessItem(brand=self.brand, channel=ChannelType.Flipkart, name=name, skuid=skuid,
                             product_href=product_href, total_skus=total_skus, available_skus=available_skus,
                             score=score, extracted_date=extracted_date)

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Brand: " + self.brand)
        self.logger.info("scrape_limit: " + str(self.scrape_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
