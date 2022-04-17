from typing import List
import logging
import re
from datetime import datetime

from scrapy import Request, Selector
from decouple import config

from ..base import AbstractMarketplace
from ...items import BestsellersItem
from ..utilities import ChannelType


def build_style_list_urls(sku_ids: List[str]) -> List[str]:
    return [f'https://www.amazon.in/dp/{skuid}/' for skuid in sku_ids]


class AmazonIn(AbstractMarketplace):
    brand = None
    smart_proxy = None

    def __init__(self, brand, **kwargs):
        super().__init__()
        self.brand = brand
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(self.brand)
        return logging.LoggerAdapter(logger, {'bestsellers_spider': self})

    def start_requests(self, sku_ids: List[str]):
        self.print_config()
        start_urls = build_style_list_urls(sku_ids)
        for url in start_urls:
            yield self.request(url, self.extract_style)

    def extract_style(self, response, **kwargs):
        best_seller_element = response
        product_href = response.url
        skuid = re.match('(.*)/dp/([A-Z0-9]{10})', product_href).group(2)
        name = best_seller_element.xpath('.//*[@id="productTitle"]/text()').get().strip()
        text_1 = best_seller_element.xpath(
            '((.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[1]/text())[2]'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[1]/text()[1]'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[1]/text()[1]').get()
        rank_1 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_1)[0] if text_1 else ""
        print('text_1', text_1)
        category_1 = text_1.split("in ")[1].split(" (")[0] if text_1 else ""
        category_1 = category_1.strip() if category_1 else ""

        text_2 = best_seller_element.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[2]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/text()').get()
        rank_2 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_2)[0] if text_2 else ""
        category_2 = best_seller_element.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[2]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/a/text()').get()
        category_2 = category_2.strip() if category_2 else ""

        text_3 = best_seller_element.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[3]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/text()').get()
        rank_3 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_3)[0] if text_3 else ""
        category_3 = best_seller_element.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[3]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/a/text()').get()
        category_3 = category_3.strip() if category_3 else ""

        text_4 = best_seller_element.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[4]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/text()').get()
        rank_4 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_4)[0] if text_4 else ""
        category_4 = best_seller_element.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[4]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/a/text()').get()
        category_4 = category_4.strip() if category_4 else ""

        extracted_date = datetime.today().strftime('%Y%m%d')
        yield BestsellersItem(skuid=skuid, brand=self.brand, name=name, channel=ChannelType.AmazonIn,
                              product_href=product_href, extracted_date=extracted_date,
                              category_1=category_1, rank_1=rank_1, category_2=category_2, rank_2=rank_2,
                              category_3=category_3, rank_3=rank_3, category_4=category_4, rank_4=rank_4)

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("brand: " + str(self.brand))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
