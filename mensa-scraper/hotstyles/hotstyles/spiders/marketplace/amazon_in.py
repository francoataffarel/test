from typing import List
import logging
import re
from datetime import datetime

from scrapy import Request
from decouple import config

from ..base import AbstractMarketplace
from ...items import HotstylesItem
from ..utilities import ChannelType, clean_amazon_price_value


def build_style_list_urls(sku_ids: List[str]) -> List[str]:
    return [f'https://www.amazon.in/dp/{skuid}/' for skuid in sku_ids]


class AmazonIn(AbstractMarketplace):
    smart_proxy = None

    def __init__(self, **kwargs):
        super().__init__()
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.AmazonIn.name)
        return logging.LoggerAdapter(logger, {'hotstyles_spider': self})

    def start_requests(self, sku_ids: List[str]):
        self.print_config()
        start_urls = build_style_list_urls(sku_ids)
        for url in start_urls:
            yield self.request(url, self.extract_style)

    def extract_style(self, response, **kwargs):
        selection = response
        product_href = response.url
        skuid = re.match('(.*)/dp/([A-Z0-9]{10})', product_href).group(2)
        name = selection.xpath('.//*[@id="productTitle"]/text()').get().strip()
        text_1 = selection.xpath(
            '((.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[1]/text())[2]'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[1]/text()[1]'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[1]/text()[1]').get()
        rank_1 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_1)[0] if text_1 else ""
        category_1 = text_1.split("in ")[1].split(" (")[0] if text_1 else ""
        category_1 = category_1.strip() if category_1 else ""

        text_2 = selection.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[2]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/text()').get()
        rank_2 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_2)[0] if text_2 else ""
        category_2 = selection.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[2]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[2]/a/text()').get()
        category_2 = category_2.strip() if category_2 else ""

        text_3 = selection.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[3]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/text()').get()
        rank_3 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_3)[0] if text_3 else ""
        category_3 = selection.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[3]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[3]/a/text()').get()
        category_3 = category_3.strip() if category_3 else ""

        text_4 = selection.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[4]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/text()').get()
        rank_4 = re.findall(r'(?<=#)(.*)(?=\s+in)', text_4)[0] if text_4 else ""
        category_4 = selection.xpath(
            '(.//*[@id="detailBulletsWrapper_feature_div"]/ul[1]//span[@class="a-list-item"])[4]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]/tbody/tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/a/text()'
            ' | //*[@id="productDetails_detailBullets_sections1"]//tr[th//text()[contains(., "Best Sellers Rank")]]/td/*/span[4]/a/text()').get()
        category_4 = category_4.strip() if category_4 else ""

        rating = selection.xpath("//*[@id='averageCustomerReviews']//i[contains(concat(' ',normalize-space("
                                 "@class),' '),' a-icon-star ')]//span/text()").get()
        rating_count = selection.xpath("//*[@id='averageCustomerReviews']//span[contains(concat(' ',"
                                       "normalize-space(@class),' '),' a-size-base ')]").get()
        mrp = clean_amazon_price_value(selection.xpath(".//span[text()='M.R.P.: ']//span[@class='a-offscreen']/text()").get())
        price = clean_amazon_price_value(selection.xpath(".//span[contains(concat(' ',normalize-space(@class),' '),' priceToPay ')]"
                                "//span[@class='a-offscreen']/text()").get())

        if mrp == '0':
            discount = '0'
        else:
            discount = str(int(float(mrp)) - int(float(price)))

        if rating is not None:
            rating = rating.replace(' out of 5 stars', '')
        else:
            rating = '0'

        if rating_count is not None:
            rating_count = re.sub(r'\D', '', rating_count)
        else:
            rating_count = '0'

        extracted_date = datetime.today().strftime('%Y-%m-%d')
        yield HotstylesItem(skuid=skuid, name=name, channel=ChannelType.AmazonIn,
                            product_href=product_href, extracted_date=extracted_date,
                            mrp=mrp, price=price, discount=discount, rating=rating, rating_count=rating_count,
                            category_1=category_1, rank_1=rank_1, category_2=category_2, rank_2=rank_2,
                            category_3=category_3, rank_3=rank_3, category_4=category_4, rank_4=rank_4
                            )

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
