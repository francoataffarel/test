import logging
from datetime import datetime
from typing import List
from decouple import config
from scrapy import Request, Selector
import re
import json
from ..base import MarketPlaceFactory
from ..utils import ChannelType, get_query_param
from ...items import PricingItem

class Flipkart(MarketPlaceFactory):
    smart_proxy = None
    scrape_limit = None
    category = None

    def __init__(self, category, **kwargs):
        super().__init__()
        self.category = category
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Flipkart.name)
        return logging.LoggerAdapter(logger, {'pricing_spider': self})

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
            page = int(get_query_param(url, 'page'))
            if page * 40 <= self.scrape_limit:
                yield self.request(url, self.extract_list)

    def extract_style(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching style from {response.url}")
        selection = response.selector.xpath("//div[@id='container']")
        skuid = get_query_param(response.url, 'pid')
        name = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' aMaAEs "
                        "')]//h1//span[contains(concat(' ',normalize-space(@class),' '),"
                        "' B_NuCI ')]/text()").get()
        product_href = response.url
        extracted_date = datetime.today().strftime('%Y%m%d')
        out_of_stock = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' _16FRp0 ')]"
                                "/text()").get()
        mrp = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' CEmiEU ')]"
                              "//div[contains(concat(' ',normalize-space(@class),' '),' _3I9_wc ')]/text()").getall()
        price = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' CEmiEU ')]"
                                "//div[contains(concat(' ',normalize-space(@class),' '),' _30jeq3 ')]/text()").get()
        
        available = not out_of_stock
        
        if price is not None:
            price = re.sub(r'\D', '', price)
            price = int(price)
        else:
            price = 0

        if mrp:
            mrp = ''.join(mrp)
            mrp = re.sub(r'\D', '', mrp)
            mrp = int(mrp)
        else:
            mrp = price

        if mrp is not None and price is not None:
            discount = mrp - price
            discount_percentage = (mrp - price)*100 // mrp
        else:
            discount = 0
            discount_percentage = 0
        mrp, price, discount, discount_percentage = str(mrp), str(price), str(discount), str(discount_percentage)
        offers = selection.xpath(".//span[contains(concat(' ',normalize-space(@class),' '),' _3j4Zjq ')]/li").getall()
        bank_offers = []
        for i in range(len(offers)):
            item = Selector(text=offers[i])
            offer_type = item.xpath(".//span[contains(concat(' ',normalize-space(@class),' '),' u8dYXW ')]/text()").get()
            if offer_type and offer_type.strip() == 'Bank Offer':
                bank_offers.append(item.xpath("//li/span[2]/text()").get())

        bank_offers_text = ','.join(bank_offers) if len(bank_offers) > 0 else 'NA'
        brand_json_array = json.loads(selection.xpath("//script[@id='jsonLD'][1]/text()").get())
        brand = None
        for brand_json_object in brand_json_array:
            if 'brand' in brand_json_object:
                brand = brand_json_object['brand']['name']
                break
        if brand:
            yield PricingItem(skuid=skuid, category=self.category, brand=brand,
                title=name, product_href=product_href, mrp=mrp, price=price, discount=discount, 
                discount_percentage=discount_percentage, bank_offers=bank_offers_text, coupon_offers="NA",
                extracted_date=extracted_date, available=available, marketplace=ChannelType.Flipkart)


    def request(self, url, callback, meta=None, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Category: " + self.category)
        self.logger.info("scrape_limit: " + str(self.scrape_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))               