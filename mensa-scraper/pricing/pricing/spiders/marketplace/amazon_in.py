import json
import logging
from datetime import datetime
from typing import List
import re
from decouple import config
from scrapy import Request, Selector

from ..base import MarketPlaceFactory
from ..utils import ChannelType, replace_query_param, get_query_param, has_query_param, \
                remove_all_query_params, clean_amazon_price_value, get_brand_formatted_name
from ...items import PricingItem

class AmazonIn(MarketPlaceFactory):
    smart_proxy = None
    scrape_limit = None
    category = None

    def __init__(self, category, **kwargs):
        super().__init__()
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None
        self.category = category 

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.AmazonIn.name)
        return logging.LoggerAdapter(logger, {'pricing_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_list)
            
    def extract_list(self, response, **kwargs) -> List[str]:
        self.logger.info(f"Fetching Style List from {response.url}")
        selection = response.selector.xpath("//span[@data-component-type='s-search-results']/div/div[string-length(@data-asin) > 0 and not(contains(@class, 'AdHolder'))]")
        asin_product_href = re.compile("^/[a-zA-Z0-9-_]*/dp/[A-Z0-9]{10}")
        count = 0
        for i in range(len(selection)):
            item = Selector(text=selection[i].get())
            product_href = item.xpath('//h2/a/@href').get()
            if asin_product_href.match(product_href):
                product_href = f"https://www.amazon.in{product_href}"
                product_href = remove_all_query_params(product_href)
                product_href = replace_query_param(product_href, 'th', '1')
                product_href = replace_query_param(product_href, 'psc', '1')
                brand = item.xpath("//h5//span/text()").get()
                price = clean_amazon_price_value(item.xpath("//span[contains(@data-a-color,'price')]//span[@class='a-price-whole']/text()").get())
                mrp = clean_amazon_price_value(item.xpath("//span[contains(@data-a-color,'secondary')]//span[@class='a-offscreen']/text()").get())
                yield self.request(product_href, self.extract_style, price=price, mrp=mrp, brand=brand)
                count += 1

        next_page = response.selector.css('span.s-pagination-strip,ul.a-pagination') \
            .xpath('//li[last()]/a/@href | //a[last()]/@href').get()
        if next_page:
            url = response.url
            page = int(get_query_param(url, 'page')) if has_query_param(url, 'page') else 1
            if page * count <= self.scrape_limit:
                url = replace_query_param(url, 'page', page + 1)
                yield self.request(url, self.extract_list)            

    def extract_style(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching Pricing Item from {response.url}")
        mrp=int(kwargs['mrp']) if kwargs['mrp'] else 0
        price=int(kwargs['price']) if kwargs['price'] else 0
        discount = int(mrp) - int(price) if price != 0 and mrp != 0 else 0
        discount_percentage = (int(mrp) - int(price))*100 // int(mrp) if price != 0 and mrp != 0 else 0
        mrp, price, discount, discount_percentage = str(mrp), str(price), str(discount), str(discount_percentage)
        product_href = response.url
        skuid = re.match('(.*)/dp/([A-Z0-9]{10})', product_href).group(2)
        category = self.category
        if kwargs['brand']:
            brand = get_brand_formatted_name(kwargs['brand'])
        else:
            brand = response.selector.xpath('//*[@id="bylineInfo"]/text() | //*[@id="amznStoresBylineLogoTextContainer"]/a/text() | //a[@id="brand"]/text() | //a[@id="brandteaser"]/img/@alt').get().strip()
            matches = re.findall("(?<=Brand: ).*|(?<=Visit the ).*(?= Store)", brand)
            brand = matches[0] if len(matches) > 0 else brand
        extracted_date = datetime.today().strftime('%Y%m%d')
        product_title = response.selector.xpath("//span[@id='productTitle']//text()").get().strip()
        coupon = response.selector.xpath('//div[@id="vpcButton"]/div/span[2]/text()').get()
        available = response.selector.xpath('//div[@id="availability"]/span[1][contains(text(), "Currently unavailable.")]').get() is None
        if coupon:
            coupon = coupon.strip()
        else:
            coupon = 'NA'
        bank_offers_object = response.selector.xpath('//span[@id="a2i-dpx-sopp-ibd-modal"]/@data-a-modal').get()
        if bank_offers_object:
            bank_offers_object = json.loads(bank_offers_object)
            url = f"https://www.amazon.in{bank_offers_object['url']}"
            pricing_item = PricingItem(skuid=skuid, category=category, title=product_title, product_href=product_href, 
                    mrp=mrp, price=price, discount=discount, discount_percentage=discount_percentage,
                    coupon_offers=coupon, brand=brand, extracted_date=extracted_date, 
                    available=available, marketplace=ChannelType.AmazonIn)
            yield self.request(url, callback=self.extract_bank_offers, item=pricing_item)
        else:
            yield PricingItem(skuid=skuid, category=category, title=product_title, product_href=product_href, 
                    mrp=mrp, price=price, discount=discount, discount_percentage=discount_percentage,
                    coupon_offers=coupon, bank_offers='NA', brand=brand, extracted_date=extracted_date, 
                    available=available, marketplace=ChannelType.AmazonIn)

    def extract_bank_offers(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching Bank Offers from {response.url}")
        pricing_item = kwargs['item']
        bank_offers = response.selector.xpath('//div[contains(@class, "a-spacing-medium")]/text()').getall()
        if bank_offers:
            bank_offers = [x.strip() for x in bank_offers if x and len(x.strip()) != 0]
            bank_offers = ', '.join(bank_offers)
            pricing_item['bank_offers'] = bank_offers
        else:
            pricing_item['bank_offers'] = 'NA'
        yield pricing_item
        
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