import logging
import json
from functools import reduce
from typing import List
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qsl

from itemadapter import ItemAdapter
from scrapy import Request
from decouple import config

from ..base import MarketPlaceFactory, AbstractStyleList, AbstractStyle, AbstractStyleReviews
from ..utils import replace_query_param, ScrapeMode, ChannelType
from ...items import StyleItem, StyleReviewItem


class MyntraStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, selection, **kwargs) -> List[str]:
        style_list = [f"https://www.myntra.com/gateway/v2/product/{product['productId']}" for product in
                      selection["products"]]
        return style_list


class MyntraStyle(AbstractStyle):

    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, selection, **kwargs) -> StyleItem:
        mid = selection['id']
        sub_category = selection['analytics']['articleType']
        product_name = selection['name']
        manufacturer = selection['manufacturer']
        product_href = f"https://www.myntra.com/{selection['id']}"
        available = not selection['flags']['outOfStock']
        mrp = selection['mrp']
        price = str(min(map(lambda item: item['sizeSellerData'][0]['discountedPrice'] if item['available'] and len(
            item['sizeSellerData']) > 0 else mrp, selection['sizes'])))

        discount = str(int(mrp) - int(price))

        rating = selection['ratings']['averageRating'] if selection['ratings'] else '0'
        rating_count = selection['ratings']['totalCount'] if selection['ratings'] else '0'
        rating_star = {5: 0, 4: 0, 3: 0, 2: 0, 1: 0}
        if int(rating_count) > 0:
            for info in selection['ratings']['ratingInfo']:
                rating_star.update({info['rating']: int(round(info['count'] / int(rating_count) * 100))})
        rating_star = json.dumps(rating_star)
        rating_aspect = json.dumps({})

        reviews_href = f"https://www.myntra.com/reviews/{selection['id']}"

        return StyleItem(
            sku=str(mid), brand=kwargs['brand'], category=kwargs['category'],
            sub_categories=sub_category, name=product_name, product_href=product_href, mrp=str(mrp), price=price,
            discount=discount, extracted_date=datetime.today().strftime('%Y%m%d'), reviews_href=reviews_href,
            available=available, channel=ChannelType.Myntra, manufacturer=manufacturer, rating_count=str(rating_count),
            rating=str(rating), best_seller_rank='0', rating_star=rating_star, rating_aspect=rating_aspect
        )


class MyntraStyleReviews(AbstractStyleReviews):

    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, selection, **kwargs) -> List[StyleReviewItem]:
        style_review_item_list = []
        for review_item in selection:
            review_id = review_item['id']
            date = datetime.fromtimestamp(int(review_item['updatedAt']) / 1000).strftime('%Y%m%d')
            mid = review_item['style']['id']
            name = review_item['userName']
            star = str(review_item['userRating'])
            text = review_item['review']
            text = text.strip()
            upvotes = int(review_item['upvotes'])
            extracted_date = datetime.today().strftime('%Y%m%d')
            aspects = json.dumps(review_item['styleAttribute'])

            style_review_item = StyleReviewItem(id=review_id, sku=str(mid), brand=kwargs['brand'],author=name, star=star, aspects=aspects, date=date, verified=True,title=text, description=text, channel=ChannelType.Myntra,upvotes=upvotes, extracted_date=extracted_date,category=kwargs['category'])
            style_review_item_list.append(style_review_item)
        return style_review_item_list


class Myntra(MarketPlaceFactory):
    brand = None
    mode = None
    review_capture_duration = None
    smart_proxy = None
    scrape_products = True
    scrape_reviews = True
    category = None

    def __init__(self, brand, **kwargs) -> None:
        super().__init__()
        self.brand = brand
        self.mode = kwargs['mode']
        self.review_capture_duration = kwargs['review_capture_duration']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None
        self.scrape_reviews = kwargs['scrape_reviews']
        self.scrape_products = kwargs['scrape_products']
        self.category = kwargs['category']

    def create_style_list(self) -> MyntraStyleList:
        return MyntraStyleList()

    def create_style(self) -> MyntraStyle:
        return MyntraStyle()

    def create_style_reviews(self) -> MyntraStyleReviews:
        return MyntraStyleReviews()

    @property
    def logger(self):
        logger = logging.getLogger(self.brand)
        return logging.LoggerAdapter(logger, {'rnr_spider': self})

    def start_request(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.start_request_cookies_wrapper)

    def start_request_cookies_wrapper(self, response, **kwargs):
        brand_path = response.url.split('/')[-1]
        api_start_urls = [
            f'https://www.myntra.com/gateway/v2/search/{brand_path}?p=1&rows=50&o=0&plaEnabled=false'
        ]
        for url in api_start_urls:
            yield self.request(url, self.extract_style_list)

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        style_list = self.create_style_list().clean(selection)

        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        style_list = style_list[:10] if self.mode is ScrapeMode.Test and len(style_list) > 10 else style_list
        for url in style_list:
            yield self.request(url, self.extract_style)

        num_items = selection['totalCount']
        url = response.url
        page = int(dict(parse_qsl(urlparse(url)[4])).get('p', 1))

        if num_items and page * 50 < num_items and self.mode is ScrapeMode.Full:
            url = replace_query_param(url, 'p', page + 1)
            url = replace_query_param(url, 'o', (page * 50) - 1)
            yield self.request(url, self.extract_style_list)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = json.loads(response.text)
        if 'style' not in selection:
            return
        style = self.create_style().clean(selection['style'], brand=self.brand,category=self.category)

        if self.scrape_products:
            yield style

        if self.scrape_reviews:
            product_id = ItemAdapter(style).get('sku')
            url = f"https://www.myntra.com/gateway/v1/reviews/product/{product_id}"
            url = replace_query_param(url, 'size', '12')
            url = replace_query_param(url, 'sort', '1')
            url = replace_query_param(url, 'rating', '0')
            url = replace_query_param(url, 'page', '1')
            url = replace_query_param(url, 'includeMetaData', 'false')
            yield self.request(url, self.extract_reviews)

    def extract_reviews(self, response, **kwargs):
        self.logger.info(f"Fetching style review list from {response.url}")

        selection = json.loads(response.text)
        style_reviews_list = self.create_style_reviews().clean(selection['reviews'], brand=self.brand, category=self.category)

        should_continue_next_reviews = True
        for style_review in style_reviews_list:
            date = ItemAdapter(style_review).get('date')
            date = datetime.strptime(date, "%Y%m%d")
            if date < datetime.now() - timedelta(days=self.review_capture_duration):
                should_continue_next_reviews = False
                break
            yield style_review

        if should_continue_next_reviews and len(style_reviews_list) > 12:
            page = int(dict(parse_qsl(urlparse(response.url)[4])).get('page', 1))
            url = replace_query_param(response.url, 'page', page + 1)
            yield self.request(url, self.extract_reviews)

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Brand: " + self.brand)
        self.logger.info("mode: " + self.mode.name)
        self.logger.info("review_capture_duration: " + str(self.review_capture_duration))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
        self.logger.info("scrape_reviews: " + str(self.scrape_reviews))
        self.logger.info("scrape_products: " + str(self.scrape_products))