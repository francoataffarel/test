import json
import logging
import re
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse, parse_qsl

from decouple import config
from itemadapter import ItemAdapter
from scrapy import Request

from ..base import MarketPlaceFactory, AbstractStyle, AbstractStyleList, AbstractStyleReviews
from ..utils import replace_query_param, ScrapeMode, ChannelType
from ...items import StyleItem, StyleReviewItem


class NykaaFashionStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        return []

    def clean(self, selection, **kwargs) -> List[str]:
        return [f"https://www.nykaafashion.com{product['actionUrl']}"
                for product in selection['response']['products'] if 'actionUrl' in product]


class NykaaFashionStyle(AbstractStyle):
    def selectors(self, **kwargs) -> List[str]:
        return ["//script[@id='__PRELOADED_STATE__']/text()"]

    def clean(self, selection, **kwargs) -> StyleItem:

        product = selection['details']['skuData']['product']
        productId = product['id']
        product_name = product['subTitle']
        product_href = kwargs['url']
        available = not bool(product['isOutOfStock'])
        mrp = str(product['price'])
        price = str(product['discountedPrice'])

        rating = '0'
        rating_stars = {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}
        rating_count = '0'
        if 'review_rating_json' in product:
            if 'star_rating' in product['review_rating_json']:
                rating = str(product['review_rating_json']['star_rating'])
            if 'review_splitup' in product['review_rating_json']:
                rating_split_up = product['review_rating_json']['review_splitup']
                rating_count = str(rating_split_up['total_count'])
                for rating_star in rating_split_up:
                    if rating_star in ['0', '1', '2', '3', '4']:
                        rating_stars[str(rating_split_up[rating_star]['star'])] = int(rating_split_up[rating_star]['per'])
        rating_stars = json.dumps(rating_stars)

        reviews_href = re.sub(r'/p/', '/reviews/', product_href)
        reviews_href = re.sub(r'productId', 'skuId', reviews_href)
        reviews_href = replace_query_param(reviews_href, 'ptype', 'reviews')

        discount = str(int(mrp) - int(price))
        manufacturer = ''
        if 'productInfoNew' in product:
            for key in product['productInfoNew']:
                if key['slug'] == 'name_of_manufacturer':
                    manufacturer = key['value']
                    break

        return StyleItem(
            sku=productId, brand=kwargs['brand'],category=kwargs['category'],
            sub_categories=None, name=product_name, product_href=product_href,
            mrp=mrp, price=price, discount=discount, extracted_date=datetime.today().strftime('%Y%m%d'),
            reviews_href=reviews_href, available=available, channel=ChannelType.NykaaFashion,
            rating=rating, rating_count=rating_count, rating_star=rating_stars, manufacturer=manufacturer,
            best_seller_rank='0', rating_aspect=json.dumps({})
        )


class NykaaFashionStyleReviews(AbstractStyleReviews):
    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, review_response, **kwargs) -> List[StyleReviewItem]:
        reviews = review_response['response']['reviewData']
        style_review_item_list = []
        for review in reviews:
            review_id = str(review['id'])
            review_like = int(review['likeCount'])
            review_title = review['title']
            review_description = review['description']
            review_author = review['name']
            review_rating = str(review['rating'])
            match = re.search(r'/products/([0-9A-Za-z]*)/reviews', kwargs['url'])
            review_sku = match.group(1)
            review_verified_buyer = review['isBuyer']
            review_date = datetime.strptime(review['createdOn'], "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d')
            extracted_date = datetime.today().strftime('%Y%m%d')

            style_review_item = StyleReviewItem(
                id=review_id, sku=review_sku, brand=kwargs['brand'], author=review_author,
                star=review_rating, aspects=json.dumps({}), date=review_date, title=review_title,
                verified=review_verified_buyer, description=review_description, channel=ChannelType.NykaaFashion,
                upvotes=review_like, extracted_date=extracted_date, category=kwargs['category']
            )
            style_review_item_list.append(style_review_item)
        return style_review_item_list


class NykaaFashion(MarketPlaceFactory):
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

    def create_style_list(self) -> NykaaFashionStyleList:
        return NykaaFashionStyleList()

    def create_style(self) -> NykaaFashionStyle:
        return NykaaFashionStyle()

    def create_style_reviews(self) -> NykaaFashionStyleReviews:
        return NykaaFashionStyleReviews()

    @property
    def logger(self):
        logger = logging.getLogger(self.brand)
        return logging.LoggerAdapter(logger, {'rnr_spider': self})

    def start_request(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_style_list)

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        style_list = self.create_style_list().clean(selection)
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        style_list = style_list[:10] if self.mode is ScrapeMode.Test and len(style_list) > 10 else style_list
        for url in style_list:
            yield self.request(url, self.extract_style)

        count = selection['response']['count']
        current_page = int(dict(parse_qsl(urlparse(response.url)[4])).get('currentPage', 1))
        page_size = int(dict(parse_qsl(urlparse(response.url)[4])).get('PageSize', 10))

        # If page size was not given, then lets scrape again from first page with size 50
        if page_size < 50:
            current_page = 0

        if count > current_page * page_size and self.mode is ScrapeMode.Full:
            url = replace_query_param(response.url, 'currentPage', current_page + 1)
            url = replace_query_param(url, 'PageSize', 50)
            yield self.request(url, self.extract_style_list)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = response
        for selector in self.create_style().selectors():
            selection = selection.selector.xpath(selector).get()
            selection = json.loads(selection.encode('ascii', 'ignore'))

        style = self.create_style().clean(selection, url=response.url, brand=self.brand,category=self.category)

        if self.scrape_products:
            yield style

        if self.scrape_reviews:
            product_id = ItemAdapter(style).get('sku')
            url = f"https://www.nykaafashion.com/gateway-api/products/{product_id}/reviews"
            url = replace_query_param(url, 'sort', 'MOST_RECENT')
            url = replace_query_param(url, 'size', 20)
            url = replace_query_param(url, 'pageNo', 1)
            yield self.request(url, self.extract_reviews)

    def extract_reviews(self, response, **kwargs):
        self.logger.info(f"Fetching style review list from {response.url}")
        selection = response
        review_response = json.loads(selection.text)
        style_reviews_list = self.create_style_reviews().clean(review_response, url=response.url, brand=self.brand,category=self.category)

        should_continue_next_reviews = True
        for style_review in style_reviews_list:
            date = ItemAdapter(style_review).get('date')
            date = datetime.strptime(date, "%Y%m%d")
            if date < datetime.now() - timedelta(days=self.review_capture_duration):
                should_continue_next_reviews = False
                break
            yield style_review
        if should_continue_next_reviews and len(style_reviews_list) >= 20:
            page = dict(parse_qsl(urlparse(response.url)[4])).get('pageNo', 0)
            url = replace_query_param(response.url, 'sort', 'MOST_RECENT')
            url = replace_query_param(url, 'size', 20)
            url = replace_query_param(url, 'pageNo', int(page) + 1)
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