import logging
import re
import json
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse, parse_qsl

from itemadapter import ItemAdapter
from scrapy import Request
from decouple import config

from ..base import MarketPlaceFactory, AbstractStyleList, AbstractStyle, AbstractStyleReviews
from ..utils import replace_query_param, retain_query_param, ScrapeMode, \
    ChannelType, get_query_param
from ...items import StyleItem, StyleReviewItem


class FlipkartStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        return ["//div[string-length(@data-id) > 0]/div/a/@href"]

    def clean(self, selection, **kwargs) -> List[str]:
        fsn_product_href = re.compile(r"^/[a-zA-Z0-9-]*/p/itm[a-zA-Z0-9]*\?pid=[A-Z0-9]{16}&lid=[A-Z0-9]{25}")
        style_list = list(set(filter(fsn_product_href.match, selection.getall())))
        style_list = [f"https://www.flipkart.com{style}" for style in style_list]
        return style_list


class FlipkartStyle(AbstractStyle):

    def selectors(self, **kwargs) -> List[str]:
        return ["//div[@id='container']"]

    def clean(self, selection, **kwargs) -> StyleItem:
        fsn = dict(parse_qsl(urlparse(kwargs['url'])[4]))['pid']
        sub_category = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' "
                                       "_1MR4o5 ')]//a/text()").getall()
        product_name = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' aMaAEs "
                                       "')]//h1//span[contains(concat(' ',normalize-space(@class),' '),"
                                       "' B_NuCI ')]/text()").get()
        product_href = kwargs['url']
        out_of_stock = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' _16FRp0 ')]"
                                       "/text()").get()
        mrp = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' CEmiEU ')]"
                              "//div[contains(concat(' ',normalize-space(@class),' '),' _3I9_wc ')]/text()").getall()
        price = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' CEmiEU ')]"
                                "//div[contains(concat(' ',normalize-space(@class),' '),' _30jeq3 ')]/text()").get()
        rating = selection.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' _1YokD2 ')]"
                                 "//span[contains(@id,'productRating')]//div/text()").get()
        reviews_href = re.sub(r'/p/', '/product-reviews/', product_href)
        reviews_href = retain_query_param(reviews_href, 'pid', 'lid')

        available = not out_of_stock

        if price is not None:
            price = re.sub(r'\D', '', price)
        else:
            price = '0'

        if mrp:
            mrp = ''.join(mrp)
            mrp = re.sub(r'\D', '', mrp)
        else:
            mrp = price

        if mrp is not None and price is not None:
            discount = str(int(mrp) - int(price))
        else:
            discount = '0'

        rating = rating if rating else '0'

        sub_category = sub_category[1].strip() if sub_category is not None and len(sub_category) > 1 else None
        return StyleItem(
            sku=fsn, brand=kwargs['brand'],category=kwargs['category'],
            sub_categories=sub_category, name=product_name, product_href=product_href,
            mrp=mrp, price=price, discount=discount, extracted_date=datetime.today().strftime('%Y%m%d'),
            reviews_href=reviews_href, available=available, channel=ChannelType.Flipkart,
            rating=rating, manufacturer='', best_seller_rank='0',
        )


class FlipkartStyleReviews(AbstractStyleReviews):

    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, selection, **kwargs) -> List[StyleReviewItem]:
        pass


class Flipkart(MarketPlaceFactory):
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

    def create_style_list(self) -> FlipkartStyleList:
        return FlipkartStyleList()

    def create_style(self) -> FlipkartStyle:
        return FlipkartStyle()

    def create_style_reviews(self) -> FlipkartStyleReviews:
        return FlipkartStyleReviews()

    @property
    def logger(self):
        logger = logging.getLogger(self.brand)
        return logging.LoggerAdapter(logger, {'rnr_spider': self})

    def start_request(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_style_list)

    def extract_style_list(self, response, **kwargs):
        selection = response
        for selector in self.create_style_list().selectors():
            selection = selection.selector.xpath(selector)

        style_list = self.create_style_list().clean(selection)
        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        style_list = style_list[:10] if self.mode is ScrapeMode.Test and len(style_list) > 10 else style_list
        for url in style_list:
            yield self.request(url, self.extract_style)

        next_pages = response.selector.xpath("//nav/a/@href").getall()
        next_pages = next_pages[:1] if self.mode is ScrapeMode.Test and len(next_pages) > 1 else next_pages
        for next_page in next_pages:
            yield self.request(f"https://www.flipkart.com{next_page}", self.extract_style_list)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = response
        for selector in self.create_style().selectors():
            selection = selection.selector.xpath(selector)
        style = self.create_style().clean(selection, url=response.url, brand=self.brand,category=self.category)

        url = ItemAdapter(style).get('reviews_href')
        url = replace_query_param(url, 'aid', 'overall')
        url = replace_query_param(url, 'certifiedBuyer', 'false')
        url = replace_query_param(url, 'sortOrder', 'MOST_RECENT')
        url = replace_query_param(url, 'page', '1')
        yield self.request(url, self.extract_reviews, meta={'style': style})

    def extract_reviews(self, response, **kwargs):
        self.logger.info(f"Fetching style review list from {response.url}")
        if 'style' in response.meta and self.scrape_products:
            style = response.meta['style']
            stars = response.selector.xpath(".//ul[contains(concat(' ',normalize-space(@class),' '),' _2jr1F_ ')]"
                                            "//li//span[contains(concat(' ',normalize-space(@class),' '),' _26f_zl ')]"
                                            "//text()").getall()
            values = response.selector.xpath(".//ul[contains(concat(' ',normalize-space(@class),' '),' _36LmXx ')]"
                                             "//li//div//text()").getall()
            values = [re.sub(r'(\D)', '', v) for v in values]
            rating_count = sum([int(v) for v in values])
            rating_stars = dict(zip(stars, values))
            if rating_count == 0:
                rating_stars = {5: 0, 4: 0, 3: 0, 2: 0, 1: 0}
            else:
                rating_stars = {k: int(round(int(v) / int(rating_count) * 100)) for k, v in rating_stars.items()}
            style['rating_star'] = json.dumps(rating_stars)
            style['rating_count'] = str(rating_count)
            style['rating_aspect'] = json.dumps({})
            yield style

        if not self.scrape_reviews:
            return

        selection = response.selector.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' _27M-vq ')]")
        # For cases where there are not reviews for the given style
        if len(selection) == 0:
            return

        should_continue_next_reviews = True
        for review_container in selection:
            review_href = review_container.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' _3E8aIl ')]"
                                                 "//a/@href").get()
            id = review_href.replace('/reviews/', '')
            days_ago = review_container.xpath(".//p[@class='_2sc7ZR']/text()").get()
            extracted_date = datetime.today().strftime('%Y%m%d')

            if 'days ago' in days_ago or 'day ago' in days_ago:
                review_posted = re.match(r'^(\d*)(\W)', days_ago).group(1)
                date = datetime.now() - timedelta(days=int(review_posted))
                date = datetime.strftime(date, "%Y%m%d")
            elif 'months ago' in days_ago or 'month ago' in days_ago:
                review_posted = int(re.match(r'^(\d*)(\W)', days_ago).group(1))
                date = datetime.now() - timedelta(days=(review_posted + 1) * 30)  # Taking 1 additional month buffer
                date = datetime.strftime(date, "%Y%m%d")
            elif 'Today' in days_ago:
                date = extracted_date
            else:
                should_continue_next_reviews = False
                break

            date = datetime.strptime(date, "%Y%m%d")
            if date < datetime.now() - timedelta(days=self.review_capture_duration):
                should_continue_next_reviews = False
                break
            yield self.request(f"https://1.rome.api.flipkart.com/api/3/reviews/permalink?reviewId={id}",
                               self.extract_review_item)

        if should_continue_next_reviews:
            page = dict(parse_qsl(urlparse(response.url)[4]))['page']
            url = replace_query_param(response.url, 'page', int(page) + 1)
            url = replace_query_param(url, 'aid', 'overall')
            url = replace_query_param(url, 'certifiedBuyer', 'false')
            url = replace_query_param(url, 'sortOrder', 'MOST_RECENT')
            yield self.request(url, self.extract_reviews)

    def extract_review_item(self, response, **kwargs):
        self.logger.info(f"Fetching style review item from {response.url}")
        review_json = json.loads(response.text)
        review_title = review_json['RESPONSE']['reviewInfo']['title']
        text = review_json['RESPONSE']['reviewInfo']['reviewText']
        name = review_json['RESPONSE']['reviewInfo']['author']
        star = str(review_json['RESPONSE']['reviewInfo']['rating'])
        vp = review_json['RESPONSE']['reviewInfo']['certifiedBuyer']
        fsn = get_query_param(review_json['RESPONSE']['allReviewsUrl'], 'pid')
        id = review_json['RESPONSE']['reviewInfo']['reviewId']
        vp = True if vp == 'Certified Buyer' else False
        upvotes = review_json['RESPONSE']['reviewInfo']['yesCount']
        extracted_date = datetime.today().strftime('%Y%m%d')
        date = datetime.fromtimestamp(review_json['RESPONSE']['reviewInfo']['date'] / 1000.0).strftime('%Y%m%d')
        category = review_json['RESPONSE']['productSummary'][fsn]['value']['analyticsData']['category']

        yield StyleReviewItem(id=id, sku=fsn, brand=self.brand,
                              author=name, star=star, aspects=json.dumps({}), title=review_title, date=date,
                              verified=vp, extracted_date=extracted_date, description=text,
                              channel=ChannelType.Flipkart, upvotes=upvotes, category=category)

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