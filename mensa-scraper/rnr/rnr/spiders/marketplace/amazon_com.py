import json
import logging
import re
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse, parse_qsl

from itemadapter import ItemAdapter
from scrapy import Request, Selector
from decouple import config

from ..base import MarketPlaceFactory, AbstractStyle, AbstractStyleList, AbstractStyleReviews
from ..utils import get_query_param, replace_query_param, ScrapeMode,\
    ChannelType, clean_amazon_price_value
from ...items import StyleItem, StyleReviewItem


class AmazonComStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        return ["//span[@data-component-type='s-search-results']/div/div[string-length(@data-asin) > 0 and not(contains(@class, 'AdHolder'))]"]

    def clean(self, selection, **kwargs) -> List[dict]:
        asin_product_href = re.compile("^/[a-zA-Z0-9-_]*/dp/[A-Z0-9]{10}")
        result = []
        for i in range(len(selection)):
            item = Selector(text=selection[i].get())
            product_href = item.xpath('//h2/a/@href').get()
            if asin_product_href.match(product_href):
                product_href = f"https://www.amazon.com{product_href}"
                price = clean_amazon_price_value(item.xpath("//span[contains(@data-a-color,'base')]//span[@class='a-offscreen']/text()").get())
                mrp = clean_amazon_price_value(item.xpath("//span[contains(@data-a-color,'secondary')]//span[@class='a-offscreen']/text()").get())
                result.append(dict(product_href=product_href, price=price, mrp=mrp))
        return result


class AmazonComStyle(AbstractStyle):

    def selectors(self, **kwargs) -> List[str]:
        return ["//div[@id='dp']"]

    def clean(self, selection, **kwargs) -> StyleItem:
        style = kwargs['style']
        sub_category = selection.xpath("//*[contains(concat(' ',normalize-space(@class),' '),' a-breadcrumb "
                                       "')]//ul/li//a/text()").getall()
        product_name = selection.xpath("//span[contains(concat(' ',normalize-space(@class),' '),"
                                       "' product-title-word-break ')]//text()").get().strip()
        product_href = kwargs['url']
        out_of_stock = selection.xpath("//div[@id='outOfStock']").get()
        rating = selection.xpath("//*[@id='averageCustomerReviews']//i[contains(concat(' ',normalize-space("
                                 "@class),' '),' a-icon-star ')]//span/text()").get()
        rating_count = selection.xpath("//*[@id='averageCustomerReviews']//span[contains(concat(' ',"
                                       "normalize-space(@class),' '),' a-size-base ')]").get()
        rating_star_list = selection.xpath("//*[@id='histogramTable']//tr/@aria-label").getall()
        aspects = selection.xpath("//div[@data-hook='cr-summarization-attribute']")
        product_details = selection.xpath("//*[@id='productDetails_db_sections']")
        az_rank = product_details.xpath(".//tr[contains(.//th/text(),'Rank')]/td/span/span/text()").get()
        manufacturer = product_details.xpath(".//tr[contains(.//th/text(), 'Manufacturer')]/td/text()").get()
        reviews_href = selection.xpath("//div[@id='reviews-medley-footer']"
                                       "//a[@data-hook='see-all-reviews-link-foot']/@href").get()
        asin = re.match('(.*)/dp/([A-Z0-9]{10})', product_href).group(2)
        sub_category = sub_category[0].strip() if sub_category is not None and len(sub_category) > 0 else None

        # Check for out of stock
        if out_of_stock:
            available = False
        else:
            available = True

        if style['mrp'] != '0':
            discount = str(round(float(style['mrp']) - float(style['price']), 2))
        else:
            discount = '0'

        if rating is not None:
            rating = rating.replace(' out of 5 stars', '')
        else:
            rating = '0'

        if rating_count is not None:
            rating_count = re.sub(r'\D', '', rating_count)
        else:
            rating_count = '0'

        rating_star_list = list(set(rating_star_list))
        rating_star = {5: 0, 4: 0, 3: 0, 2: 0, 1: 0}
        for star in rating_star_list:
            # Match for two numeric group. Ex: 5 stars represent 65% of rating
            match = re.search('(^[0-9]*)([a-zA-Z ]*)([0-9]{1,3})', star)
            # Add groups in map. If group is not found then by default it will be 0
            if match.group(1) != '':
                rating_star[int(match.group(1))] = int(match.group(3))
            else:
                match = re.search('(^[0-9]*).*([0-9])', star)
                rating_star[int(match.group(2))] = int(match.group(1))

        rating_aspect = {}
        for aspect in aspects:
            values = aspect.xpath(".//span/text()").getall()
            rating_aspect[values[0]] = values[1]

        # Loop over tr and search if key with th has a rank keyword
        # then find the corresponding td and get first span text
        if az_rank is not None:
            az_rank = re.sub(r'\D', '', az_rank)
        else:
            az_rank = '0'

        if manufacturer is not None:
            manufacturer = manufacturer.strip()
        else:
            manufacturer = ''

        reviews_href = f"https://www.amazon.com{reviews_href}"
        return StyleItem(
            sku=asin, brand=kwargs['brand'],category=kwargs['category'],
            sub_categories=sub_category, name=product_name, product_href=product_href,
            mrp=style['mrp'], price=style['price'], discount=discount, rating=rating, rating_count=rating_count,
            rating_star=json.dumps(rating_star), rating_aspect=json.dumps(rating_aspect),
            extracted_date=datetime.today().strftime('%Y%m%d'), reviews_href=reviews_href,
            best_seller_rank=az_rank, available=available, manufacturer=manufacturer, channel=ChannelType.AmazonCom
        )


class AmazonComStyleReviews(AbstractStyleReviews):

    def selectors(self, **kwargs) -> List[str]:
        return ["//div[@id='cm_cr-review_list']/div[@data-hook='review']"]

    def clean(self, selection, **kwargs) -> List[StyleReviewItem]:
        style_review_item_list = []
        for review_container in selection:
            id = review_container.xpath('./@id').get()
            name = review_container.css('.a-profile-name').xpath('./text()').get()
            star = review_container.xpath(".//i[@data-hook='review-star-rating']/span/text()").get()
            title = review_container.xpath(".//a[@data-hook='review-title']/span/text()").get()
            date = review_container.xpath(".//span[@data-hook='review-date']/text()").get()
            vp = review_container.xpath(".//span[@data-hook='avp-badge']/text()").get()
            text = review_container.xpath(".//span[@data-hook='review-body']/span/text()").get()
            aspects = review_container.xpath(".//a[@data-hook='format-strip']/text()").getall()
            review_href = review_container.xpath(".//a[@data-hook='review-title']/@href").get()
            asin = get_query_param(review_href, 'ASIN')

            if text is not None:
                text = text.strip()
            else:
                text = ''
            if vp is not None and vp == 'Verified Purchase':
                vp = True
            else:
                vp = False
            if star is not None:
                star = star.replace(' out of 5 stars', '')
            else:
                star = '0'
            date = re.findall(r"(?<=\son\s)(.*)", date)[0]
            date = datetime.strptime(date, '%B %d, %Y').strftime('%Y%m%d')
            aspects = [aspect for aspect in aspects if not aspect.startswith("<i class")]
            aspects = [{aspect.split(':')[0].strip(): aspect.split(':')[1].strip()} for aspect in aspects]
            extracted_date = datetime.today().strftime('%Y%m%d')

            style_review_item = StyleReviewItem(id=id, sku=asin, brand=kwargs['brand'],
                                                author=name, star=star, description=text,
                                                title=title, aspects=json.dumps(aspects), date=date, verified=vp,
                                                channel=ChannelType.AmazonCom, extracted_date=extracted_date,
                                                category=kwargs['category'], upvotes=0)
            style_review_item_list.append(style_review_item)
        return style_review_item_list


class AmazonCom(MarketPlaceFactory):
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
        self.smart_proxy = config('PROXY_SESSION_US', None) if kwargs['proxy'] else None
        self.scrape_reviews = kwargs['scrape_reviews']
        self.scrape_products = kwargs['scrape_products']
        self.category = kwargs['category']

    def create_style_list(self) -> AmazonComStyleList:
        return AmazonComStyleList()

    def create_style(self) -> AmazonComStyle:
        return AmazonComStyle()

    def create_style_reviews(self) -> AmazonComStyleReviews:
        return AmazonComStyleReviews()

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
        for style in style_list:
            yield self.request(style['product_href'], callback=self.extract_style, style=style)
        next_page = response.selector.css('span.s-pagination-strip,ul.a-pagination') \
            .xpath('//li[last()]/a/@href | //a[last()]/@href').get()
        if next_page is not None and self.mode is ScrapeMode.Full:
            page = int(dict(parse_qsl(urlparse(response.url)[4])).get('page', 1))
            url = replace_query_param(response.url, 'page', page + 1)
            yield self.request(url, self.extract_style_list)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = response
        for selector in self.create_style().selectors():
            selection = selection.selector.xpath(selector)
        style = kwargs['style']    
        style = self.create_style().clean(selection, url=response.url, brand=self.brand, style=style,category=self.category)

        if self.scrape_products:
            yield self.request(url=f'https://www.amazon.com/hz/reviews-render/ajax/lazy-widgets/stream?asin={style["sku"]}&language=en_US&lazyWidget=cr-summarization-attributes',
                            callback=self.extract_aspects, style=style)

        if self.scrape_reviews:
            url = ItemAdapter(style).get('reviews_href')
            url = replace_query_param(url, 'pageNumber', '1')
            url = replace_query_param(url, 'sortBy', 'recent')
            yield self.request(url, self.extract_reviews)

    def extract_aspects(self, response, **kwargs):
        selection = response.text.split(",")
        rating_aspect = {}
        if len(selection) >= 2:
            selection = selection[2].strip()
            selection = Selector(text=selection)
            div_features_element = selection.xpath('.//div[contains(@id, "cr-summarization-attribute-attr")]/div').getall()
            for div_element in div_features_element:
                div_element = Selector(text=div_element)
                aspect, rating = div_element.xpath('.//span[contains(@class, "a-size-base")]/text()').getall()
                rating_aspect[aspect] = rating
        style = kwargs['style']
        style['rating_aspect'] = json.dumps(rating_aspect)
        yield style

    def extract_reviews(self, response, **kwargs):
        self.logger.info(f"Fetching style review from {response.url}")
        selection = response
        for selector in self.create_style_reviews().selectors():
            selection = selection.selector.xpath(selector)

        style_reviews_list = self.create_style_reviews().clean(selection, url=response.url, brand=self.brand,category=self.category)
        should_continue_next_reviews = True
        for style_reviews in style_reviews_list:
            date = ItemAdapter(style_reviews).get('date')
            date = datetime.strptime(date, "%Y%m%d")
            if date < datetime.now() - timedelta(days=self.review_capture_duration):
                should_continue_next_reviews = False
                break
            yield style_reviews
        if should_continue_next_reviews and len(style_reviews_list) >= 10:
            page = dict(parse_qsl(urlparse(response.url)[4]))['pageNumber']
            url = replace_query_param(response.url, 'pageNumber', int(page) + 1)
            url = replace_query_param(url, 'sortBy', 'recent')
            yield self.request(url, self.extract_reviews)

    def request(self, url, callback, **kwargs):
        meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Brand: " + self.brand)
        self.logger.info("mode: " + self.mode.name)
        self.logger.info("review_capture_duration: " + str(self.review_capture_duration))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
        self.logger.info("scrape_reviews: " + str(self.scrape_reviews))
        self.logger.info("scrape_products: " + str(self.scrape_products))