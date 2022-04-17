import logging
import json
from datetime import datetime
from typing import List
from urllib.parse import urlparse, parse_qsl

from itemadapter import ItemAdapter
from scrapy import Request
from decouple import config

from ..utilities import replace_query_param, ChannelType, brand_formatted_name, SortOption
from ...items import DiscoverabilityStyleItem, BrowseStyleItem
from ..base import AbstractStyleList, AbstractMarketplace


class MyntraStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, selection, **kwargs) -> List[DiscoverabilityStyleItem]:
        style_list = []
        rank = kwargs['offset']
        for style_selection in selection:
            skuid = style_selection['productId']
            style_href = f"https://www.myntra.com/{skuid}"
            name = style_selection['additionalInfo']
            brand = style_selection['brand']
            price = style_selection['price']
            mrp = style_selection['mrp']
            rating = style_selection['rating']
            rating_count = style_selection['ratingCount']
            discount = '0'

            if price is None:
                price = 0

            if mrp is None:
                mrp = 0

            if mrp != 0:
                discount = str(mrp - price)

            extracted_date = datetime.today().strftime('%Y%m%d')

            style_item = DiscoverabilityStyleItem(skuid=str(skuid), brand=brand_formatted_name(brand), name=name,
                                                  product_href=style_href, mrp=str(mrp), price=str(price),
                                                  discount=discount, rating=str(rating), rating_count=str(rating_count),
                                                  rank=str(rank), page=str(kwargs['page']), sort=kwargs['sort'].name,
                                                  extracted_date=extracted_date, keyword=str(kwargs['keyword']),
                                                  channel=ChannelType.Myntra)
            style_list.append(style_item)
            rank = rank + 1
        return style_list


# Other sort options: price_desc, price_asc, Customer%20Rating
sort_options = {
    SortOption.relevance.name: "relevance",
    SortOption.popularity.name: "popularity",
    SortOption.discount.name: "discount",
    SortOption.new.name: "new"
}


def build_style_list_url(keyword: str) -> str:
    url_search_text = '-'.join(keyword.lower().split())
    return f'https://www.myntra.com/{url_search_text}'


def build_browse_style_list_url(url: str) -> str:
    url_search_text = '-'.join(url.lower().split())
    return f'https://www.myntra.com/{url_search_text}'


class Myntra(AbstractMarketplace):
    smart_proxy = None
    scraping_items_limit = 200
    sort_option = None
    page_size = 50

    # Browse based fields
    primary_page_name = None
    primary_scroll_times = None
    primary_nav = None
    primary_nav_type = None
    primary_nav_title = None
    primary_nav_subtitle = None
    primary_nav_url = None
    primary_nav_has_brand_name = None
    secondary_page_name = None
    secondary_scroll_times = None
    secondary_nav = None
    secondary_nav_type = None
    secondary_nav_title = None
    secondary_nav_subtitle = None
    secondary_nav_url = None
    secondary_nav_has_brand_name = None
    offer_type = None
    offer_title = None
    offer_start_date = None
    offer_end_date = None

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None
        self.scraping_items_limit = kwargs['scraping_items_limit']

        # browse based fields
        self.primary_page_name = kwargs['primary_page_name'] if 'primary_page_name' in kwargs else None
        self.primary_scroll_times = kwargs['primary_scroll_times'] if 'primary_scroll_times' in kwargs else None
        self.primary_nav = kwargs['primary_nav'] if 'primary_nav' in kwargs else None
        self.primary_nav_type = kwargs['primary_nav_type'] if 'primary_nav_type' in kwargs else None
        self.primary_nav_title = kwargs['primary_nav_title'] if 'primary_nav_title' in kwargs else None
        self.primary_nav_subtitle = kwargs['primary_nav_subtitle'] if 'primary_nav_subtitle' in kwargs else None
        self.primary_nav_url = kwargs['primary_nav_url'] if 'primary_nav_url' in kwargs else None
        self.primary_nav_has_brand_name = kwargs['primary_nav_has_brand_name'] if 'primary_nav_has_brand_name' in kwargs else None
        self.secondary_page_name = kwargs['secondary_page_name'] if 'secondary_page_name' in kwargs else None
        self.secondary_scroll_times = kwargs['secondary_scroll_times'] if 'secondary_scroll_times' in kwargs else None
        self.secondary_nav = kwargs['secondary_nav'] if 'secondary_nav' in kwargs else None
        self.secondary_nav_type = kwargs['secondary_nav_type'] if 'secondary_nav_type' in kwargs else None
        self.secondary_nav_title = kwargs['secondary_nav_title'] if 'secondary_nav_title' in kwargs else None
        self.secondary_nav_subtitle = kwargs['secondary_nav_subtitle'] if 'secondary_nav_subtitle' in kwargs else None
        self.secondary_nav_url = kwargs['secondary_nav_url'] if 'secondary_nav_url' in kwargs else None
        self.secondary_nav_has_brand_name = kwargs['secondary_nav_has_brand_name'] if 'secondary_nav_has_brand_name' in kwargs else None
        self.offer_type = kwargs['offer_type'] if 'offer_type' in kwargs else None
        self.offer_title = kwargs['offer_title'] if 'offer_title' in kwargs else None
        self.offer_start_date = kwargs['offer_start_date'] if 'offer_start_date' in kwargs else None
        self.offer_end_date = kwargs['offer_end_date'] if 'offer_end_date' in kwargs else None

    def parse_style_list(self) -> MyntraStyleList:
        return MyntraStyleList()

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Myntra.name)
        return logging.LoggerAdapter(logger, {'discoverability_spider': self})

    def start_requests(self, nav_type, keyword: str = None, sort: SortOption = None):
        self.print_config()
        if nav_type == 'browse_spider':
            url = self.primary_nav_url
            if self.secondary_nav_url:
                url = self.secondary_nav_url
            yield self.request(url, self.start_request_cookies_wrapper_browse)
        else:
            self.sort_option = sort
            url = build_style_list_url(keyword)
            yield self.request(url, self.start_request_cookies_wrapper)

    def start_request_cookies_wrapper(self, response, **kwargs):
        search_text_path = response.url.split('/')[-1]
        sort_option = sort_options[self.sort_option.name]
        api_url = f'https://www.myntra.com/gateway/v2/search/{search_text_path}?sort={sort_option}&p=1&rows=50&o=0&plaEnabled=false'
        yield self.request(api_url, self.extract_style_list)

    def start_request_cookies_wrapper_browse(self, response, **kwargs):
        search_text_path = response.url.split('myntra.com/')[1]
        connect = '?'
        if '?' in search_text_path:
            connect = '&'
        api_url = f'https://www.myntra.com/gateway/v2/search/{search_text_path}{connect}o=0&ifo=0&ifc=0&rows=50&requestType=ANY&plaEnabled=false&priceBuckets=20'
        yield self.request(api_url, self.extract_style_list_browse, meta={'page': 1})

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        if 'products' not in selection:
            return
        url = response.url
        page = int(dict(parse_qsl(urlparse(url)[4])).get('p', 1))
        keyword = urlparse(url)[2].split('/')[-1].replace('-', ' ')
        style_list = self.parse_style_list().clean(
            selection['products'],
            keyword=keyword,
            page=page,
            offset=(page - 1) * self.page_size + 1,
            sort=self.sort_option
        )

        self.logger.info(f"Fetching product listing from {response.url} count: {len(style_list)}")

        should_continue_next_styles = True
        for style in style_list:
            rank = ItemAdapter(style).get('rank')
            if self.scraping_items_limit < int(rank):
                should_continue_next_styles = False
                break
            yield style

        num_items = selection['totalCount']

        if should_continue_next_styles and num_items and page * self.page_size < num_items:
            url = replace_query_param(url, 'p', page + 1)
            url = replace_query_param(url, 'o', (page * self.page_size) - 1)
            yield self.request(url, self.extract_style_list)

    def extract_style_list_browse(self, response, **kwargs):
        selection = json.loads(response.text)
        if 'products' not in selection:
            return
        url = response.url
        page = response.meta['page']
        num_items = selection['totalCount']

        should_continue_next_styles = True
        products = selection['products']
        self.logger.info(f"Fetching product listing from {response.url} count: {len(products)}")

        rank = (page - 1) * self.page_size + 1
        for style_selection in products:
            if self.scraping_items_limit < int(rank):
                should_continue_next_styles = False
                break
            skuid = style_selection['productId']
            style_href = f"https://www.myntra.com/{skuid}"
            name = style_selection['additionalInfo']
            brand = style_selection['brand']
            price = style_selection['price']
            mrp = style_selection['mrp']
            rating = style_selection['rating']
            rating_count = style_selection['ratingCount']
            discount = '0'

            if price is None:
                price = 0

            if mrp is None:
                mrp = 0

            if mrp != 0:
                discount = str(mrp - price)

            extracted_date = datetime.today().strftime('%Y%m%d')

            style_item = BrowseStyleItem(
                skuid=str(skuid), brand=brand_formatted_name(brand), name=name,
                product_href=style_href, mrp=str(mrp), price=str(price),
                discount=discount, rating=str(rating), rating_count=str(rating_count),
                rank=str(rank), page=str(page),
                extracted_date=extracted_date,
                channel=ChannelType.Myntra,
                primary_page_name=self.primary_page_name if self.primary_page_name else '',
                primary_scroll_times=self.primary_scroll_times if self.primary_scroll_times else '',
                primary_nav=self.primary_nav if self.primary_nav else '',
                primary_nav_type=self.primary_nav_type if self.primary_nav_type else '',
                primary_nav_title=self.primary_nav_title if self.primary_nav_title else '',
                primary_nav_subtitle=self.primary_nav_subtitle if self.primary_nav_subtitle else '',
                primary_nav_url=self.primary_nav_url if self.primary_nav_url else '',
                primary_nav_has_brand_name=str(self.primary_nav_has_brand_name),
                secondary_page_name=self.secondary_page_name if self.secondary_page_name else '',
                secondary_scroll_times=self.secondary_scroll_times if self.secondary_scroll_times else '',
                secondary_nav=self.secondary_nav if self.secondary_nav else '',
                secondary_nav_type=self.secondary_nav_type if self.secondary_nav_type else '',
                secondary_nav_title=self.secondary_nav_title if self.secondary_nav_title else '',
                secondary_nav_subtitle=self.secondary_nav_subtitle if self.secondary_nav_subtitle else '',
                secondary_nav_url=self.secondary_nav_url if self.secondary_nav_url else '',
                secondary_nav_has_brand_name=str(self.secondary_nav_has_brand_name),
                offer_type=self.offer_type if self.offer_type else '',
                offer_title=self.offer_title if self.offer_title else '',
                offer_start_date=self.offer_start_date if self.offer_start_date else '',
                offer_end_date=self.offer_end_date if self.offer_end_date else ''
            )
            rank = rank + 1
            yield style_item

        if should_continue_next_styles and num_items and page * self.page_size < num_items:
            url = replace_query_param(url, 'o', page * self.page_size)
            yield self.request(url, self.extract_style_list_browse, meta={'page': page + 1})

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("scraping_items_limit: " + str(self.scraping_items_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
