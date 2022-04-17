import logging
import json
from datetime import datetime
from typing import List
from urllib.parse import urlparse, parse_qsl, unquote


from itemadapter import ItemAdapter
from scrapy import Request
from decouple import config

from ..utilities import replace_query_param, ChannelType, brand_formatted_name, SortOption
from ...items import DiscoverabilityStyleItem
from ..base import AbstractStyleList, AbstractMarketplace


class AjioStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        pass

    def clean(self, selection, **kwargs) -> List[DiscoverabilityStyleItem]:
        style_list = []
        rank = kwargs['offset']
        for style_selection in selection:
            skuid = style_selection['code']
            style_href = style_selection['url']
            style_href = f"https://www.ajio.com{style_href}"
            name = style_selection['name']
            brand = style_selection['fnlColorVariantData']['brandName']
            price = style_selection['price']['value']
            mrp = style_selection['wasPriceData']['value']
            rating = '0'
            rating_count = '0'
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
                                                  channel=ChannelType.Ajio)
            style_list.append(style_item)
            rank = rank + 1
        return style_list


# Other sort options: prce-desc, prce-asc
sort_options = {
    SortOption.relevance.name: "relevance",
    SortOption.popularity.name: "relevance",
    SortOption.discount.name: "discount-desc",
    SortOption.new.name: "newn"
}


def build_style_list_url(keyword: str, sort: SortOption) -> str:
    url_search_text = '%20'.join(keyword.lower().split())
    sort_option = sort_options[sort.name]
    return f"https://www.ajio.com/api/search?fields=SITE&currentPage=0&pageSize=45&format=json&query={url_search_text}%3A{sort_option}&sortBy={sort_option}&text={url_search_text}&gridColumns=5&facets=&advfilter=true&platform=site"


class Ajio(AbstractMarketplace):
    smart_proxy = None
    scraping_items_limit = 200
    sort_option = None
    page_size = 45

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None
        self.scraping_items_limit = kwargs['scraping_items_limit']

    def parse_style_list(self) -> AjioStyleList:
        return AjioStyleList()

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Ajio.name)
        return logging.LoggerAdapter(logger, {'discoverability_spider': self})

    def start_requests(self, nav_type, keyword: str, sort: SortOption):
        self.sort_option = sort
        self.print_config()
        url = build_style_list_url(keyword, sort)
        yield self.request(url, self.extract_style_list)

    def extract_style_list(self, response, **kwargs):
        selection = json.loads(response.text)
        if 'products' not in selection:
            return
        url = response.url
        page = int(dict(parse_qsl(urlparse(url)[4])).get('currentPage', 0))
        keyword = unquote(dict(parse_qsl(urlparse(url)[4])).get('text'))
        if keyword == '' or keyword is None:
            return
        style_list = self.parse_style_list().clean(
            selection['products'],
            keyword=keyword,
            page=page + 1,
            offset=page * self.page_size + 1,
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

        if "pagination" not in selection:
            return
        num_items = selection['pagination']['totalResults']

        if should_continue_next_styles and num_items and (page + 1) * self.page_size < num_items:
            url = replace_query_param(url, 'currentPage', page + 1)
            yield self.request(url, self.extract_style_list)

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
