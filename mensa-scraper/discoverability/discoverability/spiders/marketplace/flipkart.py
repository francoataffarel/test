import logging
import re
import json
from datetime import datetime
from typing import List

from itemadapter import ItemAdapter
from scrapy import Request
from decouple import config

from ..utilities import replace_query_param, ChannelType, brand_formatted_name, SortOption
from ...items import DiscoverabilityStyleItem
from ..base import AbstractStyleList, AbstractMarketplace


class FlipkartStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        return ["//div[string-length(@data-id) > 0]"]

    def clean(self, selection, **kwargs) -> List[DiscoverabilityStyleItem]:
        style_list = []
        rank = kwargs['offset']
        rank_sponsored = kwargs['offset_sponsored']
        fsn_product_href = re.compile(r"^/[a-zA-Z0-9-]*/p/itm[a-zA-Z0-9]*\?pid=[A-Z0-9]{16}&lid=[A-Z0-9]{25}")
        for style_selection in selection:
            style_href = style_selection.xpath(".//div/a/@href").get()
            if not fsn_product_href.match(style_href):
                continue
            sponsored = style_selection.xpath(".//span[text()='Ad']").get() is not None
            style_href = f"https://www.flipkart.com{style_href}"
            skuid = style_selection.xpath('./@data-id').get()
            name = style_selection.xpath(".//div/a[@class='IRpwTa' or @class='IRpwTa _2-ICcC' or @class='s1Q9rs']/text()").get()
            brand = style_selection.xpath(".//div[@class='_2WkVRV']/text()").get()
            price = style_selection.xpath(".//div[@class='_30jeq3']/text()").get()
            mrp = style_selection.xpath(".//div[@class='_3I9_wc']/text()[2]").get()
            rating = '0'
            rating_count = '0'
            discount = '0'

            if price is None:
                price = '0'
            else:
                price = price.split(".")[0]
                price = re.sub(r'\D', '', price)

            if mrp is None:
                mrp = '0'
            else:
                mrp = mrp.split(".")[0]
                mrp = re.sub(r'\D', '', mrp)

            if mrp != '0':
                discount = str(int(mrp) - int(price))

            extracted_date = datetime.today().strftime('%Y%m%d')

            style_rank = '0' if sponsored else str(rank)

            style_item = DiscoverabilityStyleItem(skuid=skuid, brand=brand_formatted_name(brand), name=name,
                                                  product_href=style_href, mrp=mrp, price=price, discount=discount,
                                                  rating=rating, rating_count=rating_count, rank=style_rank,
                                                  page=str(kwargs['page']), sort=kwargs['sort'].name,
                                                  extracted_date=extracted_date, keyword=str(kwargs['keyword']),
                                                  channel=ChannelType.Flipkart, rank_sponsored=str(rank_sponsored),
                                                  sponsored=str(sponsored))
            style_list.append(style_item)
            rank = rank if sponsored else rank + 1
            rank_sponsored = rank_sponsored + 1
        return style_list


# Other sort options: price_desc, price_asc
sort_options = {
    SortOption.relevance.name: "relevance",
    SortOption.popularity.name: "popularity",
    SortOption.discount.name: "relevance",
    SortOption.new.name: "recency_desc"
}


def build_style_list_url(keyword: str, sort: SortOption) -> str:
    search_words = keyword.lower().split()
    url_search_text = '%20'.join(search_words)
    sort_option = sort_options[sort.name]
    return f'https://www.flipkart.com/search?q={url_search_text}&marketplace=FLIPKART&sort={sort_option}'


class Flipkart(AbstractMarketplace):
    smart_proxy = None
    scraping_items_limit = 200
    sort_option = None

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None
        self.scraping_items_limit = kwargs['scraping_items_limit']

    def parse_style_list(self) -> FlipkartStyleList:
        return FlipkartStyleList()

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Flipkart.name)
        return logging.LoggerAdapter(logger, {'discoverability_spider': self})

    def start_requests(self, nav_type, keyword: str, sort: SortOption):
        self.sort_option = sort
        self.print_config()
        url = build_style_list_url(keyword, sort)
        yield self.request(url, self.extract_style_list,
                           meta={'keyword': keyword, 'page': 1, 'offset': 1, 'offset_sponsored': 1})

    def extract_style_list(self, response, **kwargs):

        if 'keyword' not in response.meta or 'page' not in response.meta or 'offset' not in response.meta or 'offset_sponsored' not in response.meta:
            return

        selection = response
        for selector in self.parse_style_list().selectors():
            selection = selection.selector.xpath(selector)

        style_list = self.parse_style_list().clean(
            selection,
            keyword=response.meta['keyword'],
            page=response.meta['page'],
            offset=response.meta['offset'],
            offset_sponsored=response.meta['offset_sponsored'],
            sort=self.sort_option
        )
        self.logger.info(f"Fetching product listing from {response.url} count: {len(style_list)}")
        should_continue_next_styles = True
        offset = 0
        for style in style_list:
            rank = ItemAdapter(style).get('rank')
            brand = ItemAdapter(style).get('brand')
            if self.scraping_items_limit < int(rank):
                should_continue_next_styles = False
                break
            offset = max(int(rank), offset)
            if brand is None:
                url = ItemAdapter(style).get('product_href').split('ref=')[0]
                yield self.request(url, self.extract_style, meta={'style': style})
            else:
                yield style
        next_page = response.selector.xpath("//nav//span[text()='Next']").get()
        if should_continue_next_styles and next_page is not None:
            url = replace_query_param(response.url, 'page', response.meta['page'] + 1)
            yield self.request(url, self.extract_style_list,
                               meta={
                                   'keyword': response.meta['keyword'],
                                   'page': response.meta['page'] + 1,
                                   'offset': offset + 1,
                                   'offset_sponsored': response.meta['offset_sponsored'] + len(style_list)}
                               )

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = response
        selector_script = selection.selector.xpath('(//*[@id="jsonLD"])[1]/text()').get()
        selector_script = json.loads(selector_script)
        brand = None
        for selector in selector_script:
            if 'brand' in selector:
                brand = selector['brand']['name']
                break
        if brand is None:
            return
        rating = selection.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' _1YokD2 ')]"
                                 "//span[contains(@id,'productRating')]//div/text()").get()

        rating_count = selection.selector.xpath("(//div[contains(concat(' ',normalize-space(@class),' '),' _1YokD2 ')]"
                                                "//span[contains(text(),'Ratings')])[1]/text()").get()
        style = response.meta['style']
        style['brand'] = brand
        style['rating'] = rating
        rating_count_matches = re.findall("[^\s+]*", rating_count)
        style['rating_count'] = rating_count_matches[0] if len(rating_count_matches) > 0 else '0'
        yield style

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
