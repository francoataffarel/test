import logging
import re
from datetime import datetime
from typing import List
from urllib.parse import unquote

from itemadapter import ItemAdapter
from scrapy import Request
from decouple import config

from ..utilities import replace_query_param, ChannelType, brand_formatted_name, SortOption
from ...items import DiscoverabilityStyleItem
from ..base import AbstractStyleList, AbstractMarketplace


class AmazonInDiscStyleList(AbstractStyleList):

    def selectors(self, **kwargs) -> List[str]:
        return ["//span[@data-component-type='s-search-results']/div/div[string-length(@data-asin) > 0]"]

    def clean(self, selection, **kwargs) -> List[DiscoverabilityStyleItem]:
        style_list = []
        rank = kwargs['offset']
        rank_sponsored = kwargs['offset_sponsored']
        asin_product_href = re.compile("^/[a-zA-Z0-9-_]*/dp/[A-Z0-9]{10}")
        for style_selection in selection:
            style_href = style_selection.xpath(".//h2/a/@href").get()
            sponsored = False
            if not asin_product_href.match(style_href):
                asin = re.findall("(?<=dp\/).*", unquote(style_href))[0][:10]
                style_href = f"/dp/{asin}/"
                sponsored = True
            style_href = f"https://www.amazon.in{style_href}"
            skuid = style_selection.xpath('./@data-asin').get()
            name = style_selection.xpath('.//h2//span/text()').get()
            brand = style_selection.xpath('.//h5//span/text()').get()
            price = style_selection.xpath(".//span[@class='a-price']//span[@class='a-offscreen']/text()").get()
            mrp = style_selection.xpath(".//span[@class='a-price a-text-price']//span[@class='a-offscreen']/text()").get()
            rating = style_selection.xpath(".//span[@class='a-icon-alt']/text()").get()
            rating_count = style_selection.xpath(".//div[contains(@class,'a-row') and contains(@class, 'a-size-small')]/span[2]//span/text()").get()
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

            if rating is not None:
                rating = rating.replace(' out of 5 stars', '')
            else:
                rating = '0'

            if rating_count is not None:
                rating_count = re.sub(r'\D', '', rating_count)
            else:
                rating_count = '0'
            extracted_date = datetime.today().strftime('%Y%m%d')

            style_rank = '0' if sponsored else str(rank)

            style_item = DiscoverabilityStyleItem(skuid=skuid, brand=brand_formatted_name(brand), name=name,
                                                  product_href=style_href, mrp=mrp, price=price, discount=discount,
                                                  rating=rating, rating_count=rating_count, rank=style_rank,
                                                  page=str(kwargs['page']), sort=kwargs['sort'].name,
                                                  extracted_date=extracted_date, keyword=str(kwargs['keyword']),
                                                  channel=ChannelType.AmazonIn, rank_sponsored=str(rank_sponsored),
                                                  sponsored=str(sponsored))
            style_list.append(style_item)
            rank = rank if sponsored else rank + 1
            rank_sponsored = rank_sponsored + 1
        return style_list


# Other sort options: sr_st_price-desc-rank&s=price-desc-rank, sr_st_price-asc-rank&s=price-asc-rank
sort_options = {
    SortOption.relevance.name: "sr_st_relevanceblender&s=relevanceblender",
    SortOption.popularity.name: "sr_st_review-rank&s=review-rank",
    SortOption.discount.name: "sr_st_relevanceblender&s=relevanceblender",
    SortOption.new.name: "sr_st_date-desc-rank&s=date-desc-rank"
}


def build_style_list_url(keyword: str, sort: SortOption) -> str:
    search_words = keyword.lower().split()
    url_search_text = '+'.join(search_words)
    sort_option = sort_options[sort.name]
    return f'https://www.amazon.in/s?k={url_search_text}&ref={sort_option}'


class AmazonIn(AbstractMarketplace):
    smart_proxy = None
    scraping_items_limit = 200
    sort_option = None

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_SESSION', None) if kwargs['proxy'] else None
        self.scraping_items_limit = kwargs['scraping_items_limit']

    def parse_style_list(self) -> AmazonInDiscStyleList:
        return AmazonInDiscStyleList()

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.AmazonIn.name)
        return logging.LoggerAdapter(logger, {'discoverability_spider': self})

    def start_requests(self, nav_type, keyword: str, sort: SortOption):
        self.sort_option = sort
        self.print_config()
        url = build_style_list_url(keyword, sort)
        yield self.request(url, self.extract_style_list,
                           meta={'keyword': keyword, 'page': 1, 'offset': 1, 'offset_sponsored': 1})

    def extract_style_list(self, response, **kwargs):
        selection = response
        for selector in self.parse_style_list().selectors():
            selection = selection.selector.xpath(selector)

        if 'keyword' not in response.meta or 'page' not in response.meta or 'offset' not in response.meta:
            return

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
        next_page = response.selector.css('span.s-pagination-strip,ul.a-pagination')\
            .xpath('//li[last()]/a/@href | //a[last()]/@href').get()
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
        brand = selection.selector.xpath('//*[@id="bylineInfo"]/text() | //*[@id="amznStoresBylineLogoTextContainer"]/a').get().strip()
        matches = re.findall("(?<=Brand: ).*|(?<=Visit the ).*(?= Store)", brand)
        style = response.meta['style']
        style['brand'] = matches[0] if len(matches) > 0 else brand
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
