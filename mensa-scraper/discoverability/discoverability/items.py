# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class DiscoverabilityStyleItem(Item):
    skuid = Field()
    brand = Field()
    name = Field()
    product_href = Field()
    mrp = Field()
    price = Field()
    discount = Field()
    rating = Field()
    rating_count = Field()
    extracted_date = Field()
    channel = Field()
    rank = Field()
    page = Field()
    keyword = Field()
    sort = Field()
    sponsored = Field()
    rank_sponsored = Field()


class BrowseStyleItem(Item):
    skuid = Field()
    brand = Field()
    name = Field()
    product_href = Field()
    mrp = Field()
    price = Field()
    discount = Field()
    rating = Field()
    rating_count = Field()
    extracted_date = Field()
    channel = Field()
    rank = Field()
    page = Field()
    sponsored = Field()
    rank_sponsored = Field()
    primary_page_name = Field()
    primary_scroll_times = Field()
    primary_nav = Field()
    primary_nav_type = Field()
    primary_nav_title = Field()
    primary_nav_subtitle = Field()
    primary_nav_url = Field()
    primary_nav_has_brand_name = Field()
    secondary_page_name = Field()
    secondary_scroll_times = Field()
    secondary_nav = Field()
    secondary_nav_type = Field()
    secondary_nav_title = Field()
    secondary_nav_subtitle = Field()
    secondary_nav_url = Field()
    secondary_nav_has_brand_name = Field()
    offer_type = Field()
    offer_title = Field()
    offer_start_date = Field()
    offer_end_date = Field()
