# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class HotstylesItem(Item):
    # define the fields for your item here like:
    skuid = Field()
    name = Field()
    channel = Field()
    product_href = Field()
    extracted_date = Field()
    mrp = Field()
    price = Field()
    discount = Field()
    rating = Field()
    rating_count = Field()
    category_1 = Field()
    category_2 = Field()
    category_3 = Field()
    category_4 = Field()
    rank_1 = Field()
    rank_2 = Field()
    rank_3 = Field()
    rank_4 = Field()
