# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class BestsellersItem(Item):
    skuid = Field()
    brand = Field()
    name = Field()
    channel = Field()
    product_href = Field()
    extracted_date = Field()
    category_1 = Field()
    category_2 = Field()
    category_3 = Field()
    category_4 = Field()
    rank_1 = Field()
    rank_2 = Field()
    rank_3 = Field()
    rank_4 = Field()
