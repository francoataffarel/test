# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class StyleItem(scrapy.Item):
    sku = Field()
    brand = Field()
    category = Field()
    sub_categories = Field()
    name = Field()
    product_href = Field()
    mrp = Field()
    price = Field()
    discount = Field()
    rating = Field()
    rating_count = Field()
    rating_star = Field()
    rating_aspect = Field()
    extracted_date = Field()
    best_seller_rank = Field()
    available = Field()
    manufacturer = Field()
    channel = Field()
    reviews_href = Field()


class StyleReviewItem(scrapy.Item):
    id = Field()
    sku = Field()
    brand = Field()
    author = Field()
    star = Field()
    aspects = Field()
    title = Field()
    date = Field()
    verified = Field()
    description = Field()
    extracted_date = Field()
    channel = Field()
    upvotes = Field()
    category = Field()
