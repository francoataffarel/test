# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item
from scrapy import Field


class PricingItem(Item):
    skuid = Field()
    brand = Field()
    category = Field()
    title = Field()
    product_href = Field()
    mrp = Field()
    price = Field()
    discount = Field()
    discount_percentage = Field()
    extracted_date = Field()
    bank_offers = Field()
    coupon_offers = Field()
    available = Field()
    marketplace = Field()
    
