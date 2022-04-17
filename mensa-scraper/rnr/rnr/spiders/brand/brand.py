from ..base import MarketPlaceFactory
from ..marketplace import AmazonIn, Flipkart, AmazonCom, Myntra, Nykaa, NykaaFashion, AmazonAe, AmazonCa


class Brand:
    marketplace: MarketPlaceFactory
    brand: None

    def __init__(self, brand, marketplace, **kwargs):
        self.brand = brand
        if 'amazon.in' == marketplace:
            self.marketplace = AmazonIn(brand, **kwargs)
        elif 'flipkart.com' == marketplace:
            self.marketplace = Flipkart(brand, **kwargs)
        elif 'amazon.com' == marketplace:
            self.marketplace = AmazonCom(brand, **kwargs)
        elif 'amazon.ca' == marketplace:
            self.marketplace = AmazonCa(brand, **kwargs)
        elif 'amazon.ae' == marketplace:
            self.marketplace = AmazonAe(brand, **kwargs)
        elif 'myntra.com' == marketplace:
            self.marketplace = Myntra(brand, **kwargs)
        elif 'nykaa.com' == marketplace:
            self.marketplace = Nykaa(brand, **kwargs)
        elif 'nykaafashion.com' == marketplace:
            self.marketplace = NykaaFashion(brand, **kwargs)
