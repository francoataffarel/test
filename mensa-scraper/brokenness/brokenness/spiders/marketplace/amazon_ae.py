from .amazon_in import AmazonIn

from ..uitilies import ChannelType


class AmazonAe(AmazonIn):

    def __init__(self, brand, **kwargs):
        super().__init__(brand, **kwargs)
        self.marketplace_url = 'https://www.amazon.ae'
        self.channel = ChannelType.AmazonAe
