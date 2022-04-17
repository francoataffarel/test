from abc import ABC, abstractmethod
from ..marketplace import AmazonIn, Myntra, Flipkart, AmazonCom, Nykaa, NykaaFashion, Ajio


def disc_marketplace_factory(domain, **kwargs):

    marketplace = {
        "amazon.in": AmazonIn,
        "myntra.com": Myntra,
        "flipkart.com": Flipkart,
        "amazon.com": AmazonCom,
        "nykaa.com": Nykaa,
        "nykaafashion.com": NykaaFashion,
        "ajio.com": Ajio,
    }
    return marketplace[domain](**kwargs)


class MarketplaceBotMiddleware(ABC):
    @abstractmethod
    def is_bot_discovered(self, response) -> bool:
        """
        Method to check if our bots has been detected by marketplace
        :param response:
        :return: true if bot detected else false
        """
        pass
