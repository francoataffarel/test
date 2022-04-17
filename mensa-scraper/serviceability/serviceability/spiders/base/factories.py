from abc import ABC, abstractmethod
from typing import List

class MarketPlaceFactory(ABC):
    """
    This interface declares a set of methods that return
    different SERIVEABILITY items.
    """
    @abstractmethod
    def start_request(self):
        """
        Method to start the crawling process. Called only once.
        """
        pass

    def extract_serviceability_list(self, response, **kwargs):
        """
        Method to extract list of serivceability items for each product_url
        :param response: response from spider
        :param kwargs: keyword arguments
        """
    
    @abstractmethod
    def extract_serviceability_item(self, response, **kwargs):
        """
        Method to extract ServiceabilityItem object from the response
        Should yield item of type ServiceabilityItem
        :param response: response from spider
        :param kwargs: keyword arguments
        """

class MarketplaceBotMiddleware(ABC):
    @abstractmethod
    def is_bot_discovered(self, response) -> bool:
        """
        Method to check if our bots has been detected by marketplace
        :param response:
        :return: true if bot detected else false
        """
        pass