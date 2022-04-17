from abc import ABC, abstractmethod
from typing import List

from ...items import StyleItem, StyleReviewItem


class AbstractStyleList(ABC):

    @abstractmethod
    def selectors(self, **kwargs) -> List[str]:
        """
        Method to define a container level xpath selectors
        :param kwargs:
        :return: Return list of xpath selectors
        """
        pass

    @abstractmethod
    def clean(self, selection, **kwargs):
        """
        Method to apply a container level xpath selector to extract a list of product urls
        :param selection:
        :param kwargs:
        :return: list of product urls or list of dictionary having product information
        """
        pass


class AbstractStyle(ABC):

    @abstractmethod
    def selectors(self, **kwargs) -> List[str]:
        """
        Method to define a container level xpath selectors
        :param kwargs:
        :return: Return list of xpath selectors
        """
        pass

    @abstractmethod
    def clean(self, selection, **kwargs) -> StyleItem:
        """
        Method to apply a container level xpath selector to extract a Style object
        :param selection:
        :param kwargs:
        :return: StyleItem
        """
        pass


class AbstractStyleReviews(ABC):

    @abstractmethod
    def selectors(self, **kwargs) -> List[str]:
        """
        Method to define a container level xpath selectors
        :param kwargs:
        :return: Return list of xpath selectors
        """
        pass

    @abstractmethod
    def clean(self, selection, **kwargs) -> List[StyleReviewItem]:
        """
        Method to apply a container level xpath selector to extract a list of StyleReviewItem object
        :param selection:
        :param kwargs:
        :return: StyleReviewItem
        """
        pass


class MarketPlaceFactory(ABC):
    """
    This Abstract Factory interface declares a set of methods that return
    different abstract products (StyleList, Style, StyleReview).
    """

    @abstractmethod
    def create_style_list(self) -> AbstractStyleList:
        """
        Creator method for return a concrete implementation of AbstractStyleList
        :return: AbstractStyleList
        """
        pass

    @abstractmethod
    def create_style(self) -> AbstractStyle:
        """
        Creator method to return a concrete implementation of AbstractStyle
        :return: AbstractStyle
        """
        pass

    @abstractmethod
    def create_style_reviews(self) -> AbstractStyleReviews:
        """
        Creator method to return a concrete implementation of AbstractStyleReviews
        :return: AbstractStyleReviews
        """
        pass

    @abstractmethod
    def start_request(self, start_urls: List[str]):
        """
        Method to start the crawling process. Called only once.
        :param start_urls: List of initial URLs which are added in LIFO queue
        """
        pass

    @abstractmethod
    def extract_style_list(self, response, **kwargs):
        """
        Method to extract list of styles from a brand/listing/search pages of a marketplace
        Should yield request for individual styles and next pages if paginated
        :param response: response from spider
        :param kwargs: keyword arguments
        """
        pass

    @abstractmethod
    def extract_style(self, response, **kwargs):
        """
        Method to extract styles object PDP of a marketplace
        Should yield request for all reviews for this product
        Should yield item of type StyleItem
        :param response: response from spider
        :param kwargs: keyword arguments
        """
        pass

    @abstractmethod
    def extract_reviews(self, response, **kwargs):
        """
        Method to extract reviews from all reviews page for a product of a marketplace
        Should yield next pages if paginated
        Should yield item of type StyleReviewItem
        :param response: response from spider
        :param kwargs: keyword arguments
        """
        pass


class MarketplaceBotMiddleware(ABC):
    @abstractmethod
    def is_bot_discovered(self, response) -> bool:
        """
        Method to check if our bots has been detected by marketplace
        :param response:
        :return: true if bot detected else false
        """
        pass
