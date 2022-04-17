from abc import ABC, abstractmethod
from datetime import datetime


class BaseScraper(ABC):
    """
    This class provides different set of methods to extract parameters for scrapyd-scheduler request
    """

    def clean_arguments(self, arguments) -> dict:
        """
        Returns a dictionary of arguments as key value pairs from line seperate strings
        :param arguments in string line seperate
        """
        arguments = arguments.split('\n')
        args_dict = {}
        for args in arguments:
            args = args.strip()
            args = args.split('=', 1)
            args_dict[args[0]] = args[1]
        return args_dict

    @abstractmethod
    def get_arguments(self, marketplace, arguments, **kwargs) -> dict:
        """
        :param marketplace marketplace of the scheduler
        :param arguments
        Return the dictionary of arguments
        """

    @abstractmethod
    def get_settings(self, marketplace) -> str:
        """
        :param marketplace of the scheduler
        Return the settings for scraping job
        """


class RNRScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.ticket_review_request='ticket_review_request' in kwargs
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"rnr-{marketplace}-{self.arguments_dict['brand']}-{str(datetime.now())}".replace(" ", "")
        
    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['project'] = 'rnr'
        arguments_dict['spider'] = 'rnr_spider'
        arguments_dict['brand'] = kwargs['name']
        if self.ticket_review_request:
            arguments_dict['review_capture_duration'] = 7
            arguments_dict['scrape_products'] = 0
        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"


class ServiceabilityScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"serviceability-{marketplace}-{self.arguments_dict['brand']}-{str(datetime.now())}".replace(" ", "")

    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['project'] = 'serviceability'
        arguments_dict['spider'] = 'serviceability_spider'
        arguments_dict['brand'] = kwargs['name']
        if 'flipkart.com' == marketplace or 'amazon.in' == marketplace:
            arguments_dict['is_selenium_request'] = True
        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"


class PricingScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"pricing-{marketplace}-{kwargs['name']}-{str(datetime.now())}".replace(" ", "")

    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['project'] = 'pricing'
        arguments_dict['spider'] = 'pricing_spider'
        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"


class DiscoverabilityScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"discoverability-{marketplace}-{self.arguments_dict['name']}-{str(datetime.now())}".replace(" ", "")

    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['project'] = 'discoverability'
        arguments_dict['spider'] = 'discoverability_spider'
        arguments_dict["name"] = kwargs['name']

        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"


class BestSellersScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"bestsellers-{marketplace}-{self.arguments_dict['brand']}-{str(datetime.now())}".replace(" ", "")

    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['project'] = 'bestsellers'
        arguments_dict['spider'] = 'bestsellers_spider'

        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"


class HotstylesScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"hotstyles-{marketplace}-{str(datetime.now())}".replace(" ", "")

    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['project'] = 'hotstyles'
        arguments_dict['spider'] = 'hotstyles_spider'

        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"


class BrokennessScraper(BaseScraper):
    def __init__(self, marketplace, arguments, **kwargs):
        self.arguments_dict = self.get_arguments(marketplace, arguments, **kwargs)
        self.settings = self.get_settings(marketplace)
        self.job_name = f"brokenness-{marketplace}-{self.arguments_dict['brand']}-{str(datetime.now())}".replace(" ", "")

    def get_arguments(self, marketplace, arguments, **kwargs):
        arguments_dict = self.clean_arguments(arguments)
        arguments_dict['marketplace'] = marketplace
        arguments_dict['brand'] = kwargs['name']
        arguments_dict['project'] = 'brokenness'
        arguments_dict['spider'] = 'brokenness_spider'

        if 'flipkart.com' == marketplace or 'nykaa.com' == marketplace or 'nykafashion.com' == marketplace \
                or 'ajio.com' == marketplace:
            arguments_dict['proxy'] = 0
        return arguments_dict

    def get_settings(self, marketplace) -> str:
        if marketplace == "myntra.com":
            return "COOKIES_ENABLED=True"
        return "COOKIES_ENABLED=False"
