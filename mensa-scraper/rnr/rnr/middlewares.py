# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import logging
import random
from http.cookies import SimpleCookie

from scrapy import signals
from scrapy.utils.response import response_status_message
from scrapy_fake_useragent.middleware import RetryUserAgentMiddleware, RandomUserAgentMiddleware
from twisted.internet import reactor
from twisted.internet.defer import Deferred

from .spiders.base import MarketplaceBotMiddleware
from .spiders.utils.constants import custom_fake_user_agents, flipkart_agent

logger = logging.getLogger(__name__)


class RnrSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class RnrDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


# This custom retry extends RetryUserAgentMiddleware which added custom user agents on every retry
class CustomRetryMiddleware(RetryUserAgentMiddleware):
    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            # Added a parameter to relay request. The actual delay will be handle by DelayedRequestsMiddleware
            request.meta['delay_request'] = 15
            # Since we are overriding RetryUserAgentMiddleware.process_response(), we need to add user-agent here.
            request.headers['User-Agent'] = self._ua_provider.get_random_ua()
            return self._retry(request, reason, spider) or response

        return response


# This custom random agent extends RandomUserAgentMiddleware and adds random user agents on every myntra request
class CustomRandomUserAgentMiddleware(RandomUserAgentMiddleware):
    def process_request(self, request, spider):
        if 'myntra.com' in request.url:
            request.headers['User-Agent'] = random.choice(custom_fake_user_agents)
        elif 'flipkart.com' in request.url:
            request.headers['X-User-Agent'] = flipkart_agent


class DelayedRequestsMiddleware(object):
    def process_request(self, request, spider):
        delay = request.meta.get('delay_request', None)
        if delay:
            logger.info(f"Deferred the request by {delay} secs - {request.url}")
            d = Deferred()
            reactor.callLater(delay, d.callback, None)
            return d


class BotMiddleware(RetryUserAgentMiddleware):
    @staticmethod
    def _get_bot_middleware(url):
        if 'amazon.ae' in url:
            return AmazonAeMiddleware()
        elif 'amazon' in url:
            return AmazonMiddleware()
        elif 'flipkart.com' in url and 'rome.api' not in url:
            return FlipkartBotMiddleware()
        elif 'myntra.com' or 'nykaa.com' or 'nykaafashion.com' in url or 'rome.api' in url:
            return ApiBotMiddleware()

    def process_response(self, request, response, spider):
        bot = self._get_bot_middleware(request.url)
        if bot.is_bot_discovered(response):
            logger.info(f"This BOT got discovered, Will retry again - {request.url}")
            reason = 'bot_detected'
            # # Added a parameter to relay request. The actual delay will be handle by DelayedRequestsMiddleware
            request.meta['delay_request'] = 2
            # Since we are overriding RetryUserAgentMiddleware.process_response(), we need to add user-agent here.
            request.headers['User-Agent'] = self._ua_provider.get_random_ua()
            return self._retry(request, reason, spider) or response
        return response


# When using API, bot won't detect us. Even if they do, they will never send 200 OK response.
class ApiBotMiddleware(MarketplaceBotMiddleware):
    def is_bot_discovered(self, response) -> bool:
        return False


class FlipkartBotMiddleware(MarketplaceBotMiddleware):
    def is_bot_discovered(self, response) -> bool:
        legacy_browser = "//div[@id='legacy-browser-warning']"
        something_went_wrong = "//div[@id='container']/div"
        something_not_right = "//div[@class='_2RZvAZ']/text()"
        if response.selector.xpath(legacy_browser).get() is not None:
            return True
        if response.selector.xpath(something_went_wrong).get() is None:
            return True
        if response.selector.xpath(something_not_right).get() == "Something's not right!":
            return True
        return False


class AmazonMiddleware(MarketplaceBotMiddleware):
    def is_bot_discovered(self, response) -> bool:
        bot_container = response.selector.css('.a-last').xpath('./text()').get()
        error_response = response.selector.xpath('.//div[@class="a-alert-content"]/text()').get()
        if bot_container and "Sorry, we just need to make sure you're not a robot" in bot_container:
            return True
        if error_response and "We're sorry, an error has occurred. Please reload this page and try again." in error_response:
            return True
        return False

class AmazonAeMiddleware(MarketplaceBotMiddleware):
    def is_bot_discovered(self, response) -> bool:
        bot_container = response.selector.css('.a-last').xpath('./text()').get()
        if bot_container:
            return True
        return False


class RemoveUnwantedCookieMiddleware(object):
    def process_request(self, request, spider):
        try:
            if 'myntra.com' in request.url:
                raw_cookie = request.headers.pop('Cookie', None)
                if raw_cookie is not None:
                    cookie = SimpleCookie()
                    cookie.load(raw_cookie[0].decode("utf-8"))
                    cookies = {}
                    for key, morsel in cookie.items():
                        cookies[key] = morsel.value
                    cookies.pop('ak_bmsc')
                    cookie_string = "; ".join([str(x) + "=" + str(y) for x, y in cookies.items()])
                    request.headers['Cookie'] = cookie_string
        except Exception as e:
            pass
