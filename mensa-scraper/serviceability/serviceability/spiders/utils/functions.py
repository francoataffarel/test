from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
from selenium.webdriver.common.keys import Keys

def remove_all_query_params(url):
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    query = urlencode({})
    return urlunparse((scheme, netloc, path, params, query, fragment))

def replace_query_param(url, attr, val):
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    query_dict = dict(parse_qsl(query))
    query_dict[attr] = val
    query = urlencode(query_dict)
    return urlunparse((scheme, netloc, path, params, query, fragment))

def get_query_param(url, key):
    return dict(parse_qsl(urlparse(url)[4]))[key]

def has_query_param(url, key):
    return key in dict(parse_qsl(urlparse(url)[4]))

def delete_query_param(url, key):
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    query_dict = dict(parse_qsl(query))
    del query_dict[key]
    query = urlencode(query_dict)
    return urlunparse((scheme, netloc, path, params, query, fragment))

def get_key(char):
    keys = {'0': Keys.NUMPAD0, '1': Keys.NUMPAD1,'2': Keys.NUMPAD2, '3': Keys.NUMPAD3
    ,'4': Keys.NUMPAD4,'5': Keys.NUMPAD5,'6': Keys.NUMPAD6,'7': Keys.NUMPAD7
    ,'8': Keys.NUMPAD8,'9': Keys.NUMPAD9}
    return keys[char]        