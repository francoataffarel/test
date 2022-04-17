from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
import re
from .constants import brand_synonyms

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


def retain_query_param(url, *args):
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    query_dict = dict(parse_qsl(query))
    new_dict = {arg: query_dict[arg] for arg in args}
    query = urlencode(new_dict)
    return urlunparse((scheme, netloc, path, params, query, fragment))

def get_query_param(url, key):
    return dict(parse_qsl(urlparse(url)[4]))[key]

def has_query_param(url, key):
    return key in dict(parse_qsl(urlparse(url)[4]))

def clean_amazon_price_value(value):
    if value:
        value = re.findall(r"[-+]?\d*\.?\d+|[-+]?\d+", value)
        if len(value) >= 0:
            # in case of 1,999 need to join 1 and 999
            value = ''.join(value)
        else:
            value = '0'
    else:
        value = '0'
    return value

def get_brand_formatted_name(brand_name):
    if brand_name is None:
        return brand_name
    if brand_name.lower() in brand_synonyms:
        brand_name = brand_synonyms[brand_name.lower()]
    return brand_name.title()        