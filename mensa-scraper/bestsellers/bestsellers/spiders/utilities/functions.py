from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl


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


def brand_formatted_name(brand_name):
    return brand_name.title()
