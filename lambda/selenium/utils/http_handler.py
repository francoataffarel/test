
def get_headers():
    response_headers = {
        'Content-Type': 'application/json'
    }
    return response_headers


def build_response(status, response_body):
    """
    Returns success_response and sets the headers for the response
    All Lambdas must use it.
    :param status code
    :param response_body object
    """
    response = {
        "statusCode": status,
        "headers": get_headers()
    }
    if response_body:
        response["body"] = response_body

    return response