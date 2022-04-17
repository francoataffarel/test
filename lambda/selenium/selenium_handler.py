from utils import build_response, ExceptionHandler
from http import HTTPStatus
from projects import Serviceability
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function for each selenium request
    """
    query_params = event['queryStringParameters']
    if not query_params or 'marketplace' not in query_params or 'project' not in query_params:
        error_response = {
            "message": "marketplace, project are required query string paramaters."
        }
        return build_response(status=HTTPStatus.BAD_REQUEST, response_body=json.dumps(error_response))
    
    project=query_params['project']
    
    try:
        if "serviceability" == project:
            if 'product_url' not in query_params or 'pincode' not in query_params:
                response_body = {
                    "message": "product_url or pincode is missing"
                }
                raise ExceptionHandler(code=HTTPStatus.BAD_REQUEST, message=json.dumps(response_body))

            response = Serviceability().marketplace_handler(**query_params)
            return build_response(status=HTTPStatus.OK, response_body=json.dumps(response))
        
        raise ExceptionHandler(code=HTTPStatus.BAD_REQUEST, message=json.dumps({"message": "No project found"}))

    except ExceptionHandler as exception:
        logger.error(exception.message)
        return build_response(status=exception.code, response_body=exception.message)