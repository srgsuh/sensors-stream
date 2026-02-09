from helpers.logs import get_logger
from helpers.config import  ConfigurationError, InternalServerError
from cognito_auth import AuthError, authenticate_user
from ingress_helpers import (
    InvalidRequestError, UnsupportedEndpointError,
    build_response, publish_sns_message, validate_path_and_method, get_request_body, get_path_and_method
)

APP_PATH = "/api/v1/sensors"
HEALTH_PATH = "/health"

logger = get_logger(__name__)

def lambda_handler(event, context):
    try:
        logger.debug("EVENT: %s", event)
        response = build_response(200, "Accepted")
        path, method = get_path_and_method(event)
        if path == HEALTH_PATH:
            return build_response(200, "Healthy")

        validate_path_and_method(path, method, APP_PATH, ["POST"])
        authenticate_user(event)
        request_body = get_request_body(event)
        publish_sns_message(request_body)
        logger.debug("RESPONSE: ", response)

    except UnsupportedEndpointError as e:
        logger.error("Path and method error: %s", e)
        response = build_response(404, "Not Found")
    except InvalidRequestError as e:
        logger.error("Invalid request: %s", e)
        response = build_response(400, "Bad Request")
    except AuthError as e:
        logger.error("Authentication error: %s", e)
        response = build_response(401, "Unauthorized")
    except (ConfigurationError, InternalServerError) as e:
        logger.error("Internal Server Error: %s", e)
        response = build_response(500, "Internal Server Error")
    except Exception as e:
        logger.error("Error parsing request body: %s", e)
        response = build_response(400, "Bad Request")

    return response