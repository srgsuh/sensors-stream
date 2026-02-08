from typing import Optional
from jose import ExpiredSignatureError, JWTError, jwt
import requests
from helpers.config import get_env_var, get_region, InternalServerError, ConfigurationError
from helpers.logs import get_logger

logger = get_logger("auth")

_auth_config: dict = {}

def _init_auth_config() -> dict:
    _auth_config["region_id"] = get_region()
    _auth_config["user_pool_id"] = get_env_var("COGNITO_USER_POOL_ID")
    _auth_config["app_id"] = get_env_var("COGNITO_USER_POOL_CLIENT_ID")
    _auth_config["cognito_issuer"] = f"https://cognito-idp.{_auth_config['region_id']}.amazonaws.com/{_auth_config['user_pool_id']}"
    _auth_config["cognito_jwks_url"] = f"{_auth_config['cognito_issuer']}/.well-known/jwks.json"
    return _auth_config

def get_auth_config(key: str) -> str:
    if not _auth_config:
        _init_auth_config()
    if key not in _auth_config:
        raise ConfigurationError(f"Key {key} not found in auth config")
    return _auth_config[key]

class AuthError(Exception):
    pass

AUTH_TOKEN_PREFIX = "Bearer "

def get_auth_token(event: dict) -> str:
    headers: dict = event.get("headers", {})
    auth_header = headers.get("authorization", "")
    if not auth_header:
        raise AuthError("Authorization header is required")
    if not auth_header.startswith("Bearer "):
        raise AuthError("Authorization header must start with Bearer")
    
    return auth_header[len(AUTH_TOKEN_PREFIX):]

def _fetch_cognito_keys() -> list[dict]:
    try:
        response: requests.Response = requests.get(get_auth_config("cognito_jwks_url"))
        response.raise_for_status()
        jwks: dict = response.json()

        return jwks.get("keys", [])
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching Cognito keys: %s", e)
        raise InternalServerError(f"Error fetching Cognito keys: {e}")
    except Exception as e:
        logger.error("Unexpected error fetching Cognito keys: %s", e)
        raise InternalServerError(f"Error fetching Cognito keys: {e}")

def _extract_kid(token: str) -> str:
    header: dict = jwt.get_unverified_header(token)
    key_id = header.get("kid")
    if not key_id:
        logger.error("No key ID found")
        raise AuthError("No key ID found")
    return key_id

def get_user_token(event: dict) -> dict:
    token = get_auth_token(event)
    logger.debug(".get_current_token token=%s...%s", token[:5], token[-5:])
    kid = _extract_kid(token)
    keys = _fetch_cognito_keys()

    public_key: Optional[dict] = next((k for k in keys if k["kid"] == kid), None)

    if not public_key:
        logger.error("Public key is not found")
        raise AuthError("Public key is not found")
    
    payload: dict
    try:
        payload = jwt.decode(
            token,
            public_key,
            algorithms=public_key.get("alg", "RS256"),
            issuer=get_auth_config("cognito_issuer")
        )
    except ExpiredSignatureError:
        logger.error("Expired token")
        raise AuthError("Expired token")
    except JWTError as je:
        logger.error(f"JWTError occurred: \"%s\"", str(je))
        raise AuthError("JWTError occurred")
    
    token_type: Optional[str] = payload.get("token_use")
    if token_type != "access":
        logger.error(f"Wrong type of token: {token_type}")
        raise AuthError("Wrong type of token")
    
    client_id: Optional[str] = payload.get("client_id")
    if client_id and client_id != get_auth_config("app_id"):
        logger.error("Wrong app_id")
        raise AuthError("Wrong app_id")
    
    return payload

def authenticate_user(event: dict) -> None:
    get_user_token(event)
    logger.debug("User authenticated successfully")