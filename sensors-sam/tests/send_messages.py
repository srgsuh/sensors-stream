from datetime import datetime
import random
from typing import Optional
from requests import post, get, Response
import json
from helpers.logs import get_logger

API_BASE_URL = "sensors-alb-1575707353.il-central-1.elb.amazonaws.com"
API_PATH = "/api/v1/sensors"
API_URL = f"https://{API_BASE_URL}{API_PATH}"

response_statistics: dict[int, int] = {}

N_REQUESTS = 100

logger = get_logger("send_messages")

def login(username: str, password: str) -> Optional[str]:
    try:
        login_path = "https://0z9q7mhl2c.execute-api.il-central-1.amazonaws.com/auth/login"
        login_payload = {
            "username": username,
            "password": password,
            "new_password": ""
        }
        login_response: Response = post(login_path, json=login_payload)
        return login_response.json()["AccessToken"]
    except Exception as e:
        logger.error("Error logging in: %s", e)
        return None

def main() -> None:
    access_token = login("user", "12345@Com")
    logger.info("Access token: %s", access_token)
    if not access_token:
        logger.error("Failed to login")
        return
    
    health_response: Response = get(f"http://{API_BASE_URL}/health")
    if health_response.status_code != 200:
        logger.error("Failed to get health")
        return
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    for i in range(1, 1 + N_REQUESTS):
        request_body = {
            "request_id": i,
            "sensor_id": "1234567890",
            "value": 1 + random.randint(0, 100),
            "timestamp": datetime.now().isoformat(),
        }
        try:
            response: Response = post(API_URL, headers=headers, json=request_body)
            response_statistics[response.status_code] = response_statistics.get(response.status_code, 0) + 1
        except Exception as e:
            logger.error("Error sending request_id %d: %s", i, e)
            continue
    
    logger.info("Response statistics: %s", response_statistics)
    logger.info("Total requests: %d", N_REQUESTS)

if __name__ == "__main__":
    main()