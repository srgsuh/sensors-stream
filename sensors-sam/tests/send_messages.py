from datetime import datetime
import random
import time
from typing import Optional
from requests import post, get, Response, RequestException
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_LEVEL = logging.DEBUG
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
logger = logging.getLogger("send_messages")

API_BASE_URL = "sensors-alb-1575707353.il-central-1.elb.amazonaws.com"
API_PATH = "/api/v1/sensors"
API_URL = f"http://{API_BASE_URL}{API_PATH}"

RESPONSE_STATISTICS: dict[int, int] = {}    # statistics of responses by status code

MAX_WORKERS = 8 # number of workers to use for sending requests
N_REQUESTS = 256 # number of requests to send for each sensor
DELAY_SECONDS = 2 # delay between requests in seconds

NORMAL_PROBABILITY = 0.9 # probability of getting a normal value
NORMAL_RANGE = 35 # range of normal values
ABNORMAL_RANGE = 10 # range of abnormal values
ABNORMAL_LOW_END_PROBABILITY = 0.5 # probability of getting abnormal value below normal range

HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
}

def get_timestamp() -> str:
    return datetime.now().isoformat()

class Sensor:
    def __init__(
            self,
            sensor_id: str,
            min_normal_value: int,
            normal_range: int = NORMAL_RANGE,
            abnormal_range: int = ABNORMAL_RANGE
        ):
        self.sensor_id = sensor_id
        self.min_normal_value = min_normal_value
        self.max_normal_value = min_normal_value + normal_range
        self.min_lower_value = self.min_normal_value - 1 - abnormal_range
        self.max_lower_value = self.min_normal_value - 1
        self.min_upper_value = self.max_normal_value + 1 + abnormal_range
        self.max_upper_value = self.max_normal_value + 1 + abnormal_range
    
    def get_normal_value(self, random_generator: random.Random) -> int:
        return random_generator.randint(self.min_normal_value, self.max_normal_value)
    
    def get_abnormal_value(self, random_generator: random.Random, abnormal_low_end_probability: float) -> int:
        if random_generator.random() < abnormal_low_end_probability:
            return random_generator.randint(self.min_lower_value, self.max_lower_value)
        else:
            return random_generator.randint(self.min_upper_value, self.max_upper_value)

    def get_value(
            self,
            random_generator: random.Random,
            normal_probability: float = NORMAL_PROBABILITY,
            abnormal_low_end_probability: float = ABNORMAL_LOW_END_PROBABILITY
        ) -> int:
        if random_generator.random() < normal_probability:
            return self.get_normal_value(random_generator)
        else:
            return self.get_abnormal_value(random_generator, abnormal_low_end_probability)

    def get_sensor_data(self, random_generator: random.Random) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "value": self.get_value(random_generator),
            "timestamp": get_timestamp(),
        }

SENSORS: list[Sensor] = [
    Sensor("001", 23),
    Sensor("002", 47),
    Sensor("003", 55),
    Sensor("004", 70),
    Sensor("005", 63),
    Sensor("006", 54),
    Sensor("007", 72),
    Sensor("008", 82),
]

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
def send_sensor_data(sensor_data: dict, headers: dict = HEADERS) -> int:
    try:
        response: Response = post(API_URL, headers=headers, json=sensor_data)
        return response.status_code
    except RequestException as e:
        logger.error("Error sending sensor data: %s", e)
        return -1


def generate_sensor_flow(sensor: Sensor, n_requests: int = N_REQUESTS) -> None:
    random_generator = random.Random()
    for _ in range(n_requests):
        data = sensor.get_sensor_data(random_generator)
        status_code = send_sensor_data(data, HEADERS)
        RESPONSE_STATISTICS[status_code] = RESPONSE_STATISTICS.get(status_code, 0) + 1
        logger.debug("Sent sensor %s data: %s, status code: %d", sensor.sensor_id, data, status_code)
        time.sleep(DELAY_SECONDS + random_generator.randint(-4, 4)/10)

def main() -> None:
    logger.info("Starting test")
    logger.info("Logging in...")
    access_token = login("user", "12345@Com")
    
    if not access_token:
        logger.error("Failed to login")
        return
    logger.info("Login successful")

    HEADERS = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    
    logger.info("Checking health...")
    health_response: Response = get(f"http://{API_BASE_URL}/health")
    if health_response.status_code != 200:
        logger.error("Failed to get health")
        return
    logger.info("Health check successful")

    logger.info("Sending requests...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for sensor in SENSORS:
            executor.submit(generate_sensor_flow, sensor)
    
    logger.info("Response statistics: %s", RESPONSE_STATISTICS)
    logger.info("Total requests: %d", sum(RESPONSE_STATISTICS.values()))
    

if __name__ == "__main__":
    main()