from datetime import datetime
import random
import time
from typing import Optional
from requests import post, get, Response, RequestException
from queue import Queue, Empty
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
DEBUG_LEVEL = logging.DEBUG

logging.basicConfig(
    format=LOG_FORMAT,
    level=logging.WARNING,
)
logger = logging.getLogger("send_messages")
logger.setLevel(DEBUG_LEVEL)

API_BASE_URL = "sensors-alb-1575707353.il-central-1.elb.amazonaws.com"
API_PATH = "/api/v1/sensors"
API_URL = f"http://{API_BASE_URL}{API_PATH}"

RESPONSE_STATISTICS: dict[int, int] = {}    # statistics of responses by status code

MAX_WORKERS = 8 # number of workers to use for sending requests
N_REQUESTS = 64 # number of requests to send for each sensor
DELAY_SECONDS = 2 # delay between requests in seconds

HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
}

def get_timestamp() -> str:
    return datetime.now().isoformat()

def get_delay_seconds(base_delay_seconds: float = DELAY_SECONDS, max_deviation_seconds: float = 0.3) -> float:
    return base_delay_seconds + (random.random() - 1) * max_deviation_seconds


BELOW_NORM_PROBABILITY = 0.05
ABOVE_NORM_PROBABILITY = 0.05
NORMAL_RANGE = 32

class Sensor:
    def __init__(
            self,
            sensor_id: str,
            min_normal_value: int,
            value_range: int = NORMAL_RANGE
        ):
        self.sensor_id = sensor_id
        self.min_normal_value = min_normal_value
        self.value_range = value_range
    
    def get_value(self, random_generator: random.Random) -> int:
        value = random_generator.randint(self.min_normal_value, self.min_normal_value + self.value_range - 1)
        deviation_dice_roll = random_generator.random()
        if deviation_dice_roll < BELOW_NORM_PROBABILITY:
            value -= self.value_range
        elif deviation_dice_roll > 1 - ABOVE_NORM_PROBABILITY:
            value += self.value_range
        return value

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
    except RequestException as e:
        logger.error("Error logging in: %s", e)
        return None

def send_sensor_data(sensor_data: dict, headers: dict) -> int:
    try:
        response: Response = post(API_URL, headers=headers, json=sensor_data)
        return response.status_code
    except RequestException as e:
        logger.error("Error sending sensor data: %s", e)
        return -1

def task_executor(task: dict, headers: dict, delay_seconds: float = DELAY_SECONDS) -> int | None:
    status_code = send_sensor_data(task, headers)
    logger.debug("Completed task for sensor id=%s, value: %d, status code: %d", task["sensor_id"], task["value"], status_code)
    time.sleep(delay_seconds)
    return status_code

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
    random_generator = random.Random(777)
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _ in range(N_REQUESTS):
            for sensor in SENSORS:
                futures.append(executor.submit(task_executor, sensor.get_sensor_data(random_generator), HEADERS, get_delay_seconds()))

    response_stat = {}    
    for future in as_completed(futures):
        status_code = future.result()
        response_stat[status_code] = response_stat.get(status_code, 0) + 1
    
    logger.info("Response statistics: %s", response_stat)
    logger.info("Total requests: %d", sum(response_stat.values()))

if __name__ == "__main__":
    main()