import random
import threading
import time
import argparse
from typing import Optional
from requests import post, get, Response, RequestException
import queue
from collections import Counter
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

MAX_SENDER_WORKERS = 4 # number of workers to use for sending sensor data
N_REQUESTS_PER_SENSOR = 256 # number of requests to send for each sensor
DELAY_SECONDS = 2 # delay between requests in seconds
DELAY_MAX_DEVIATION = 0.3 # maximum deviation from the delay in seconds

HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
}

def get_timestamp() -> float:
    return time.time()

BELOW_NORM_PROBABILITY = 0.05
ABOVE_NORM_PROBABILITY = 0.05
default_params = [
    {"sensor_id": "101", "min_value": 43, "max_value": 73},
    {"sensor_id": "102", "min_value": 52, "max_value": 82},
    {"sensor_id": "103", "min_value": 38, "max_value": 68},
    {"sensor_id": "104", "min_value": 65, "max_value": 95},
    {"sensor_id": "105", "min_value": 25, "max_value": 35},
    {"sensor_id": "106", "min_value": 32, "max_value": 52},
    {"sensor_id": "107", "min_value": 17, "max_value": 47},
    {"sensor_id": "108", "min_value": 11, "max_value": 31},
]

class Sensor:
    def __init__(
            self,
            sensor_id: str,
            min_normal_value: int,
            max_normal_value: int,
        ):
        self.sensor_id = sensor_id
        self.min_normal_value = min_normal_value
        self.max_normal_value = max_normal_value
        self.value_range = max_normal_value - min_normal_value + 1
    
    def get_value(self, random_generator: random.Random) -> int:
        value = random_generator.randint(self.min_normal_value, self.max_normal_value)
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
        }

SENSORS: list[Sensor] = [
    Sensor(param["sensor_id"], param["min_value"], param["max_value"]) for param in default_params
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

def task_worker(queue: queue.Queue, headers: dict, responses: Counter[int], worker_id: int) -> None:
    logger.info("==== Worker %d started ====", worker_id)
    while True:
        task = queue.get()
        if task is None:
            break
        status_code = send_sensor_data(task, headers)
        responses[status_code] += 1
        logger.debug("Worker %d completed task: sensor_id=%s, value=%d, status=%d", worker_id, task["sensor_id"], task["value"], status_code)
        queue.task_done()
    logger.info("==== Worker %d completed all tasks ====", worker_id)

def produce_sensor_data(queue: queue.Queue, sensor: Sensor) -> None:
    random_generator = random.Random()
    for _ in range(N_REQUESTS_PER_SENSOR):
        queue.put(sensor.get_sensor_data(random_generator))
        time.sleep(DELAY_SECONDS + 2 * (random_generator.random() - 0.5) * DELAY_MAX_DEVIATION)

def generate_stream(username: str, password: str) -> Counter[int] | None:
    logger.info("Starting test")
    logger.info("Logging in...")
    access_token = login(username, password)
    
    if not access_token:
        logger.error("Failed to login")
        return None
    logger.info("Login successful")

    HEADERS = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    
    logger.info("Checking health...")
    health_response: Response = get(f"http://{API_BASE_URL}/health")
    if health_response.status_code != 200:
        logger.error("Failed to get health")
        return None
    logger.info("Health check successful")

    logger.info("Sending requests...")
    task_queue: queue.Queue[dict | None] = queue.Queue()
    
    workers: list[threading.Thread] = []
    counters: list[Counter[int]] = []
    for worker_id in range(MAX_SENDER_WORKERS):
        counters.append(Counter[int]())
        worker = threading.Thread(target=task_worker, args=(task_queue, HEADERS.copy(), counters[worker_id], worker_id))
        workers.append(worker)
        worker.start()
    
    with ThreadPoolExecutor(max_workers=len(SENSORS)) as executor:
        for sensor in SENSORS:
            executor.submit(produce_sensor_data, task_queue, sensor)
    
    task_queue.join()
    
    for _ in range(MAX_SENDER_WORKERS):
        task_queue.put(None)
    # wait for workers to complete
    for worker in workers:
        worker.join()
    # aggregate response statistics
    response_statistics: Counter[int] = Counter()
    for counter in counters:
        response_statistics.update(counter)

    return response_statistics

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate test sensor data stream")
    parser.add_argument("username", help="Username used for login")
    parser.add_argument("password", help="Password used for login")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    response_statistics = generate_stream(args.username, args.password)
    if response_statistics is not None:
        logger.info("Response statistics: %s", response_statistics)
        logger.info("Total requests: %d", sum(response_statistics.values()))
        logger.info("Success rate: %f%%", response_statistics.get(200, 0) / sum(response_statistics.values()) * 100)