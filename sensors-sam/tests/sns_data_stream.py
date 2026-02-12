import json
import random
from threading import Thread
import time
import uuid
import queue
from collections import Counter
import logging
from concurrent.futures import ThreadPoolExecutor
import boto3

DEFAULT_TOPIC_ARN = "arn:aws:sns:il-central-1:049345153436:sns-sensors-ingress"

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
DEBUG_LEVEL = logging.DEBUG

logging.basicConfig(
    format=LOG_FORMAT,
    level=logging.WARNING,
)
logger = logging.getLogger("send_messages")
logger.setLevel(DEBUG_LEVEL)


N_SENDERS = 4 # number of sender threads to use for sending sensor data
N_REQUESTS_PER_SENSOR = 256 # number of requests to send for each sensor
DELAY_SECONDS = 1.5 # delay between requests in seconds
DELAY_MAX_DEVIATION = 0.5 # maximum deviation from the delay in seconds

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
            "timestamp": time.time(),
        }

SENSORS: list[Sensor] = [
    Sensor(param["sensor_id"], param["min_value"], param["max_value"]) for param in default_params
]

SNS_CLIENT = boto3.client("sns", region_name="il-central-1")

def publish_data(sensor_data: dict) -> int:
    try:
        package_id = str(uuid.uuid4())
        package_data = {
            "package_id": package_id,
            **sensor_data,
        }
        SNS_CLIENT.publish(TopicArn=DEFAULT_TOPIC_ARN, Message=json.dumps(package_data))
        return 200
    except Exception as e:
        logger.error("Error sending sensor data: %s", e)
        return -1

def publisher(queue: queue.Queue, responses: Counter[int], worker_id: int) -> None:
    logger.info("==== Worker %d started ====", worker_id)
    while True:
        task = queue.get()
        if task is None:
            break
        status_code = publish_data(task)
        responses[status_code] += 1
        logger.debug("Worker %d completed task: sensor_id=%s, value=%d, status=%d", worker_id, task["sensor_id"], task["value"], status_code)
        queue.task_done()
    logger.info("==== Worker %d completed all tasks ====", worker_id)

def produce_sensor_data(queue: queue.Queue, sensor: Sensor) -> None:
    random_generator = random.Random()
    for _ in range(N_REQUESTS_PER_SENSOR):
        queue.put(sensor.get_sensor_data(random_generator))
        time.sleep(DELAY_SECONDS + 2 * (random_generator.random() - 0.5) * DELAY_MAX_DEVIATION)

def generate_stream() -> Counter[int] | None:
    logger.info("Starting test")
    logger.info("Publishing sensor data to SNS...")
    task_queue: queue.Queue[dict | None] = queue.Queue()
    
    counters = [Counter[int]() for _ in range(N_SENDERS)]
    workers = [Thread(target=publisher, args=(task_queue, counters[w_id], w_id)) for w_id in range(N_SENDERS)]
    
    for w in workers:
        w.start()
    
    with ThreadPoolExecutor(max_workers=len(SENSORS)) as exe:
        producers = [exe.submit(produce_sensor_data, task_queue, sensor) for sensor in SENSORS]
    
    task_queue.join()   # wait for all tasks to be completed
    # signal workers to stop
    for _ in producers:
        task_queue.put(None)
    # wait for workers to complete
    for worker in workers:
        worker.join()
    # aggregate response statistics
    response_statistics: Counter[int] = Counter[int]()
    for counter in counters:
        response_statistics.update(counter)

    return response_statistics


if __name__ == "__main__":
    response_statistics = generate_stream()
    if response_statistics is not None:
        logger.info("Response statistics: %s", response_statistics)
        logger.info("Total requests: %d", sum(response_statistics.values()))
        logger.info("Success rate: %f%%", response_statistics.get(200, 0) / sum(response_statistics.values()) * 100)