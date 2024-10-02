from yt.yt.orm.example.python.client.client import ExampleClient

import yt.logger as logger

from library.python.testing.recipe import declare_recipe

from concurrent import futures
from datetime import datetime
import os
import random
import uuid


START_YEAR = 0
OBJECTS_COUNT = 50_000
BATCH_SIZE = 100
THREADS_COUNT = 10
REPEAT_COUNT = 10

CONFIG = {"enable_ssl": False}

OBJECT_TYPE = "book"
UNIQUE_EDITOR_ID = "editor_id_unique"
FREQUENT_EDITOR_ID = "editor_id_frequent"


def get_address():
    return os.environ["EXAMPLE_MASTER_GRPC_INSECURE_ADDR"]


def make_random_string(length=10):
    result = ""
    while len(result) < length:
        result += uuid.uuid4().hex
    return result[:length]


def make_book(idx):
    for basket in (1000, 100, 10):
        if 0 == idx % basket:
            idx = basket
            break
    genres = [make_random_string() for _ in range(random.randrange(3))] + [
        "{}x_era".format(idx // 10)
    ]
    keywords = [make_random_string() for _ in range(random.randrange(7))] + [
        "created_in_{}".format(idx)
    ]
    result = {
        "spec": {
            "name": make_random_string(length=30),
            "year": idx,
            "genres": genres,
            "keywords": keywords,
            "design": {
                "cover": {
                    "hardcover": True,
                    "image": make_random_string(),
                },
                "color": make_random_string(),
                "illustrations": [make_random_string() for _ in range(random.randrange(5))],
            },
            "digital_data": {
                "store_rating": idx / 1000.0,
                "available_formats": [
                    {
                        "format": "epub",
                        "size": 11778382,
                    },
                    {
                        "format": "pdf",
                        "size": 12778383,
                    },
                ],
            },
            "page_count": idx % 500,
        },
        "meta": {"isbn": make_random_string(), "publisher_id": 42},
    }
    if 2013 == idx:
        result["spec"]["editor_id"] = UNIQUE_EDITOR_ID
    elif 10 == idx:
        result["spec"]["editor_id"] = FREQUENT_EDITOR_ID
    return result


def create_objects(start_idx, finish_idx, thread_idx):
    with ExampleClient(address=get_address(), config=CONFIG) as client:
        for i in range(start_idx, finish_idx, BATCH_SIZE):
            for repeat in range(REPEAT_COUNT):
                try:
                    start = datetime.utcnow()
                    client.create_objects(
                        [(OBJECT_TYPE, make_book(idx=i + j)) for j in range(BATCH_SIZE)],
                        request_meta_response=True,
                    )
                    finish = datetime.utcnow()
                    if repeat:
                        logger.info(
                            "Successful object creation. Step #{}, retry #{}, thread #{}, duration is {}".format(
                                i, repeat, thread_idx, (finish - start)
                            )
                        )
                    break
                except Exception as e:
                    logger.info(
                        "Fail on create_objects. Step #{}, retry #{}, thread #{}. {}".format(
                            i, repeat, thread_idx, e
                        )
                    )
                    if (REPEAT_COUNT - 1) == repeat:
                        raise


def start(argv):
    logger.info("Start creating objects for benchmark")
    with ExampleClient(address=get_address(), config=CONFIG) as client:
        client.update_object(
            "user", "root", set_updates=[dict(path="/spec/request_weight_rate_limit", value=10000)]
        )
        client.create_object(
            "publisher",
            attributes={"meta": {"id": 42}, "spec": {"name": make_random_string()}},
            request_meta_response=True,
        )
        for editor_id in (FREQUENT_EDITOR_ID, UNIQUE_EDITOR_ID):
            client.create_object(
                "editor",
                attributes={"meta": {"id": editor_id}, "spec": {"name": make_random_string()}},
                request_meta_response=True,
            )
    with futures.ThreadPoolExecutor(max_workers=THREADS_COUNT) as executor:
        for thread_idx in range(THREADS_COUNT):
            executor.submit(
                create_objects,
                START_YEAR + thread_idx * OBJECTS_COUNT // THREADS_COUNT,
                START_YEAR + (thread_idx + 1) * OBJECTS_COUNT // THREADS_COUNT,
                thread_idx,
            )
    logger.info("Finish creating objects for benchmark")


def stop(argv):
    pass


if __name__ == "__main__":
    declare_recipe(start, stop)
