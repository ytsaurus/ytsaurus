from .logger import configure_logger

from yt.cron.library.helpers import create_yt_client
from yt.cron.library.solomon import push_cluster_data_to_solomon

from yt.orm.library.orchid_client import OrmOrchidClient

import argparse
import logging
import os


MONIUM_SERVICE_NAME = "object_counts"


def _get_object_count_sensors(orm_client, orchid_client, object_types):
    def count_objects(object_type):
        aggregate_result = orm_client.aggregate_objects(
            object_type,
            group_by=["1"],
            aggregators=["sum(1)"],
        )
        if not aggregate_result:
            return 0
        return aggregate_result[0][1]

    # NB! When new objects are added to ORM, this code and ORM master can disagree
    # about the supported object types. To be safe, we take only the types recognized
    # by both of them.
    object_types = set(object_types).intersection(orchid_client.get("/object_manager/types"))

    object_type_to_count = dict()
    for object_type in object_types:
        object_count = count_objects(object_type)
        logging.info(f"Got object count (object_type: {object_type}, count: {object_count})")
        object_type_to_count[object_type] = object_count

    return [
        dict(
            value=object_count,
            labels=dict(
                object_type=object_type,
                sensor="object_count",
            ),
        ) for object_type, object_count in object_type_to_count.items()
    ]


def _parse_arguments(human_readable_orm_name):
    parser = argparse.ArgumentParser(
        description=f"Monitor {human_readable_orm_name} object counts",
    )
    parser.add_argument("--cluster", help="YT cluster name", default=os.environ.get("YT_PROXY"))
    parser.add_argument(
        "--no-solomon-push",
        action="store_false",
        default=True,
        dest="solomon_push",
        help="Do not push data to Solomon",
    )
    return parser.parse_args()


def main(
    orm_client_cls,
    orm_logger,
    object_types,
    human_readable_orm_name,
    orm_path,
    monium_project,
    monium_cluster_mapper,
    orm_token,
):
    configure_logger(orm_logger)
    arguments = _parse_arguments(human_readable_orm_name)
    orm_cluster = arguments.cluster.replace("yp-", "")

    logging.info(f'Monitoring {human_readable_orm_name} object counts on cluster "{orm_cluster}"')

    orchid_client = OrmOrchidClient(
        yt_client=create_yt_client(arguments.cluster, retry_count=3),
        orm_path=orm_path,
        service_name="master",
        human_readable_service_name=f"{human_readable_orm_name} master",
    )
    with orm_client_cls(address=orm_cluster, config=dict(token=orm_token)) as orm_client:
        sensors = _get_object_count_sensors(orm_client, orchid_client, object_types)

    if arguments.solomon_push:
        logging.info("Sending data to Solomon")
        push_cluster_data_to_solomon(
            cluster=monium_cluster_mapper(orm_cluster),
            service=MONIUM_SERVICE_NAME,
            sensors=sensors,
            project=monium_project,
        )
