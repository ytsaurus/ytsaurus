import logging

from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model.types import Types

LOG = logging.getLogger("yt_sync")


def ensure_consumer(
    desired_consumer: YtNativeConsumer,
    actual_consumer: YtNativeConsumer,
    yt_client_factory: YtClientFactory,
    clean: bool,
    log_only: bool,
) -> bool:
    has_diff: bool = False
    consumer_path = desired_consumer.full_path
    for registration_key, actual_registration in actual_consumer.registrations.items():
        if registration_key not in desired_consumer.registrations or clean:
            queue_cluster = actual_registration.cluster_name
            queue_path = actual_registration.path
            LOG.warning(
                "Drop registration of consumer %s:%s to queue %s:%s",
                desired_consumer.table.cluster_name,
                desired_consumer.table.path,
                queue_cluster,
                queue_path,
            )
            has_diff = True
            if not log_only:
                yt_client_factory(queue_cluster).unregister_queue_consumer(
                    queue_path=queue_path, consumer_path=consumer_path
                )
    for registration_key, desired_registration in desired_consumer.registrations.items():
        LOG.debug("Check registration %s, %s", registration_key, desired_registration)
        desired_vital = desired_registration.vital
        desired_partitions = desired_registration.partitions
        queue_cluster = desired_registration.cluster_name
        queue_path = desired_registration.path
        registration_description = (
            f"consumer={desired_consumer.table.rich_path} queue={desired_registration.rich_path} vital={desired_vital}"
        )
        if desired_partitions is not None:
            registration_description += " partitions={desired_partitions}"

        if registration_key not in actual_consumer.registrations or clean:
            LOG.warning("Create new registration (%s)", registration_description)
            has_diff = True
            if not log_only:
                yt_client_factory(queue_cluster).register_queue_consumer(
                    queue_path=queue_path,
                    consumer_path=consumer_path,
                    vital=desired_vital,
                    partitions=desired_partitions,
                )
        else:
            current_registration = actual_consumer.registrations[registration_key]
            current_vital = current_registration.vital
            current_partitions = current_registration.partitions

            need_update = False
            update_info = []

            if desired_vital != current_vital:
                need_update = True
                update_info.append(f"change vital {current_vital} -> {desired_vital}")

            if desired_partitions != current_partitions:
                need_update = True
                update_info.append(f"change partitions {current_partitions} -> {desired_partitions}")

            if need_update:
                LOG.warning("Update registration (%s): %s", registration_description, ", ".join(update_info))
                has_diff = True
                if not log_only:
                    yt_client_factory(queue_cluster).register_queue_consumer(
                        queue_path=queue_path,
                        consumer_path=consumer_path,
                        vital=desired_vital,
                        partitions=desired_partitions,
                    )
            else:
                LOG.info("No diff for registration (%s)", registration_description)
    return has_diff


def ensure_cluster_consumers(
    desired: YtCluster, actual: YtCluster, yt_client_factory: YtClientFactory, clean: bool, log_only: bool
) -> bool:
    has_diff: bool = False
    for table_key, desired_consumer in desired.consumers.items():
        actual_consumer = actual.consumers[table_key]
        has_diff |= ensure_consumer(desired_consumer, actual_consumer, yt_client_factory, clean, log_only)
    return has_diff


def ensure_db_consumers(
    desired: YtDatabase,
    actual: YtDatabase,
    yt_client_factory: YtClientFactory,
    clean: bool = False,
    log_only: bool = False,
) -> bool:
    has_consumers: bool = any(cluster.consumers for cluster in desired.clusters.values())
    if not has_consumers:
        return False

    LOG.info("Ensure consumer registrations")
    has_diff: bool = False
    for name, cluster in desired.clusters.items():
        has_diff |= ensure_cluster_consumers(cluster, actual.clusters[name], yt_client_factory, clean, log_only)
    return has_diff


def unregister_queues(
    db: YtDatabase,
    yt_client_factory: YtClientFactory,
) -> bool:
    has_diff = False

    # cluster_name -> [(queue_path, consumer)]
    registrations: dict[str, set[tuple[str, Types.ReplicaKey]]] = dict()
    consumers: list[YtNativeConsumer] = list()
    for cluster in db.clusters.values():
        yt_client = yt_client_factory(cluster.name)
        for consumer in cluster.consumers.values():
            consumers.append(consumer)
            for registration in consumer.registrations.values():
                registrations.setdefault(registration.cluster_name, set()).add(
                    (registration.path, consumer.table.replica_key)
                )
        for table in cluster.tables.values():
            if not table.is_ordered:
                continue
            for registration in yt_client.list_queue_consumer_registrations(queue_path=table.path):
                registrations.setdefault(cluster.name, set()).add(
                    (
                        str(registration["queue_path"]),
                        (registration["consumer_path"].attributes["cluster"], str(registration["consumer_path"])),
                    )
                )

    for cluster_name, cluster_registrations in registrations.items():
        for (
            queue_path,
            consumer_replica_key,
        ) in cluster_registrations:
            LOG.warning(
                "Drop registration of consumer %s:%s to queue %s:%s",
                *consumer_replica_key,
                cluster_name,
                queue_path,
            )

    for cluster_name, cluster_registrations in registrations.items():
        yt_client = yt_client_factory(cluster_name)
        for (
            queue_path,
            consumer_replica_key,
        ) in cluster_registrations:
            yt_client.unregister_queue_consumer(
                queue_path=queue_path, consumer_path=YtNativeConsumer.format_consumer_path(*consumer_replica_key)
            )
    for consumer in consumers:
        consumer.registrations.clear()

    return has_diff
