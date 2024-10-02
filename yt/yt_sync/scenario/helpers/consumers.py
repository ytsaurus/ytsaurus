import logging

from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer

LOG = logging.getLogger("yt_sync")


def ensure_consumer(
    desired_consumer: YtNativeConsumer,
    actual_consumer: YtNativeConsumer,
    yt_client_factory: YtClientFactory,
    clean: bool,
    log_only: bool,
):
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
                if not log_only:
                    yt_client_factory(queue_cluster).register_queue_consumer(
                        queue_path=queue_path,
                        consumer_path=consumer_path,
                        vital=desired_vital,
                        partitions=desired_partitions,
                    )
            else:
                LOG.info("No diff for registration (%s)", registration_description)


def ensure_cluster_consumers(
    desired: YtCluster, actual: YtCluster, yt_client_factory: YtClientFactory, clean: bool, log_only: bool
):
    for table_key, desired_consumer in desired.consumers.items():
        actual_consumer = actual.consumers[table_key]
        ensure_consumer(desired_consumer, actual_consumer, yt_client_factory, clean, log_only)


def ensure_db_consumers(
    desired: YtDatabase,
    actual: YtDatabase,
    yt_client_factory: YtClientFactory,
    clean: bool = False,
    log_only: bool = False,
):
    LOG.info("Ensure consumer registrations")
    for name, cluster in desired.clusters.items():
        ensure_cluster_consumers(cluster, actual.clusters[name], yt_client_factory, clean, log_only)


def unregister_queues(
    db: YtDatabase,
    yt_client_factory: YtClientFactory,
):
    for cluster in db.clusters.values():
        yt_client = yt_client_factory(cluster.name)
        for consumer in cluster.consumers.values():
            for registration in consumer.registrations.values():
                LOG.warning(
                    "Drop registration of consumer %s:%s to queue %s:%s",
                    consumer.table.cluster_name,
                    consumer.table.path,
                    registration.cluster_name,
                    registration.path,
                )
                yt_client.unregister_queue_consumer(queue_path=registration.path, consumer_path=consumer.full_path)
            consumer.registrations.clear()
