from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.wrapper.retries import run_with_retries

from contextlib import contextmanager

GARBAGE_COLLECTOR_ROBOTS = ["robot-yt-cron", "robot-yt-odin"]
GARBAGE_SIGNATURE = "95c3e53cfc77acd268a7bd00942af84d"


# TODO(s-berdnikov): Use dataclass once fully migrated to python 3.
class GarbageTraits(object):
    def __init__(self, collected_types, remove_hook=None):
        self.collected_types = collected_types
        self.remove_hook = remove_hook or GarbageTraits.default_remove_hook

    @staticmethod
    def default_remove_hook(orm_client, *args, **kwargs):
        return orm_client.remove_object(*args, **kwargs)


def _get_remove_hook(garbage_traits):
    if garbage_traits is not None:
        return garbage_traits.remove_hook

    return GarbageTraits.default_remove_hook


@contextmanager
def create_temp_object(orm_client, logger, object_type, garbage_traits=None, *args, **kwargs):
    remove_hook = _get_remove_hook(garbage_traits)

    object_id = orm_client.create_object(object_type, *args, **kwargs)
    logger.info("Object of type '%s' with id '%s' created", object_type, object_id)
    try:
        yield object_id
    finally:
        try:
            run_with_retries(
                lambda: remove_hook(orm_client, object_type, object_id),
                retry_count=10,
                backoff=2.0,
                exceptions=(YtTabletTransactionLockConflict,),
            )

            logger.info("Object of type '%s' with id '%s' removed", object_type, object_id)
        except Exception:
            logger.exception(
                "Failed to remove temp object of type '%s' with id '%s'",
                object_type,
                object_id,
            )


@contextmanager
def create_temp_objects(orm_client, logger, create_object_requests_original, garbage_traits=None):
    remove_hook = _get_remove_hook(garbage_traits)

    create_object_requests = [list(request) for request in create_object_requests_original]
    object_ids = orm_client.create_objects(create_object_requests)
    object_types = [create_object_request[0] for create_object_request in create_object_requests]
    logging_message = ", ".join(
        "{} of type {}".format(object_id, object_type) for object_id, object_type in zip(object_ids, object_types)
    )
    logger.info("Objects created (%s)", logging_message)
    try:
        yield object_ids
    finally:
        removed_object_ids = []
        for object_type, object_id in zip(object_types, object_ids):
            try:
                remove_hook(orm_client, object_type, object_id)
                removed_object_ids.append(object_id)
            except Exception:
                logger.exception(
                    "Failed to remove temp object of type '%s' with id '%s'",
                    object_type,
                    object_id,
                )
        logger.info("Objects removed (%s)", ", ".join(removed_object_ids))


class GarbageMarker(object):
    def __init__(self, garbage_traits, garbage_owner):
        self._garbage_traits = garbage_traits
        self._garbage_owner = garbage_owner

    def _get_garbage_labels(self, ttl):
        garbage_collection = dict(
            owner="{}-{}".format(self._garbage_owner, GARBAGE_SIGNATURE),
        )
        if ttl is not None:
            garbage_collection["ttl"] = ttl

        return dict(garbage_collection=garbage_collection)

    def mark_attributes(self, object_type, ttl, object_attributes=None):
        assert object_type in self._garbage_traits.collected_types
        if object_attributes is None:
            object_attributes = {}
        else:
            object_attributes = object_attributes.copy()
        object_attributes["labels"] = self._inject_labels(object_attributes.get("labels", {}), ttl)
        return object_attributes

    def _inject_labels(self, labels, ttl):
        gc_labels = self._get_garbage_labels(ttl)
        for key in gc_labels:
            if key in labels:
                raise RuntimeError(
                    "Could not mark object for garbage collection "
                    "because of present /labels/{} field in the labels {}".format(key, labels)
                )
        labels.update(gc_labels)
        return labels

    def post_create_hook(self, orm_client, transaction_id, type_and_id_pairs):
        ace = dict(
            action="allow",
            subjects=GARBAGE_COLLECTOR_ROBOTS,
            permissions=["read", "write"],
        )
        requests = [
            dict(
                object_type=object_type,
                object_id=object_id,
                set_updates=[dict(path="/meta/acl/end", value=ace)],
            )
            for object_type, object_id in type_and_id_pairs
        ]
        orm_client.update_objects(requests, transaction_id=transaction_id)


class OrmGarbageCollectedClient(object):
    def __init__(self, orm_client, garbage_traits, garbage_owner):
        self._orm_client = orm_client
        self._garbage_marker = GarbageMarker(garbage_traits, garbage_owner)

    def __getattr__(self, name):
        return getattr(self._orm_client, name)

    def create_object(self, object_type, attributes=None, transaction_id=None, ttl=None, **kwargs):
        assert transaction_id is None
        transaction_id = self._orm_client.start_transaction()

        attributes = self._garbage_marker.mark_attributes(object_type, ttl, attributes)
        object_id = self._orm_client.create_object(
            object_type, attributes=attributes, transaction_id=transaction_id, **kwargs
        )
        self._garbage_marker.post_create_hook(self._orm_client, transaction_id, [(object_type, object_id)])

        self._orm_client.commit_transaction(transaction_id)

        return object_id

    def create_objects(self, create_object_requests, transaction_id=None, ttl=None, **kwargs):
        assert transaction_id is None
        transaction_id = self._orm_client.start_transaction()

        for index in range(0, len(create_object_requests)):
            create_object_requests[index][1] = self._garbage_marker.mark_attributes(
                object_type=create_object_requests[index][0],
                ttl=ttl,
                object_attributes=create_object_requests[index][1],
            )

        object_ids = self._orm_client.create_objects(create_object_requests, transaction_id=transaction_id, **kwargs)
        type_and_id_pairs = [(request[0], object_id) for object_id, request in zip(object_ids, create_object_requests)]
        self._garbage_marker.post_create_hook(self._orm_client, transaction_id, type_and_id_pairs)

        self._orm_client.commit_transaction(transaction_id)

        return object_ids
