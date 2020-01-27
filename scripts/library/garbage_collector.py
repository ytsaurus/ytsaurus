"""
    This module defines functions to create garbage-collected objects.
    If any of them leak, they will be removed by Odin garbage collector eventually.
"""

from contextlib import contextmanager
from six.moves import range


@contextmanager
def create_temp_object(client, logger, object_type, *args, **kwargs):
    object_id = client.create_object(object_type, *args, **kwargs)
    logger.info("Object of type '%s' with id '%s' created", object_type, object_id)
    try:
        yield object_id
    finally:
        try:
            client.remove_object(object_type, object_id)
            logger.info("Object of type '%s' with id '%s' removed", object_type, object_id)
        except Exception:
            logger.exception("Failed to remove temp object of type '%s' with id '%s'",
                             object_type, object_id)


@contextmanager
def create_temp_objects(client, logger, create_object_requests_original):
    create_object_requests = [list(request) for request in create_object_requests_original]
    object_ids = client.create_objects(create_object_requests)
    object_types = [create_object_request[0] for create_object_request in create_object_requests]
    logging_message = ", ".join("{} of type {}".format(object_id, object_type)
                                for object_id, object_type in zip(object_ids, object_types))
    logger.info("Objects created (%s)", logging_message)
    try:
        yield object_ids
    finally:
        removed_object_ids = []
        for object_type, object_id in zip(object_types, object_ids):
            try:
                client.remove_object(object_type, object_id)
                removed_object_ids.append(object_id)
            except Exception:
                logger.exception("Failed to remove temp object of type '%s' with id '%s'",
                                 object_type, object_id)
        logger.info("Objects removed (%s)", ", ".join(removed_object_ids))


class GarbageManager(object):
    _RANDOM_MD5 = "95c3e53cfc77acd268a7bd00942af84d"
    _COLLECTED_TYPES = ("pod", "pod_set", "endpoint_set")
    _GARBAGE_COLLECTOR_USER = "robot-yt-odin"


class GarbageMarker(GarbageManager):
    def __init__(self, owner_name):
        self._owner_name = owner_name

    def _get_garbage_labels(self):
        return {
            "garbage-owner": "{}-{}".format(self._owner_name, self._RANDOM_MD5),
            # TODO(se4min): remove this label when Odin garbage collector is updated.
            "odin-check-owner": "odin-check-{}-{}".format(self._owner_name, self._RANDOM_MD5),
        }

    def mark_attributes(self, object_type, object_attributes=None):
        assert object_type in self._COLLECTED_TYPES
        if object_attributes is None:
            object_attributes = {}
        else:
            object_attributes = object_attributes.copy()
        object_attributes["labels"] = self._inject_labels(object_attributes.get("labels", {}))
        return object_attributes

    def _inject_labels(self, labels):
        gc_labels = self._get_garbage_labels()
        for key in gc_labels:
            if key in labels:
                raise RuntimeError(
                    "Could not mark YP object for garbage collection "
                    "because of present /labels/{} field in the labels {}"
                    .format(key, labels)
                )
        labels.update(gc_labels)
        return labels

    def post_create_hook(self, yp_client, type_and_id_pairs):
        ace = dict(
            action="allow",
            subjects=[self._GARBAGE_COLLECTOR_USER],
            permissions=["read", "write"],
        )
        requests = [dict(object_type=object_type, object_id=object_id,
                         set_updates=[dict(path="/meta/acl/end", value=ace)])
                    for object_type, object_id in type_and_id_pairs]
        yp_client.update_objects(requests)


class YpGarbageCollectedClient(object):
    _FORWARDED_METHODS = (
        "generate_timestamp",
        "get_object",
        "get_objects",
        "get_user_access_allowed_to",
        "remove_object",
        "select_objects",
        "update_object",
        "update_objects",
    )

    def __init__(self, owner_name, yp_client):
        self._owner_name = owner_name
        self._yp_client = yp_client
        self._garbage_marker = GarbageMarker(owner_name)

    def __getattr__(self, name):
        if name in self._FORWARDED_METHODS:
            return getattr(self._yp_client, name)
        raise AttributeError(name)

    def create_object(self, object_type, attributes=None, **kwargs):
        attributes = self._garbage_marker.mark_attributes(object_type, attributes)
        object_id = self._yp_client.create_object(object_type, attributes=attributes, **kwargs)
        self._garbage_marker.post_create_hook(self._yp_client, [(object_type, object_id)])
        return object_id

    def create_objects(self, create_object_requests, **kwargs):
        for index in range(len(create_object_requests)):
            create_object_requests[index][1] = self._garbage_marker.mark_attributes(create_object_requests[index][0], create_object_requests[index][1])
        object_ids = self._yp_client.create_objects(create_object_requests, **kwargs)
        type_and_id_pairs = [(request[0], object_id)
                             for object_id, request in zip(object_ids, create_object_requests)]
        self._garbage_marker.post_create_hook(self._yp_client, type_and_id_pairs)
        return object_ids
