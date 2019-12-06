# -*- coding: utf-8 -*-

from yp.common import YtResponseError

from yt.packages.six.moves import xrange

import contextlib
import logging


class BatchYpClient(object):
    def __init__(self, yp_client, batch_size, retries_count=1):
        self._yp_client = yp_client
        self._batch_size = batch_size
        self._retries_count = retries_count
        assert self._batch_size > 0
        self._batch_create = []
        self._batch_remove = []
        self._batch_update = []

    def _commit_create_requests(self):
        if self._batch_create:
            logging.debug("Commiting YP client create request batch")
            self._yp_client.create_objects(self._batch_create)
            self._batch_create = []

    def _commit_remove_requests(self):
        if self._batch_remove:
            logging.debug("Commiting YP client remove request batch")
            self._yp_client.remove_objects(self._batch_remove)
            self._batch_remove = []

    def _commit_update_requests(self):
        if self._batch_update:
            logging.debug("Commiting YP client update request batch")
            self._yp_client.update_objects(self._batch_update)
            self._batch_update = []

    def create_object(self, object_type, attributes):
        self._try_flush(self._commit_remove_requests)
        self._try_flush(self._commit_update_requests)
        self._batch_create.append((object_type, attributes))
        if len(self._batch_create) >= self._batch_size:
            self._try_flush(self._commit_create_requests)

    def remove_object(self, object_type, object_id):
        self._try_flush(self._commit_create_requests)
        self._try_flush(self._commit_update_requests)
        self._batch_remove.append((object_type, object_id))
        if len(self._batch_remove) >= self._batch_size:
            self._try_flush(self._commit_remove_requests)

    def update_object(self, object_type, object_id, set_updates=None, remove_updates=None, attribute_timestamp_prerequisites=None):
        self._try_flush(self._commit_create_requests)
        self._try_flush(self._commit_remove_requests)
        self._batch_update.append(dict(
            object_type=object_type,
            object_id=object_id,
            set_updates=set_updates,
            remove_updates=remove_updates,
            attribute_timestamp_prerequisites=attribute_timestamp_prerequisites
        ))
        if len(self._batch_update) >= self._batch_size:
            self._try_flush(self._commit_update_requests)

    def _try_flush(self, commit_function):
        for retry_number in xrange(self._retries_count):
            if retry_number > 0:
                logging.warning("Retrying flush (retry number %d)", retry_number)
            try:
                commit_function()
            except YtResponseError:
                if retry_number < self._retries_count - 1:
                    logging.exception("Flush failed")
                    retry_number += 1
                else:
                    raise
            else:
                break

    def flush(self):
        self._try_flush(self._commit_create_requests)
        self._try_flush(self._commit_remove_requests)
        self._try_flush(self._commit_update_requests)


@contextlib.contextmanager
def create_batch_yp_client(*args, **kwargs):
    result = None
    try:
        result = BatchYpClient(*args, **kwargs)
        yield result
    finally:
        if result is not None:
            try:
                result.flush()
            except:
                logging.exception("Exception occurred while commiting batch YP client")
