# -*- coding: utf-8 -*-

import contextlib
import logging


class BatchYpCreator(object):
    def __init__(self, yp_client, batch_size):
        self._yp_client = yp_client
        self._batch_size = batch_size
        assert self._batch_size > 0
        self._batch = []

    def create(self, object_type, attributes):
        if len(self._batch) >= self._batch_size:
            self.commit()
        self._batch.append((object_type, attributes))

    def commit(self):
        if len(self._batch) > 0:
            logging.info("Commiting YP creator batch")
            self._yp_client.create_objects(self._batch)
            self._batch = []


@contextlib.contextmanager
def create_batch_yp_creator(*args, **kwargs):
    result = None
    try:
        result = BatchYpCreator(*args, **kwargs)
        yield result
    finally:
        if result is not None:
            try:
                result.commit()
            except:
                logging.exception("Exception occurred while commiting batch YP creator")
