import logging
import threading
import functools
import cachetools
import collections
import concurrent.futures as futures
from os import abort

global_logger = logging.getLogger("access-log-processor")

BATCH_SIZE = 1000
MAX_CONCURRENT_BATCHES = 4
QUEUE_LIMIT_MULTIPLICATOR = 25
LOOKUP_RETRY_COUNT = 10


def get_node_id(path: str) -> str | None:
    """
    Checks if given ypath is a guid-based (e.g. #1-2-3-4/@some/attr)

    Returns guid if it is the case or None otherwise.
    """
    if path is None or len(path) == 0 or path[0] == "/":
        return None
    node_id_part = path.split("/", 1)[0][1:]
    if node_id_part.endswith("&"):
        return node_id_part[:-1]
    return node_id_part


def get_attr_or_item(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key)


def set_attr_or_item(obj, key, value):
    if isinstance(obj, dict):
        return obj.__setitem__(key, value)
    return setattr(obj, key, value)


class NodeIdResolver:
    def __init__(self, client, id_path_resolve_table, max_concurrent_batches=MAX_CONCURRENT_BATCHES, batch_size=BATCH_SIZE, cache_size=10_0000, logger=global_logger):
        self._client = client
        self._id_path_resolve_table = id_path_resolve_table
        self._max_concurrent_batches = max_concurrent_batches
        self._batch_size = batch_size
        self._logger = logger.getChild("NodeIdResolver")

        self._cache = cachetools.LRUCache(maxsize=cache_size)
        self._pool = futures.ThreadPoolExecutor(self._max_concurrent_batches)
        self._lock = threading.RLock()
        self._pending_requests = dict()
        self._request = list()

    def get_batch_size(self):
        return self._batch_size

    def get_max_concurrent_batches(self):
        return self._max_concurrent_batches

    def get_pending_requests_count(self):
        with self._lock:
            return len(self._pending_requests)

    def current_batch_size(self):
        with self._lock:
            return len(self._request)

    def __call__(self, *args, **kwargs):
        return self.resolve(*args, **kwargs)

    def _retried_lookup(self, request, retries):
        for i in range(retries):
            try:
                return self._client.lookup_rows(self._id_path_resolve_table, request, keep_missing_rows=True)
            except Exception:
                if i == (retries-1):
                    raise

    def _async_resolve(self, request):
        with self._lock:
            try:
                self._logger.debug(f"Make lookup for {request}.")
                lookup_result = self._retried_lookup(request, LOOKUP_RETRY_COUNT)
                for r, result in zip(request, lookup_result):
                    self._logger.debug(f"Lookup result: {r}->{result}")
                    key = (r["cluster"], r["node_id"])
                    future = self._pending_requests[key]
                    self._cache[key] = future
                    future.set_result(result["path"] if result else None)
                    del self._pending_requests[key]
            except Exception as e:
                for row in request:
                    key = (row["cluster"], row["node_id"])
                    self._pending_requests[key].set_exception(e)
                    del self._pending_requests[key]

    def flush(self):
        with self._lock:
            if len(self._request):
                self._logger.debug(f"Flush {len(self._request)} messages.")
                self._pool.submit(self._async_resolve, self._request)
                self._request = list()
            else:
                self._logger.debug("Queue is empty. Skip.")

    def resolve(self, cluster, node_id):
        key = (cluster, node_id)
        with self._lock:
            if key in self._cache:
                self._logger.debug(f"Resolve({key}): return from cache.")
                return self._cache[key]
            elif key in self._pending_requests:
                self._logger.debug(f"Resolve({key}): already in queue.")
                return self._pending_requests[key]
            else:
                self._logger.debug(f"Resolve({key}): add to queue.")
                future = futures.Future()
                self._pending_requests[key] = future
                self._request.append({"cluster": cluster, "node_id": node_id})
                if self.current_batch_size() >= self.get_batch_size():
                    self.flush()
                return future


class NodeIdPatcher:
    """
    Class allows to replace guid-based paths with root-based paths.

    e.g.
    {
        "path": "#1-2-3-4",
        "some-other-attr": 100500,
    }

    can become
    {
        "path": "//path/to/1-2-3-4/node",
        "some-other-attr": 100500,
    }
    """
    class PatchedRowFuture:
        def __init__(self, row, substitutions, logger):
            self._logger = logger
            self._future = futures.Future()
            if substitutions:
                self._row = row
                self._fields = set(field for field, path_tail, path_future in substitutions)
                self._logger.debug(f"Schedule substitution for {hex(id(self))}->{self._fields}.")
                for field, path_tail, path_future in substitutions:
                    path_future.add_done_callback(functools.partial(self._patch, field, path_tail))
            else:
                self._future.set_result(row)

        def _patch(self, field, path_tail, path_future):
            self._logger.debug(f"Do substitution for {hex(id(self))}->{field}.")
            if self._future.done():
                if not self._future.exception():
                    abort()
                return
            try:
                resolved_path = path_future.result()
                if resolved_path:
                    set_attr_or_item(self._row, field, resolved_path + path_tail)
                self._fields.remove(field)
                if len(self._fields) == 0:
                    self._future.set_result(self._row)
                    self._row = None
            except Exception as e:
                self._future.set_exception(e)
                self._row = None
                self._fields = set()

        def result(self):
            return self._future.result()

        def done(self):
            return self._future.done()

    def __init__(self, resolver, fields, queue_limit=0, preserve_order=True, logger=global_logger):
        self._resolver = resolver
        self._fields = fields
        self._preserve_order = preserve_order
        self._queue_limit = queue_limit if queue_limit else (self.get_batch_size() * self.get_max_concurrent_batches() * QUEUE_LIMIT_MULTIPLICATOR)
        self._logger = logger.getChild("NodeIdPatcher")
        self._patched_row_future_logger = self._logger.getChild("PatchedRowFuture")
        self._queue = collections.deque()

    def __call__(self, *args, **kwargs):
        return self.patch(*args, **kwargs)

    def get_batch_size(self):
        return self._resolver.get_batch_size()

    def get_max_concurrent_batches(self):
        return self._resolver.get_max_concurrent_batches()

    def get_queue_limit(self):
        return self._queue_limit

    def get_queue_size(self):
        return len(self._queue)

    def is_queue_overflow(self):
        return self.get_queue_size() >= self.get_queue_limit()

    def patch(self, row, cluster=None):
        if cluster is None:
            cluster = get_attr_or_item(row, "cluster")
        substitutions = []
        for field in self._fields:
            node_id = get_node_id(get_attr_or_item(row, field))
            if node_id:
                path_future = self._resolver(cluster, node_id)
                path_tail = get_attr_or_item(row, field)[1 + len(node_id):]
                substitutions.append((field, path_tail, path_future))
        return self.PatchedRowFuture(row, substitutions, self._patched_row_future_logger)

    def flush(self):
        self._resolver.flush()

    def _soft_empty_the_queue(self):
        while self.get_queue_size() and self._queue[0].done():
            yield self._pop_from_queue()

    def _hard_empty_the_queue(self):
        if self._resolver.get_pending_requests_count() == self._resolver.current_batch_size():
            self._logger.debug("Forced flush.")
            self.flush()
        yield self._pop_from_queue()

    def _pop_from_queue(self):
        future = self._queue.popleft()
        if not future.done():
            self._logger.debug(f"NodeIdPatcher._pop_from_queue(): future({hex(id(future))}) not done.")
        return future.result()

    def batched_patch(self, row, cluster=None):
        yield from self._soft_empty_the_queue()

        if self.is_queue_overflow():
            yield from self._hard_empty_the_queue()

        row_future = self.patch(row, cluster)
        if not self._preserve_order and row_future.done():
            yield row_future.result()
        else:
            self._queue.append(row_future)

    def batched_flush(self):
        self.flush()
        while self.get_queue_size():
            yield self._pop_from_queue()
