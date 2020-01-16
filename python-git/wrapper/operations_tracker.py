from .config import get_config, get_backend_type
from .common import get_value
from .errors import YtError, YtResponseError
from .exceptions_catcher import ExceptionCatcher
from .operation_commands import (Operation, _create_operation_failed_error, PrintOperationInfo,
                                 get_operation_attributes, get_operation_state, get_operation_url,
                                 OperationState, abort_operation)
from .spec_builders import SpecBuilder
from .run_operation_commands import run_operation
from .batch_helpers import batch_apply

import yt.logger as logger
from yt.packages.six.moves import xrange, zip

from collections import namedtuple
from time import sleep
from collections import deque, defaultdict
from threading import Thread, RLock
from copy import deepcopy

def copy_client(client):
    from .client import YtClient
    return YtClient(config=deepcopy(get_config(client)))


class TrackerOperation(object):
    def __init__(self, id, client=None):
        self.id = id
        self.client = copy_client(client)
        self.printer = PrintOperationInfo(id, client=client)
        self.url = get_operation_url(id, client=client)

    def get_state(self):
        return get_operation_state(self.id, client=self.client)

    def abort(self):
        abort_operation(self.id, client=self.client)

class _OperationsTrackingThread(Thread):
    def __init__(self, poll_period, print_progress, batch_size):
        super(_OperationsTrackingThread, self).__init__()
        self._poll_period = poll_period
        self._print_progress = print_progress
        self._operations_to_track = deque()
        self._thread_lock = RLock()

        self.finished = False
        self.errors = []

        self.daemon = True

        self.processing_operations_count = 0
        self.should_abort_all = False
        self.batch_size = batch_size

    def add(self, operation):
        with self._thread_lock:
            self._operations_to_track.append(operation)

    def run(self):
        while not self.finished:
            self._check_operations()

            # NOTE: Wait time should decrease with the number of operations
            # because if poll period is fixed and number of operations is large then
            # last added operation can be removed from cypress faster than
            # tracker checks it.
            with self._thread_lock:
                coef = 1.0 / max(len(self._operations_to_track) + (2 * self.processing_operations_count / self.batch_size), 1)
            sleep(coef * self._poll_period / 1000.0)

    def _check_operations(self):
        with self._thread_lock:
            if self.should_abort_all:
                while self._operations_to_track:
                    operation = self._operations_to_track.popleft()
                    operation.abort()
                    logger.info("Operation %s was aborted", operation.id)
                self.should_abort_all = False

        cluster_to_operations = defaultdict(list)
        cluster_to_states = defaultdict(list)
        with self._thread_lock:
            actual_batch_size = min(self.batch_size, len(self._operations_to_track))
            for i in xrange(actual_batch_size):
                operation = self._operations_to_track.popleft()
                self.processing_operations_count += 1
                if get_backend_type(operation.client) == "native" or operation.client is None:
                    logger.warning("Batch polling for native back-end is not supported")
                    proxy = None
                else:
                    proxy = operation.client.config.get("proxy", {}).get("url", None)

                cluster_to_operations[proxy].append(operation)

        # NB: Not in _thread_lock.
        # It is intentional since operation start request may be time-consuming.
        for proxy in cluster_to_operations:
            if proxy is None:
                # We are working with native client, so polling for operation state one-by-one.
                for operation in cluster_to_operations[proxy]:
                    cluster_to_states[proxy].append(operation.get_state())
            else:
                # Batch request is sent from _some_ client.
                client = copy_client(cluster_to_operations[proxy][0].client)
                data = [op.id for op in cluster_to_operations[proxy]]
                logger.debug("Fetching states of operations in %s", proxy)

                def get_state(operation, client=client):
                    return get_operation_attributes(operation, fields=["state"], client=client)

                try:
                    op_states_raw = batch_apply(get_state, data, client)
                except YtError:
                    logger.exception("Failed to get operation states")
                    cluster_to_states[proxy] = [None for op in cluster_to_operations[proxy]]
                else:
                    cluster_to_states[proxy] = [OperationState(operation_state["state"]) for operation_state in op_states_raw]

        with self._thread_lock:
            for proxy in cluster_to_operations:
                for operation, state in zip(cluster_to_operations[proxy], cluster_to_states[proxy]):
                    self.processing_operations_count -= 1
                    if self._print_progress and state is not None:
                        operation.printer(state)
                    if state is not None and state.is_finished():
                        if state.is_unsuccessfully_finished():
                            self.errors.append(_create_operation_failed_error(operation, state))
                    else:
                        self._operations_to_track.append(operation)

    def get_operation_count(self):
        with self._thread_lock:
            return len(self._operations_to_track) + self.processing_operations_count

    def abort_operations(self):
        with self._thread_lock:
            if self.get_operation_count() > 0:
                self.should_abort_all = True

    def stop(self):
        self.finished = True
        self.join()


class OperationsTrackerBase(object):
    """Base Operations Tracker class.
       It has controls for working with some Operations.
    """

    THREAD_CLASS = _OperationsTrackingThread

    def __init__(self, poll_period=5000, abort_on_sigint=True, print_progress=True, batch_size=100):
        self.operations = {}

        self._poll_period = poll_period
        self._abort_on_sigint = abort_on_sigint

        self._tracking_thread = self.THREAD_CLASS(poll_period, print_progress, batch_size)
        self._tracking_thread.start()

    def _add_operation(self, operation):
        if operation.id in self.operations:
            raise YtError("Operation {0} is already tracked".format(operation.id))
        self.operations[operation.id] = operation
        self._tracking_thread.add(operation)

    def add(self, operation):
        """Adds Operation object to tracker.

        :param Operation operation: operation to track.
        """

        if operation is None:
            return

        if not isinstance(operation, Operation):
            raise YtError("Valid Operation object should be passed "
                          "to add method, not {0!r}".format(operation))

        if not operation.exists():
            raise YtError("Operation {0} is already tracked".format(operation.id))

        tracker_operation = TrackerOperation(operation.id, client=operation.client)
        self._add_operation(tracker_operation)

    def wait_all(self, check_result=True, abort_exceptions=(KeyboardInterrupt,), keep_finished=False):
        """Waits all added operations and prints progress.

        :param bool check_result: if `True` then :class:`YtError <yt.common.YtError>` will be raised if \
        any operation failed. For each failed operation \
        :class:`YtOperationFailedError <yt.wrapper.errors.YtOperationFailedError>` \
        object with details will be added to raised error.

        :param tuple abort_exceptions: if any exception from this tuple is caught all operations \
        will be aborted.
        :param bool keep_finished: do not remove finished operations from tracker, just wait for them to finish.
        """
        logger.info("Waiting for all operations to finish...")

        with ExceptionCatcher(abort_exceptions, self.abort_all, enable=self._abort_on_sigint):
            while self._tracking_thread.get_operation_count() > 0:
                sleep(self._poll_period / 1000.0)
                if not self._tracking_thread.is_alive():
                    raise YtError("OperationsTracker tracking thread died unexpectedly")

        inner_errors = self._tracking_thread.errors
        self._tracking_thread.errors = []

        if not keep_finished:
            self.operations.clear()

        if check_result and inner_errors:
            raise YtError("All tracked operations finished but {0} operations finished unsucessfully"
                          .format(len(inner_errors)), inner_errors=inner_errors)

    def abort_all(self):
        """Aborts all added operations."""
        logger.info("Aborting all operations")
        self._tracking_thread.abort_operations()
        self.wait_all(check_result=False)

    def get_operation_count(self):
        """Return current number of operations in tracker."""
        return self._tracking_thread.get_operation_count()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            self.wait_all()
        else:
            logger.warning(
                "Operations wait failed with error %s, aborting all operations in tracker...",
                repr(exc_value))
            self.abort_all()

    def __del__(self):
        self._tracking_thread.stop()


class OperationsTracker(OperationsTrackerBase):
    """Holds operations and allows to wait or abort all tracked operations."""

    def add_by_id(self, operation_id, client=None):
        """Adds operation to tracker (by operation id)

        :param str operation_id: operation id.
        """

        try:
            operation = TrackerOperation(operation_id, client=client)
        except YtResponseError as err:
            if not err.is_resolve_error():
                raise
            logger.warning("Operation %s does not exist and is not added", operation_id)
            return

        tracker_operation = TrackerOperation(operation.id, client=operation.client)
        self._add_operation(tracker_operation)

SpecTask = namedtuple("SpecTask", ["spec_builder", "client", "enable_optimizations"])

class _OperationsTrackingPoolThread(_OperationsTrackingThread):
    def __init__(self, *args, **kwargs):
        super(_OperationsTrackingPoolThread, self).__init__(*args, **kwargs)

        self._pool_size = None
        self._queue = deque()
        self.should_clear_queue = False

    def set_pool_size(self, pool_size):
        self._pool_size = pool_size

    def add(self, spec_task):
        """Adds SpecTask object to queue.

        :param SpecTask spec_task: task to track.
        """
        if spec_task is None:
            return

        if not isinstance(spec_task, SpecTask):
            raise YtError("Valid SpecTask object should be passed "
                          "to add method, not {0}".format(repr(spec_task)))

        with self._thread_lock:
            new_spec_task = SpecTask(spec_task.spec_builder, copy_client(spec_task.client), spec_task.enable_optimizations)
            self._queue.append(new_spec_task)

    def _check_operations(self):
        super(_OperationsTrackingPoolThread, self)._check_operations()
        with self._thread_lock:
            if self.should_clear_queue:
                self._queue.clear()
                self.should_clear_queue = False
        spec_tasks = []
        operations = []
        with self._thread_lock:
            while self._pool_size is None or len(self._operations_to_track) + len(spec_tasks) < self._pool_size:
                if len(self._queue) == 0:
                    break
                spec_tasks.append(self._queue.popleft())
            self.processing_operations_count += len(spec_tasks)

        # NB: Not in _thread_lock.
        # It is intentional, operation start may be long operation.
        for spec_task in spec_tasks:
            operation = run_operation(
                spec_task.spec_builder,
                sync=False,
                enable_optimizations=spec_task.enable_optimizations,
                client=spec_task.client)
            operations.append(operation)

        with self._thread_lock:
            self._operations_to_track.extend(operations)
            self.processing_operations_count -= len(operations)

    def get_operation_count(self):
        with self._thread_lock:
            return len(self._operations_to_track) + len(self._queue) + self.processing_operations_count

    def abort_operations(self):
        with self._thread_lock:
            if self.get_operation_count() > 0:
                self.should_clear_queue = True
        super(_OperationsTrackingPoolThread, self).abort_operations()


class OperationsTrackerPool(OperationsTrackerBase):
    """Pool for operations that are started automatically. """

    THREAD_CLASS = _OperationsTrackingPoolThread

    def __init__(self, pool_size, enable_optimizations=True, client=None, **kwargs):
        super(OperationsTrackerPool, self).__init__(**kwargs)
        self._tracking_thread.set_pool_size(pool_size)

        self.enable_optimizations = enable_optimizations
        self.client = client

    def _is_spec_builder(self, spec_builder):
        return isinstance(spec_builder, SpecBuilder)

    def add(self, spec_builder, enable_optimizations=None, client=None):
        """Adds Operation object to tracker.

        :param SpecBuilder spec_builder: spec_builder to run operation and track it.
        """

        enable_optimizations = get_value(enable_optimizations, self.enable_optimizations)
        client = get_value(client, self.client)

        if not self._is_spec_builder(spec_builder):
            raise YtError("Spec builder is not valid")

        task = SpecTask(spec_builder, copy_client(client), enable_optimizations)
        self._tracking_thread.add(task)

    def map(self, spec_builders, enable_optimizations=None, client=None):
        """Adds Operation object to tracker.

        :param spec_builders: spec_builders to run operations and track them.
        """
        enable_optimizations = get_value(enable_optimizations, self.enable_optimizations)
        client = get_value(client, self.client)

        spec_builders = list(spec_builders)
        if not all(self._is_spec_builder(spec_builder) for spec_builder in spec_builders):
            raise YtError("Some of the spec builders are not valid")

        for spec_builder in spec_builders:
            self._tracking_thread.add(SpecTask(spec_builder, copy_client(client), enable_optimizations))
