import six
import time
import threading
import yt.wrapper as yt
import logging

logger = logging.getLogger(__name__)


class PipelineFail(Exception):
    pass


class IPipelinePart:
    def __init__(self):
        raise NotImplementedError

    def __call__(self, print_progress=True, abort_concurrent_on_fail=True, semaphore=None):
        raise NotImplementedError

    def abort(self, reason=None):
        raise NotImplementedError


class Chain(IPipelinePart):
    def __init__(self, *gens, title=None, error_handler=None):
        self.gens = gens
        self.title = title
        self.errors = []
        self.error_handler = error_handler

    def __call__(self, print_progress=True, abort_concurrent_on_fail=True, logger=logger, semaphore=None):
        try:
            for gen in self.gens:
                yield from gen(print_progress=print_progress, abort_concurrent_on_fail=abort_concurrent_on_fail, logger=logger, semaphore=semaphore)
                self.errors.extend(gen.errors)
                if self.errors:
                    if abort_concurrent_on_fail:
                        raise PipelineFail(self.errors)
                    return

        except Exception as e:
            if self.error_handler is None or not self.error_handler(e):
                self.abort(f"Part of chain has excepted in [{self.title}]")
                logger.error(f"Exception in [{self.title}]")
                raise e

    def abort(self, reason=None):
        for gen in self.gens:
            gen.abort(reason)


class Parallel(IPipelinePart):
    def __init__(self, *gens, title=None, error_handler=None):
        self.gens = gens
        self.title = title
        self.errors = []
        self.error_handler = error_handler

    def __call__(self, print_progress=True, abort_concurrent_on_fail=True, logger=logger, semaphore=None):
        try:
            gens = {
                idx: gen(print_progress=print_progress, abort_concurrent_on_fail=abort_concurrent_on_fail, logger=logger, semaphore=semaphore)
                for idx, gen in enumerate(self.gens)
            }
            while gens:
                filtered_out_gens = []
                for idx, gen in six.iteritems(gens):
                    try:
                        yield next(gen)
                    except StopIteration:
                        self.errors.extend(self.gens[idx].errors)
                        filtered_out_gens.append(idx)

                        if self.errors and abort_concurrent_on_fail:
                            raise PipelineFail(self.errors)
                for gen_idx in filtered_out_gens:

                    gens.pop(gen_idx)

        except Exception as e:
            if self.error_handler is None or not self.error_handler(e):
                self.abort(f"Concurrent line failed in [{self.title}]")
                logger.error(f"Exception in [{self.title}]")
                raise e

    def abort(self, reason=None):
        for gen in self.gens:
            gen.abort(reason)


class Function(IPipelinePart):
    def __init__(self, function, title=None, error_handler=None):
        self.function = function
        self.title = title
        self.errors = []
        self.error_handler = error_handler

    def __call__(self, print_progress=True, abort_concurrent_on_fail=True, logger=logger, semaphore=None):
        try:
            yield self.function()
        except Exception as e:
            if self.error_handler is None or not self.error_handler(e):
                self.abort(f"Function call excepted in [{self.title}]")
                logger.error(f"Exception in [{self.title}]")
                raise e

    def abort(self, reason=None):
        pass


class AsyncOp(IPipelinePart):
    def __init__(self, yt_client, spec):
        self.yt_client = yt_client
        self.spec = spec
        self.op = None
        self.errors = []

    def __call__(self, print_progress=True, abort_concurrent_on_fail=True, logger=logger, semaphore=None):
        while semaphore is not None and not semaphore.acquire(blocking=False):
            yield "Waiting"
        self.op = self.yt_client.run_operation(
            self.spec,
            sync=False,
        )
        while True:
            time.sleep(1)
            state = self.op.get_state()
            if print_progress:
                self.op.printer(state)
            if state is not None and state.is_finished():
                if state.is_unsuccessfully_finished():
                    error = self.op.get_error(state=state)
                    assert error is not None
                    self.errors.append(error)
                yield state
                break
            yield state

        if semaphore is not None:
            semaphore.release()

    def abort(self, reason=None):
        if self.op is not None:
            self.op.abort(reason=reason)


class Workspace(IPipelinePart):
    """
    Will attempt to take shared lock on workspace_root with child_key for each child in child_keys list
    """
    def __init__(self, yt_client, paths, remove, gen, title=None, skip_on_lock_error=True, error_handler=None):
        self.yt_client = yt_client
        self.paths = sorted(path.rsplit("/", 1) for path in paths)
        self.gen = gen
        self.remove = remove
        self.errors = []
        self.title = title
        self.error_handler = error_handler
        self.skip_on_lock_error = skip_on_lock_error

    def __call__(self, print_progress=True, abort_concurrent_on_fail=True, logger=logger, semaphore=None):
        try:
            with self.yt_client.Transaction():
                for root, child in self.paths:
                    self.yt_client.lock(root, mode="shared", child_key=child)
                yield from self.gen(print_progress=print_progress, abort_concurrent_on_fail=abort_concurrent_on_fail, logger=logger, semaphore=semaphore)
                self.errors.extend(self.gen.errors)
                if self.errors:
                    if abort_concurrent_on_fail:
                        raise PipelineFail(self.errors)
                    return

                if self.remove:
                    batch_client = self.yt_client.create_batch_client()
                    for path in self.paths:
                        batch_client.remove("/".join(path))
                    batch_client.commit_batch()
        except Exception as e:
            if isinstance(e, yt.YtResponseError) and e.is_cypress_transaction_lock_conflict() and self.skip_on_lock_error:
                logger.info(f"Skipping [{self.title}] due to a lock conflict on [{', '.join('/'.join(path) for path in self.paths)}]")
                return
            if self.error_handler is None or not self.error_handler(e):
                self.abort(f"Function call excepted in [{self.title}]")
                logger.error(f"Exception in [{self.title}]")
                raise e

    def abort(self, reason=None):
        self.gen.abort(reason=reason)


class PipelineWatcher:
    def __init__(self, pipeline, logger=logger, max_concurrent_operations=None):
        self.pipeline = pipeline
        self.logger = logger
        self.semaphore = None
        if max_concurrent_operations is not None:
            self.semaphore = threading.Semaphore(max_concurrent_operations)

    def wait(self, print_progress=True, abort_concurrent_on_fail=True):
        for _ in self.pipeline(print_progress=print_progress, abort_concurrent_on_fail=abort_concurrent_on_fail, logger=self.logger, semaphore=self.semaphore):
            pass
        if self.errors:
            raise yt.YtError("Some operations finished unsuccessfully", inner_errors=self.errors)

    @property
    def errors(self):
        return self.pipeline.errors
