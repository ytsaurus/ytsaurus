import random
import time

from yt.wrapper import YtError
import yt.wrapper

from yt.yt.tests.library.smooth_movement_helper import SmoothMovementHelperBase, CommandProvider

from .log import logger
from .runner import ThreadWrapper, Periodic, ThreadStatistics


class YtWrapperCommandProvider(CommandProvider):
    _command_mapping = {
        "list": "list",
    }

    client: yt.wrapper.YtClient = yt.wrapper

    @classmethod
    def _make_command(cls, command_name):
        return lambda self, *args, **kwargs: getattr(self.client, command_name)(*args, **kwargs)

    def wait(self, callback):
        start_time = time.time()
        while time.time() - start_time < 300:
            if callback():
                return
            time.sleep(0.3)
        raise Exception("Wait failed")

    def print_debug(self, *args, **kwargs):
        logger.debug(*args, **kwargs)


class SmoothMovementHelper(SmoothMovementHelperBase, YtWrapperCommandProvider):
    def __init__(self, *args, client=None, **kwargs):
        if client is not None:
            self.client = client
        super().__init__(*args, **kwargs)


class SmoothMovementRunner(ThreadWrapper, Periodic):
    def __init__(self, tables, period=0, client=None):
        self.tables = tables
        assert client is not None
        self.client = client
        self.completed = 0
        self.failed = 0

        super().__init__(period=period)

    def do_run_once(self, trace=None):
        try:
            table = random.choice(self.tables)
            tablet_id = random.choice(
                [t["tablet_id"] for t in self.client.get(table + "/@tablets")])
            h = SmoothMovementHelper(tablet_id, client=self.client)
            logger.debug(
                f"Moving tablet {h.tablet_id} of table {table} from {h.source_cell_id} "
                f"to {h.target_cell_id}")
            h.start()
        except YtError as e:
            if hasattr(e, "error") and "message" in e.error:
                message = e.error["message"]
            else:
                message = e.message

            if not any(text in message for text in ["Only mounted tablet can be moved", "already participating", "is in state"]):
                logger.debug("Failed to create smooth movement action", exc_info=True)
            if "already participating" not in message:
                logger.info(f"Failed to create smooth movement action: {message}")
            self.failed += 1
            return

        logger.debug(f"Action id: {h.action_id}")
        h.wait_for_action(ignore_errors=True)
        if h.get_action_state() == "completed":
            self.completed += 1
            logger.debug(f"Action {h.action_id} completed")
        else:
            self.failed += 1
            error = h.get_action_error()
            if error is not None:
                try:
                    raise error
                except YtError:
                    logger.debug("Smooth movement action failed", exc_info=True)
                    logger.info("Smooth movement action failed")

    def do_collect_statistics(self):
        result = ThreadStatistics(self.completed, self.failed, id="Smooth move " + ", ".join(self.tables))
        self.completed = 0
        self.failed = 0
        return result

    def get_sleep_error_message(self, delay):
        return f"Waiting for {delay}s until next smooth movement"
