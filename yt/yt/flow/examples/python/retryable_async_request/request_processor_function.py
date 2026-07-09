"""RequestProcessorFunction: processes requests with retry logic using timers."""

import logging
import time

from yt.yt.flow.library.python.companion.computation import RowFunction

log = logging.getLogger(__name__)

MAX_RETRIES = 3
DELAY_SECONDS = 5


def _is_request_successful(request_id: int, failed_attempts: int) -> bool:
    return (request_id + failed_attempts) % MAX_RETRIES == 0


# [BEGIN request_processor]
class RequestProcessorFunction(RowFunction):
    """Retries requests up to MAX_RETRIES times; emits response on success."""

    def on_message(self, message, output, ctx):
        request_id = message.payload["request_id"]
        data = {
            "request_id": request_id,
            "key": message.payload["key"],
            "request": message.payload["request"],
            "failed_attempts": 0,
        }
        state = ctx.state("request-state", message)
        self._try_or_retry(state, data, output, ctx)

    def on_timer(self, timer, output, ctx):
        state = ctx.state("request-state", timer)
        data = state.get_or_default(None)
        if data is not None:
            self._try_or_retry(state, data, output, ctx)

    def _try_or_retry(self, state, data, output, ctx):
        request_id = data["request_id"]
        failed_attempts = data["failed_attempts"]

        if not _is_request_successful(request_id, failed_attempts):
            data["failed_attempts"] += 1
            state.set(data)
            output.add_timer(int(time.time()) + DELAY_SECONDS)
            log.debug(
                "Failed request (RequestId: %s, FailedAttempts: %s)",
                request_id,
                data["failed_attempts"],
            )
        else:
            key = data["key"]
            length = len(data["request"])

            builder = ctx.message_builder("response")
            builder.set("request_id", request_id)
            builder.set("key", key)
            builder.set("length", length)
            output.add_message(builder.finish())

            state.clear()
            log.debug(
                "Processed request (RequestId: %s, FailedAttempts: %s)",
                request_id,
                failed_attempts,
            )
# [END request_processor]
