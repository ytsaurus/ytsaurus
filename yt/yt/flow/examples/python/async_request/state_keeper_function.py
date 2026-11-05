"""StateKeeperFunction: handles events and responses."""

import logging
import random

from yt.yt.flow.library.python.companion.computation import RowFunction

log = logging.getLogger(__name__)


# [BEGIN state_keeper]
class StateKeeperFunction(RowFunction):
    """Sends requests for events; accumulates total_length for responses."""

    def on_message(self, message, output, ctx):
        stream_id = message.stream_id

        if stream_id == "event":
            key = message.payload["key"]
            data = message.payload["data"]
            request_id = random.getrandbits(64)

            builder = ctx.message_builder("request")
            builder.set("request_id", request_id)
            builder.set("key", key)
            builder.set("request", data)
            output.add_message(builder.finish())
            log.debug("Send request (RequestId: %s)", request_id)

        elif stream_id == "response":
            length = message.payload["length"]
            request_id = message.payload["request_id"]
            log.debug("Received response (RequestId: %s)", request_id)

            state = ctx.external_state("/state", message)
            current = state.get("total_length")
            total = (current if current is not None else 0) + length
            builder = state.to_builder()
            builder.set("total_length", total)
            state.set(builder.finish())
        else:
            raise ValueError(f"Unknown stream_id: {stream_id}")


# [END state_keeper]
