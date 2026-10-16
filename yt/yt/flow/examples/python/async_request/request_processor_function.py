"""RequestProcessorFunction: computes response for each request."""

import logging

from yt.yt.flow.library.python.companion.computation import RowFunction

log = logging.getLogger(__name__)


# [BEGIN request_processor]
class RequestProcessorFunction(RowFunction):
    """Stateless: returns length of request string as response."""

    def on_message(self, message, output, ctx):
        request_id = message.payload["request_id"]
        key = message.payload["key"]
        request = message.payload["request"]
        length = len(request)

        builder = ctx.message_builder("response")
        builder.set("request_id", request_id)
        builder.set("key", key)
        builder.set("length", length)
        output.add_message(builder.finish())
        log.debug("Processed request (RequestId: %s)", request_id)
# [END request_processor]
