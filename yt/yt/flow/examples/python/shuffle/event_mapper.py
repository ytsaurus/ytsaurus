"""EventMapper: RowFunction that parses JSON data and emits event messages."""

import json
import logging

from yt.yt.flow.library.python.companion.computation import RowFunction

log = logging.getLogger(__name__)


# [BEGIN event_mapper]
class EventMapper(RowFunction):
    """Parses JSON from data field and emits event messages with key fields."""

    def on_message(self, message, output, ctx):
        data = message.payload["data"]
        try:
            parsed = json.loads(data)
        except (json.JSONDecodeError, TypeError) as e:
            log.error("Error parsing json from data field: %s", e)
            raise

        builder = ctx.message_builder("event")
        builder.set("value", parsed.get("value"))
        builder.set("key_a", parsed.get("key_a"))
        builder.set("key_b", parsed.get("key_b"))
        builder.set("key_c", parsed.get("key_c"))
        builder.set("key_d", parsed.get("key_d"))
        output.add_message(builder.finish())
# [END event_mapper]
