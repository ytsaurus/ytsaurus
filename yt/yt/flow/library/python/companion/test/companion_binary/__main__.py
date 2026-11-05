"""Simple passthrough companion binary for E2E testing."""

import logging

from yt.yt.flow.library.python.companion import Pipeline
from yt.yt.flow.library.python.companion.row import Message
from yt.yt.flow.library.python.companion.computation import RowFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class PassthroughMapper(RowFunction):
    """Simple passthrough: re-emits each input message."""

    def on_message(self, message, output, ctx):
        output.add_message(
            Message(
                message_id=message.message_id,
                stream_id=message.stream_id,
                payload=message.payload,
            )
        )


def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("mapper", PassthroughMapper())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")


if __name__ == "__main__":
    main()
