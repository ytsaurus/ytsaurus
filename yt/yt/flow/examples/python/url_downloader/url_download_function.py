"""UrlDownloadFunction: RowFunction processing URL download events."""

import logging
import time

from yt.yt.flow.library.python.companion.computation import RowFunction

log = logging.getLogger(__name__)


# [BEGIN url_download_function]
class UrlDownloadFunction(RowFunction):
    """Groups URLs by host, processes them with a timer-driven loop."""

    def on_message(self, message, output, ctx):
        host = message.payload["host"]
        url = message.payload["url"]

        state = ctx.state("host-state", message)
        data = state.get_or_default({"host": host, "pending_urls": []})
        data["pending_urls"].append(url)
        state.set(data)

        output.add_timer(int(time.time()) + 5)

    def on_timer(self, timer, output, ctx):
        state = ctx.state("host-state", timer)
        data = state.get_or_default(None)
        if not data or not data.get("pending_urls"):
            state.clear()
            return

        host = data["host"]
        for url in list(data["pending_urls"]):
            result = _process_url(url)
            builder = ctx.message_builder("processed_urls")
            builder.set("host", host)
            builder.set("url", url)
            builder.set("data", result)
            output.add_message(builder.finish())
            log.debug("Processed url (Host: %s, Url: %s)", host, url)

        state.clear()
# [END url_download_function]


def _process_url(url: str) -> str:
    digits = sum(1 for c in url if c.isdigit())
    return f"length: {len(url)}, digits: {digits}"
