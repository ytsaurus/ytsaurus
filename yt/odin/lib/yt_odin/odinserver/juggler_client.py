from yt.wrapper.retries import Retrier
import yt.packages.requests as requests

from six.moves import http_client as httplib

import socket
import json
import logging

logger = logging.getLogger("Odin")


class JugglerError(Exception):
    pass


class _JugglerRequestRetrier(Retrier):
    def __init__(self, url, request_timeout, retry_count):
        retry_config = {
            "enable": True,
            "count": retry_count,
            "backoff": {
                "policy": "rounded_up_to_request_timeout"
            }
        }

        self.events = None
        self.url = url

        super(_JugglerRequestRetrier, self).__init__(
            retry_config,
            timeout=request_timeout,
            exceptions=(requests.RequestException, socket.error, httplib.BadStatusLine, httplib.IncompleteRead)
        )

    def except_action(self, error, attempt):
        logger.warning("HTTP POST request %s has failed with error %s, message: '%s'",
                       self.url, str(type(error)), str(error))

    def action(self):
        rsp = requests.post(self.url, data=json.dumps(self.events))
        rsp.raise_for_status()

        events_results = json.loads(rsp.content)["events"]

        logger.debug("Juggler push result: %s", rsp.content)

        failed_event_count = 0
        for event_result, event in zip(events_results, self.events):
            if event_result["code"] != 200:
                logger.warning('Failed to push event "%s" to juggler: "%s", code: "%d"',
                               event, event_result["message"], event_result["code"])
                failed_event_count += 1

        if failed_event_count == len(self.events):
            raise JugglerError("Juggler failed to push all events in one batch. "
                               "See log for more details.")

    def send_events(self, events):
        self.events = events
        self.run()


class JugglerClient(object):
    """Client for juggler. See `wiki <https://wiki.yandex-team.ru/sm/juggler/batching/#juggler-server-batch>`_
       for details.
    """
    HTTP_REQUEST_TIMEOUT = 10000  # in milliseconds
    HTTP_RETRY_COUNT = 5

    def __init__(self, host, port=None, scheme=None):
        scheme = scheme or "http"
        url = "{0}://{1}{2}/api/1/batch".format(
            scheme, host, ":" + str(port) if port else "")
        self.url = url

        self.retrier = _JugglerRequestRetrier(
            url,
            self.HTTP_REQUEST_TIMEOUT,
            self.HTTP_RETRY_COUNT)

    def get_status(self, host, service):
        raise NotImplementedError()

    def push_events(self, events):
        if events:
            logger.info("Sending events to Juggler (count: %d, events: %s)", len(events), events)
            self.retrier.send_events(events)
