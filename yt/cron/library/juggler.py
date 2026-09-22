from enum import Enum

from yt.packages.urllib3 import Retry
import yt.packages.requests as requests


class EventStatus(Enum):
    OK = "OK"
    CRIT = "CRIT"
    WARN = "WARN"
    INFO = "INFO"


class JugglerClient:
    """The juggler client sending raw events

    :param host: default host for raw events
    """
    def __init__(self, host=None):
        self._host = host
        self._session = requests.Session()
        retries = 10
        retry = Retry(
            backoff_factor=0.3,
            read=retries,
            connect=retries,
            total=retries,
            status=retries,
        )
        self._session.mount(
            "http://",
            requests.adapters.HTTPAdapter(max_retries=retry),
        )
        self._session.mount(
            "https://",
            requests.adapters.HTTPAdapter(max_retries=retry),
        )

    def push(self, source, events):
        """ Push list of events to solomon

        :param source: application name
        :param events: list of raw events documented here: https://docs.yandex-team.ru/juggler/raw_events
        """
        for event in events:
            if "host" not in event:
                event["host"] = self._host
        result = self._session.post(
            "http://juggler-push.search.yandex.net/events",
            json={
                "source": source,
                "events": events,
            },
            timeout=10,
        )
        result.raise_for_status()
