from .common import round_down_to_minute

from yt_odin.logserver import (
    FULLY_AVAILABLE_STATE,
    PARTIALLY_AVAILABLE_STATE,
    FAILED_STATE,
)

from yt.wrapper.common import update

from six import iteritems, itervalues
from six.moves import queue

from datetime import datetime
from copy import deepcopy
from collections import deque, defaultdict
import logging
from threading import Thread
import time
from email.mime.text import MIMEText
import smtplib
import socket
import traceback
from enum import Enum

MAIL_SERVER = "outbound-relay.yandex.net"

# Logger of odin runner
odin_logger = logging.getLogger("Odin")


def parse_time(time_str):
    return datetime.strptime(time_str, "%H:%M:%S").time()


class AlertLoadError(Exception):
    pass


class AlertConfig(object):
    MAIN_KEYS = ("enable", "threshold", "period", "partially_available_strategy", "juggler_service_name")

    def _get_attr_value(self, attr):
        now_time = datetime.now().time()
        for start_time, end_time, override in self._time_period_overrides:
            if start_time <= now_time <= end_time:
                return override._get_attr_value(attr)
        return self.__dict__["_" + attr]

    def _check_key(self, config, key):
        if key not in config:
            raise AlertLoadError('Key "{}" is missing in alert config'.format(key))

    def __init__(self, config, allow_time_period_overrides=True):
        config = deepcopy(config)

        base_config = {}
        for key in self.MAIN_KEYS:
            self._check_key(config, key)
            value = config.pop(key)
            self.__dict__["_" + key] = value
            base_config[key] = value

        for key in self.MAIN_KEYS:
            self.__dict__["get_" + key] = lambda attr=key: self._get_attr_value(attr)

        self._time_period_overrides = []
        if allow_time_period_overrides and "time_period_overrides" in config:
            for time_period_override in config["time_period_overrides"]:
                self._check_key(time_period_override, "start_time")
                start_time = parse_time(time_period_override.pop("start_time"))

                self._check_key(time_period_override, "end_time")
                end_time = parse_time(time_period_override.pop("end_time"))

                if end_time < start_time:
                    raise AlertLoadError("Time interval of override should be non-empty, "
                                         "but start time {0} is greater than or equal to end time {1}"
                                         .format(start_time, end_time))

                time_period_override = update(base_config, time_period_override)
                self._time_period_overrides.append(
                    (start_time, end_time, AlertConfig(time_period_override, False)))


class JugglerPushThread(Thread):
    _SLEEP_TIME = 1.0

    def __init__(self, juggler_client, juggler_responsibles):
        super(JugglerPushThread, self).__init__()
        self._queue = queue.Queue()
        self._juggler_client = juggler_client
        self._juggler_responsibles = juggler_responsibles

        self._juggler_push_failed = None

    def run(self):
        try:
            import prctl
            prctl.set_name("JugglerPush")
        except ImportError:
            pass

        self._is_running = True

        while True:
            try:
                events = self._queue.get_nowait()
                if events is None:
                    break
            except queue.Empty:
                events = []

            try:
                self._juggler_client.push_events(events)
                self._juggler_push_failed = False
            except:  # noqa
                msg = "Exception occurred in Juggler push thread"
                odin_logger.exception(msg)
                if not self._juggler_push_failed and self._juggler_responsibles:
                    self._send_mail_to_responsibles(
                        "{0}:\n{1}".format(msg, traceback.format_exc()), "Juggler push failed")
                self._juggler_push_failed = True

            time.sleep(self._SLEEP_TIME)

    def enqueue_events(self, events):
        self._queue.put(events)

    def stop(self):
        self._queue.put(None)
        self.join()

    def _send_mail_to_responsibles(self, text, subject):
        from_ = "odin@" + socket.getfqdn()

        msg = MIMEText(text)
        msg["Subject"] = subject
        msg["From"] = from_
        msg["To"] = ", ".join(self._juggler_responsibles)

        s = smtplib.SMTP(MAIL_SERVER)
        s.sendmail(from_, self._juggler_responsibles, msg.as_string())
        s.quit()


class PeriodStatus(Enum):
    OK = "OK"
    WARN = "WARN"
    CRIT = "CRIT"

    @staticmethod
    def priority_order():
        return [PeriodStatus.CRIT, PeriodStatus.WARN, PeriodStatus.OK]


class AlertsManager(object):
    def __init__(self, storage, host, juggler_client, juggler_responsibles):
        self._host = host
        self._storage = storage

        if juggler_client is not None:
            self._push_thread = JugglerPushThread(juggler_client, juggler_responsibles)
            self._push_thread.daemon = True
            self._push_thread.start()
        else:
            self._push_thread = None

        self._alerts_configs = defaultdict(dict)
        self._checks_results = defaultdict(deque)
        self._max_periods = {}

    def _reload_configs(self, check_configs):
        self._alerts_configs.clear()

        for check, check_config in iteritems(check_configs):
            alerts_configs = deepcopy(check_config.get("alerts"))
            if not alerts_configs:
                continue

            for name, config in iteritems(alerts_configs):
                if config:
                    if "juggler_service_name" not in config:
                        config["juggler_service_name"] = check
                    if "enable" not in config:
                        config["enable"] = True
                    if "partially_available_strategy" not in config:
                        config["partially_available_strategy"] = "force_crit"

                    try:
                        loaded_config = AlertConfig(config)
                    except AlertLoadError as err:
                        odin_logger.warning('Failed to load alert config "%s" for check "%s": "%s"',
                                            name, check, str(err))
                        continue

                    self._alerts_configs[check][name] = loaded_config

    def _drop_unexisting_checks(self):
        for check in list(self._checks_results):
            if check not in self._alerts_configs:
                del self._checks_results[check]

    def _update_max_periods_and_retrieve_historical_records(self):
        timestamp = round_down_to_minute(time.time())

        for check in self._alerts_configs:
            max_period = -1

            for config in itervalues(self._alerts_configs[check]):
                max_period = max(max_period, config.get_period())

            assert max_period != -1
            odin_logger.info("Check %s max period is %d", check, max_period)

            if not self._checks_results[check] or (check in self._max_periods and self._max_periods[check] < max_period):
                self._checks_results[check].clear()

                records = self._storage.get_records(check, timestamp - max_period * 60, timestamp)
                odin_logger.info("Retrieved %d historical records for check %s", len(records), check)

                for record in records:
                    self._checks_results[check].append(record.state)

            self._max_periods[check] = max_period

    def reload_alerts_configs(self, check_configs):
        self._reload_configs(check_configs)
        self._drop_unexisting_checks()
        self._update_max_periods_and_retrieve_historical_records()

    def store_check_result(self, check, value):
        if check not in self._alerts_configs:
            return

        if value["description"] is None:
            result = value["state"]
        else:
            result = (value["state"], value["description"])

        self._checks_results[check].append(result)
        while len(self._checks_results[check]) > self._max_periods[check]:
            self._checks_results[check].popleft()

    def _get_check_result_state(self, result):
        if isinstance(result, tuple):
            return result[0]
        if not isinstance(result, (float, int)):
            return FAILED_STATE
        return result

    def _get_check_result_description(self, result):
        if isinstance(result, tuple):
            try:
                return str(result[1])
            except UnicodeEncodeError:
                return repr(result[1])
        return None

    def _get_period_status(self, results, alert_config):
        eps = 0.001

        fully_available_count = 0
        partially_available_count = 0
        for result in results:
            state = self._get_check_result_state(result)
            if abs(state - FULLY_AVAILABLE_STATE) < eps:
                fully_available_count += 1
            elif abs(state - PARTIALLY_AVAILABLE_STATE) < eps:
                partially_available_count += 1
            else:
                pass  # Unavailable.

        if fully_available_count < alert_config.get_threshold():
            if alert_config.get_partially_available_strategy() == "skip" and \
                    partially_available_count == alert_config.get_period() - fully_available_count:
                return PeriodStatus.OK
            if fully_available_count + partially_available_count >= alert_config.get_threshold():
                if alert_config.get_partially_available_strategy() == "force_ok":
                    return PeriodStatus.OK
                elif alert_config.get_partially_available_strategy() == "warn":
                    return PeriodStatus.WARN
            return PeriodStatus.CRIT

        return PeriodStatus.OK

    def _build_description(self, results, status, alert_config, alert_name):
        if status == PeriodStatus.OK:
            sign = ">="
        else:
            sign = "<"

        states = list(map(lambda result: self._get_check_result_state(result), results))

        description_parts = []

        last_check_description = self._get_check_result_description(results[-1])
        if last_check_description is not None:
            description_parts.append("Last description: {}".format(last_check_description))

        description_parts.append("Last {0} states for \"{1}\": {2}".format(
            alert_config.get_period(),
            alert_name,
            states,
        ))

        description_parts.append("Availability {0} {1}".format(
            sign,
            alert_config.get_threshold(),
        ))

        return "; ".join(description_parts)

    def send_alerts(self):
        if self._push_thread is None:
            return

        events = []

        for check, results in iteritems(self._checks_results):
            alerts_configs = self._alerts_configs[check]

            status = None
            description = None

            juggler_service_to_status_to_description = dict()
            for name, config in iteritems(alerts_configs):
                if not config.get_enable():
                    continue

                if len(results) < config.get_period():
                    odin_logger.info("Status of check %s:%s cannot be verified since "
                                     "check does not have enough results (period: %d, available result count: %d)",
                                     check, name, config.get_period(), len(results))
                    continue

                results_slice = list(results)[-config.get_period():]
                status = self._get_period_status(results_slice, config)
                description = self._build_description(results_slice, status, config, name)

                juggler_service = config.get_juggler_service_name()
                if juggler_service not in juggler_service_to_status_to_description:
                    juggler_service_to_status_to_description[juggler_service] = dict()

                juggler_service_to_status_to_description[juggler_service][status] = description

            for juggler_service, status_to_description in juggler_service_to_status_to_description.items():
                for status in PeriodStatus.priority_order():
                    if status not in status_to_description:
                        continue

                    event = {
                        "host": self._host,
                        "service": juggler_service,
                        "status": status.value,
                        "description": status_to_description[status],
                    }
                    events.append(event)
                    break

        self._push_thread.enqueue_events(events)

    def terminate(self):
        if self._push_thread is not None:
            self._push_thread.stop()
