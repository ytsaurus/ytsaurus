"""
Check whether queue agent object controllers are running by comparing minimal pass instant
with current time.

Options explanation:
{
    "max_lag_ms": 60 * 1000,  # Max difference between now and minimal pass instant in milliseconds
    "ignore_unreachable_instances": True,  # False if failing to get "controller_info" should result in check failure.
}
"""

from yt_odin_checks.lib.check_runner import main
from yt.wrapper import YtClient

from yt import yson

from collections import namedtuple, defaultdict
from dacite import from_dict
from dataclasses import dataclass
import datetime
from logging import Logger
import pytz

BANNED_ATTRIBUTE_NAME = "banned"
DEFAULT_MAX_LAG_MS = 60 * 1000


@dataclass
class QueueAgentControllerLivenessOptions:
    # Passed automatically by Odin.
    cluster_name: str

    max_lag_ms: int = 60 * 1000
    ignore_unreachable_instances: bool = True


ControllerPass = namedtuple("ControllerPass", ["path", "pass_instant"])


class InactiveObjects:
    FIELDS = [
        "leading_queues",
        "leading_consumers",
        "following_queues",
        "following_consumers",
    ]

    @staticmethod
    def _parse_passes_list(passes_list_raw):
        return [
            ControllerPass(
                path=controller_pass_raw["path"],
                pass_instant=datetime.datetime.fromisoformat(controller_pass_raw["pass_instant"])
            ) for controller_pass_raw in passes_list_raw
        ]

    def __init__(self, inactive_objects_raw):
        for field in self.FIELDS:
            passes_list_raw = inactive_objects_raw[field]

            setattr(self, field, self._parse_passes_list(passes_list_raw))

    def __str__(self):
        state = {field: getattr(self, field) for field in self.FIELDS}
        return f"InactiveObjects({state!r})"


class ControllerInfo:
    def __init__(self, raw_controller_info):
        self.inactive_objects = InactiveObjects(raw_controller_info["inactive_objects"])
        self.erroneous_objects = raw_controller_info["erroneous_objects"]

    def __str__(self):
        return f"ControllerInfo(inactive_objects={self.inactive_objects}, erroneous_objects={self.erroneous_objects!r})"


class QueueAgentControllerLivenessChecker:
    def __init__(self, client: YtClient, logger: Logger, options: QueueAgentControllerLivenessOptions, states):
        self.client = client
        self.logger = logger
        self.options = options
        self.states = states

        self.running_instances = []
        self.has_failed_controller_passes = False
        self.failed_controller_passes: dict[str, dict[str, list[ControllerPass]]] = defaultdict(lambda: defaultdict(list))
        self.unreachable_instances = []
        self.last_unreachable_exception = None

    def check(self):
        queue_agent_instances = self.client.list("//sys/queue_agents/instances", attributes=[BANNED_ATTRIBUTE_NAME])

        now = datetime.datetime.now(pytz.UTC)
        self.logger.info(f"Using {now} as now() value")
        self.logger.info(f"Options: {self.options}")

        for yson_instance in queue_agent_instances:
            instance = str(yson_instance)
            if yson_instance.has_attributes() and BANNED_ATTRIBUTE_NAME in yson_instance.attributes:
                attribute_value = yson_instance.attributes[BANNED_ATTRIBUTE_NAME]
                # NB(apachee): Check type to match the behavior of queue agent sharding manager (it ignores anything except bool).
                if isinstance(attribute_value, yson.YsonBoolean) and attribute_value:
                    self.logger.info(f"Skipping banned instance {instance}")
                    continue

            try:
                controller_info = ControllerInfo(self.client.get(f"//sys/queue_agents/instances/{instance}/orchid/queue_agent/controller_info"))
            except Exception as ex:
                if not self.options.ignore_unreachable_instances:
                    self.last_unreachable_exception = ex
                    self.unreachable_instances.append(instance)
                continue

            self.handle_controller_info(now, instance, controller_info)

        if self.has_failed_controller_passes:
            # NB(apachee): Default dict has a terrible string representation.
            dict_failed_controller_passes = {k: dict(v) for k, v in self.failed_controller_passes.items()}

            for instance, failed_controller_passes in dict_failed_controller_passes.items():
                self.logger.error(f"Failed controller passes on instance {instance}: {failed_controller_passes}")

            error_msg = f"Check failed due to these controller passes: {dict_failed_controller_passes}"
            return self.states.UNAVAILABLE_STATE, error_msg

        if len(self.unreachable_instances) > 0:
            error_msg = f"Some instances are unreachable: {self.unreachable_instances}"
            self.logger.error(error_msg)
            if self.last_unreachable_exception is not None:
                self.logger.exception(self.last_unreachable_exception)
            return self.states.PARTIALLY_AVAILABLE_STATE, error_msg

        if len(self.running_instances) == 0:
            error_msg = "No controllers are running"
            self.logger.error(error_msg)
            return self.states.PARTIALLY_AVAILABLE_STATE, error_msg

        return self.states.FULLY_AVAILABLE_STATE

    def handle_controller_info(self, now: datetime.datetime, instance: str, controller_info: ControllerInfo):
        self.logger.debug(f"Controller info on instance {instance}: {controller_info}")

        for field in InactiveObjects.FIELDS:
            if len(getattr(controller_info.inactive_objects, field)) == 0:
                self.logger.debug(f"No passes present on instance {instance} for {field} controller passes")
                continue
            self.running_instances.append(instance)
            least_active_object = getattr(controller_info.inactive_objects, field)[0]
            least_active_object_ts = least_active_object.pass_instant
            lag_ms = (now - least_active_object_ts).total_seconds() * 1000
            if lag_ms > self.options.max_lag_ms:
                self.has_failed_controller_passes = True
                self.failed_controller_passes[instance][field].append(least_active_object)
                self.logger.debug(f"For {instance} ({field}) object {least_active_object} exceeded max lag milliseconds with lag {lag_ms}ms")
            else:
                self.logger.debug(f"For {instance} ({field}) least active object is {least_active_object} with lag {lag_ms}ms")


def run_check(secrets, yt_client: YtClient, logger: Logger, options, states):
    typed_options = from_dict(QueueAgentControllerLivenessOptions, options)

    checker = QueueAgentControllerLivenessChecker(client=yt_client, logger=logger, options=typed_options, states=states)
    return checker.check()


if __name__ == "__main__":
    main(run_check)
