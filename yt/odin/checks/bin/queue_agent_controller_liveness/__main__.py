from yt_odin_checks.lib.check_runner import main
from yt.wrapper import YtClient

from yt import yson

from collections import namedtuple
import datetime
from logging import Logger
import pytz


"""
Check whether queue agent object controllers are running by comparing minimal pass instant
with current time.

Options explanation:
{
    "max_lag_ms": 60 * 1000,  # Max difference between now and minimal pass instant in milliseconds
    "ignore_unavailable_instances": True,  # False if failing to get "controller_info" should result in check failure.
}
"""

BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME = "banned_queue_agent_instance"
DEFAULT_MAX_LAG_MS = 60 * 1000


ControllerPass = namedtuple("ControllerPass", ["path", "pass_instant"])


class InactiveObjects:
    FIELDS = [
        "leading_queues",
        "following_queues",
        "leading_consumers",
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


def run_check(secrets, yt_client: YtClient, logger: Logger, options, states):
    client = yt_client

    max_lag_ms = options.get("max_lag_ms", DEFAULT_MAX_LAG_MS)
    ignore_unavailable_instances = options.get("ignore_unavailable_instances", True)

    queue_agent_instances = client.list("//sys/queue_agents/instances", attributes=[BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME])

    now = datetime.datetime.now(pytz.UTC)

    failed_instances = []
    # Instances for which at least 1 controller is running regularly.
    running_instances = []

    for yson_instance in queue_agent_instances:
        instance = str(yson_instance)
        if yson_instance.has_attributes() and BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME in yson_instance.attributes:
            attribute_value = yson_instance.attributes[BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME]
            # NB(apachee): Check type to match the behavior of queue agent sharding manager (it ignores anything except bool).
            if isinstance(attribute_value, yson.YsonBoolean) and attribute_value:
                logger.info(f"Skipping check for instance {instance}, since it is banned by {BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME!r} attribute")

        try:
            controller_info = ControllerInfo(client.get(f"//sys/queue_agents/instances/{instance}/orchid/queue_agent/controller_info"))
        except Exception as ex:
            if ignore_unavailable_instances:
                # NB(apachee): These failures are handled by queue_agent_alerts check, so just skip them.
                logger.exception(ex)
                continue
            else:
                raise

        logger.info(f"Controller info on instance {instance}: {controller_info}")

        failed = False

        for field in InactiveObjects.FIELDS:
            if len(getattr(controller_info.inactive_objects, field)) == 0:
                logger.warning(f"No passes present on instance {instance} for {field} controller passes")
                continue
            running_instances.append(instance)
            least_active_object = getattr(controller_info.inactive_objects, field)[0]
            least_active_object_ts = least_active_object[1]
            if now - least_active_object_ts > datetime.timedelta(milliseconds=max_lag_ms):
                failed = True
                logger.error(f"Max lag milliseconds ({max_lag_ms}) exceeded for {field} controller passes on instance {instance} for pass {least_active_object}")

        if failed:
            failed_instances.append(instance)

    if len(failed_instances) > 0:
        error_msg = f"Check failed on instances {failed_instances}"
        logger.error(error_msg)
        return states.UNAVAILABLE_STATE, error_msg

    if len(running_instances) == 0:
        error_msg = "No controllers are running"
        logger.error(error_msg)
        return states.PARTIALLY_AVAILABLE_STATE, error_msg

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
