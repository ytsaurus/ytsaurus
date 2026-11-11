"""
Check whether queue agent object controllers are running by comparing minimal pass instant
with current time.

If "queue_agent_stage_clusters" is specified, objects of the current cluster are collected from all
listed queue agent stages via "controller_info/per_cluster". Otherwise, all objects are collected
from the queue agent stage of the current cluster.

Options explanation:
{
    "max_lag_ms": 60 * 1000,  # Max difference between now and minimal pass instant in milliseconds
    "ignore_unreachable_instances": True,  # False if failing to get "controller_info" should result in check failure.
    "queue_agent_stage_clusters": {"hume": "hume"},  # Map from queue agent stage cluster to its proxy.
}
"""

from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.queue_agent_helpers import (
    BANNED_ATTRIBUTE_NAME,
    MAINTENANCE_ATTRIBUTE_NAME,
    is_attribute_true,
)
from yt.wrapper import YtClient

from collections import namedtuple, defaultdict
from copy import deepcopy
from dacite import from_dict
from dataclasses import dataclass
import datetime
from logging import Logger
import pytz
from typing import Optional


DEFAULT_MAX_LAG_MS = 60 * 1000


@dataclass
class QueueAgentControllerLivenessOptions:
    # Passed automatically by Odin.
    cluster_name: str

    max_lag_ms: int = 60 * 1000
    ignore_unreachable_instances: bool = True
    queue_agent_stage_clusters: Optional[dict[str, str]] = None


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

        self.has_own_stage = False
        self.running_instances = []
        self.has_failed_controller_passes = False
        self.failed_controller_passes: dict[str, dict[str, list[ControllerPass]]] = defaultdict(lambda: defaultdict(list))
        self.unreachable_instances = []
        self.last_unreachable_exception = None

    def check(self):
        now = datetime.datetime.now(pytz.UTC)
        self.logger.info(f"Running check (Now: {now}, Options: {self.options})")

        if self.options.queue_agent_stage_clusters is None:
            self.check_queue_agent_stage(now, self.options.cluster_name, self.client, object_cluster=None)
        else:
            for queue_agent_stage_cluster, queue_agent_stage_proxy in self.options.queue_agent_stage_clusters.items():
                config = deepcopy(self.client.config)
                config["proxy"]["retries"] = {"count": 1, "enable": False}
                queue_agent_stage_client = YtClient(proxy=queue_agent_stage_proxy, config=config)
                self.check_queue_agent_stage(now, queue_agent_stage_cluster, queue_agent_stage_client, object_cluster=self.options.cluster_name)

        if self.has_failed_controller_passes:
            # NB(apachee): Default dict has a terrible string representation.
            dict_failed_controller_passes = {k: dict(v) for k, v in self.failed_controller_passes.items()}

            error_msg = f"Check failed due to these controller passes: {dict_failed_controller_passes}"
            self.logger.error(error_msg)
            return self.states.UNAVAILABLE_STATE, error_msg

        if len(self.unreachable_instances) > 0:
            error_msg = f"Some instances are unreachable: {self.unreachable_instances}"
            self.logger.error(error_msg)
            if self.last_unreachable_exception is not None:
                self.logger.exception(self.last_unreachable_exception)
            return self.states.PARTIALLY_AVAILABLE_STATE, error_msg

        if self.has_own_stage and len(self.running_instances) == 0:
            error_msg = "No controllers are running"
            self.logger.error(error_msg)
            return self.states.PARTIALLY_AVAILABLE_STATE, error_msg

        return self.states.FULLY_AVAILABLE_STATE

    def check_queue_agent_stage(self, now: datetime.datetime, queue_agent_stage_cluster: str, queue_agent_stage_client: YtClient, object_cluster: Optional[str]):
        # Unavailability of a queue agent stage should only cause this check to fail
        # when running on the host cluster of this stage.
        is_own_stage = queue_agent_stage_cluster == self.options.cluster_name
        self.has_own_stage |= is_own_stage

        try:
            queue_agent_instances = queue_agent_stage_client.list("//sys/queue_agents/instances", attributes=[BANNED_ATTRIBUTE_NAME, MAINTENANCE_ATTRIBUTE_NAME])
        except Exception:
            if is_own_stage:
                raise
            self.logger.warning(f"Could not list queue agent instances on cluster {queue_agent_stage_cluster}", exc_info=True)
            return

        skipped_instances = defaultdict(list)
        for yson_instance in queue_agent_instances:
            instance = str(yson_instance)
            if is_attribute_true(yson_instance, BANNED_ATTRIBUTE_NAME):
                skipped_instances["banned"].append(instance)
                continue
            if is_attribute_true(yson_instance, MAINTENANCE_ATTRIBUTE_NAME):
                skipped_instances["maintenance"].append(instance)
                continue

            try:
                raw_controller_info = queue_agent_stage_client.get(f"//sys/queue_agents/instances/{instance}/orchid/queue_agent/controller_info")
                controller_info = ControllerInfo(raw_controller_info)
                if object_cluster is None:
                    relevant_controller_info = controller_info
                else:
                    raw_cluster_controller_info = raw_controller_info["per_cluster"].get(object_cluster)
                    relevant_controller_info = ControllerInfo(raw_cluster_controller_info) if raw_cluster_controller_info is not None else None
            except Exception as ex:
                if is_own_stage and not self.options.ignore_unreachable_instances:
                    self.last_unreachable_exception = ex
                    self.unreachable_instances.append(instance)
                else:
                    self.logger.warning(f"Could not get controller info from instance {instance} on cluster {queue_agent_stage_cluster}", exc_info=True)
                continue

            if is_own_stage and any(getattr(controller_info.inactive_objects, field) for field in InactiveObjects.FIELDS):
                self.running_instances.append(instance)

            if relevant_controller_info is not None:
                self.handle_controller_info(now, instance, relevant_controller_info)

        if skipped_instances:
            self.logger.info(f"Skipped instances on cluster {queue_agent_stage_cluster}: {dict(skipped_instances)}")

    def handle_controller_info(self, now: datetime.datetime, instance: str, controller_info: ControllerInfo):
        for field in InactiveObjects.FIELDS:
            passes = getattr(controller_info.inactive_objects, field)
            if not passes:
                continue
            least_active_object = passes[0]
            lag_ms = (now - least_active_object.pass_instant).total_seconds() * 1000
            if lag_ms > self.options.max_lag_ms:
                self.has_failed_controller_passes = True
                self.failed_controller_passes[instance][field].append(least_active_object)


def run_check(secrets, yt_client: YtClient, logger: Logger, options, states):
    typed_options = from_dict(QueueAgentControllerLivenessOptions, options)

    checker = QueueAgentControllerLivenessChecker(client=yt_client, logger=logger, options=typed_options, states=states)
    return checker.check()


if __name__ == "__main__":
    main(run_check)
