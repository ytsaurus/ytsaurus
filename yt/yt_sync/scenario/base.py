from abc import ABC
from abc import abstractmethod
import logging
import time
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

LOG = logging.getLogger("yt_sync")


class ScenarioBase(ABC):
    """Name of scenario, should be overriden in subclasses."""

    SCENARIO_NAME: ClassVar[str] = "_base_"
    SCENARIO_DESCRIPTION: ClassVar[str] = "_base_"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        self.has_diff: bool = False
        self.desired: YtDatabase = desired
        self.actual: YtDatabase = actual
        self.settings: Settings = settings
        self.yt_client_factory: YtClientFactory = yt_client_factory
        self.use_clients_cache = False
        self._clients_cache: dict[str, YtClientProxy] = {}
        self._batch_clients_cache: dict[str, YtClientProxy] = {}

    def setup(self, **kwargs):
        """Custom params for scenario here. Passed from YtSync.sync() method."""
        pass

    def pre_action(self):
        """Run before all cluster scripts."""
        pass

    @abstractmethod
    def generate_actions(self) -> list[ActionBatch]:
        """Generate sequence of ActionBatches to be executed via run() method."""
        raise NotImplementedError

    def post_action(self):
        """Run after all cluster scripts."""
        pass

    def get_or_create_batch_client(self, cluster_name: str):
        if not self.use_clients_cache:
            yt_client = self.yt_client_factory(cluster_name)
            return yt_client.create_batch_client()

        batch_client = self._batch_clients_cache.get(cluster_name, None)
        if batch_client is not None:
            return batch_client
        yt_client = self._clients_cache.get(cluster_name, None)
        if yt_client is None:
            yt_client = self.yt_client_factory(cluster_name)
            self._clients_cache[cluster_name] = yt_client
        batch_client = yt_client.create_batch_client()
        self._batch_clients_cache[cluster_name] = batch_client
        return batch_client

    def run(self) -> bool:
        LOG.info("Before actions generate")
        self.pre_action()
        LOG.info("Generate actions")
        actions = self.generate_actions()
        self.has_diff |= len(actions) > 0
        LOG.info("Execute actions")

        for batch in actions:
            batch_client = self.get_or_create_batch_client(batch.cluster_name)

            scheduled_actions = batch.actions
            iteration = 0
            while len(scheduled_actions) > 0:
                next_scheduled = list()
                for action in scheduled_actions:
                    action.dry_run = self.yt_client_factory.is_dry_run()
                    LOG.debug("Run action %s, iteration #%s", action.__class__.__name__, iteration)
                    if action.schedule_next(batch_client):
                        next_scheduled.append(action)
                batch_client.commit_batch()
                for action in scheduled_actions:
                    action.process()
                scheduled_actions = next_scheduled
                iteration += 1
                if batch.max_iterations and scheduled_actions:
                    assert iteration < batch.max_iterations, (
                        f"Max iterations reached: current={iteration}, max={batch.max_iterations}, "
                        + f"one of is {scheduled_actions[0].__class__.__name__}"
                    )
                if batch.iteration_delay:
                    LOG.debug("Sleep for iteration_delay=%f seconds", batch.iteration_delay)
                    time.sleep(batch.iteration_delay)
        LOG.info("After actions execute")
        self.post_action()
        return self.has_diff
