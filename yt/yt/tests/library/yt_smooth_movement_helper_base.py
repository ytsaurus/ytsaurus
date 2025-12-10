import random
import abc
import contextlib

from yt.common import YtError


class CommandProvider(abc.ABC):
    @abc.abstractmethod
    def get(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def set(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def list(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def create(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def wait(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def print_debug(self, *args, **kwargs):
        pass

    _initialized = False

    _command_mapping = {
        "get": "get",
        "set": "set",
        "create": "create",
        "exists": "exists",
        "remove": "remove",
        "remount_table": "remount_table",
    }

    def __new__(cls, *args, **kwargs):
        if not cls._initialized:
            cls._initialized = True
            cls._init_commands()
            abc.update_abstractmethods(cls)

        return super().__new__(cls)

    @classmethod
    def _init_commands(cls):
        for lhs, rhs in (CommandProvider._command_mapping | cls._command_mapping).items():
            setattr(cls, lhs, cls._make_command(rhs))


class SmoothMovementHelperBase(CommandProvider):
    def __init__(self, tablet_or_table, cell_id=None):
        self.stop_at_stage = None
        if tablet_or_table.startswith("//"):
            self.tablet_id = self.get(f"{tablet_or_table}/@tablets/0/tablet_id")
            self.table_path = tablet_or_table
        else:
            self.tablet_id = tablet_or_table
            self.table_path = self.get(f"#{self.tablet_id}/@table_path")

        self.source_cell_id = self.get(f"#{self.tablet_id}/@cell_id")

        if cell_id is None:
            self.target_cell_id = random.choice(
                [x for x in self.list("//sys/tablet_cells") if x != self.source_cell_id])
        else:
            self.target_cell_id = cell_id

        self.source_orchid = f"#{self.source_cell_id}/orchid/tablets/{self.tablet_id}"
        self.target_orchid = f"#{self.target_cell_id}/orchid/tablets/{self.tablet_id}"

        if not self.exists(f"{self.table_path}/@mount_config/testing"):
            self.set(f"{self.table_path}/@mount_config/testing", {})

        self.action_id = None

    def start(self, stage=None):
        assert self.action_id is None, "Cannot use the same helper twice"
        if stage is not None:
            self._set_breakpoint(stage)

        self._run_action()

        if stage is not None:
            self._wait_for_stage_at_any_servant(stage)

        return self.action_id

    def start_forwarding_mutations(self):
        self.start("target_activated")

    def advance_to_stage(self, stage):
        self._set_breakpoint(stage)
        self._wait_for_stage_at_any_servant(stage)

    def finish(self):
        assert self.action_id is not None, "Smooth movement is not started"
        self._set_breakpoint(None)
        self.wait_for_action()

    def get_action_state(self):
        state = self.get(f"#{self.action_id}/@state")
        self.print_debug(f"State of action {self.action_id} is {state}")
        return state

    def wait_for_action(self, ignore_errors=False):
        def _check():
            state = self.get_action_state()
            if state == "completed":
                return True
            elif state == "failed":
                if ignore_errors:
                    return True
                raise self.get_action_error()
            return False

        self.wait(_check)

    def get_action_error(self):
        try:
            error = self.get(f"#{self.action_id}/@error")
            return YtError.from_dict(error)
        except YtError as e:
            if e.is_resolve_error():
                return None
            raise

    def get_source_stage(self):
        try:
            return self.get(self.source_orchid + "/smooth_movement/stage")
        except YtError as e:
            if e.is_resolve_error():
                return None

    def get_target_stage(self):
        try:
            return self.get(self.target_orchid + "/smooth_movement/stage")
        except YtError as e:
            if e.is_resolve_error():
                return None

    def forwarding_context(self):
        @contextlib.contextmanager
        def wrapper():
            self.start_forwarding_mutations()
            yield
            self.finish()

        return wrapper()

    def _run_action(self):
        self.action_id = self.create(
            "tablet_action",
            None,
            attributes={
                "kind": "smooth_move",
                "tablet_ids": [self.tablet_id],
                "cell_ids": [self.target_cell_id],
                "expiration_timeout": 60000,
            })
        self.print_debug(f"Action started, id = {self.action_id}")

    def _set_breakpoint(self, stage):
        if stage is not None:
            self.set(f"{self.table_path}/@mount_config/testing/pause_at_smooth_movement_stage", stage)
        else:
            try:
                self.remove(f"{self.table_path}/@mount_config/testing/pause_at_smooth_movement_stage")
            except YtError as e:
                if not e.is_resolve_error():
                    raise
        self.remount_table(self.table_path)

    def _wait_for_stage_at_any_servant(self, stage):
        def _check():
            if self.get_source_stage() == stage:
                return True
            if self.get_target_stage() == stage:
                return True
            return False
        self.wait(_check)
        self.print_debug(f"Finished waiting for stage {stage}")
