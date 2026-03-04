from __future__ import annotations

from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Optional, Sequence, cast
from typing_extensions import override

import yt.wrapper as yt
import yt.yson as yson

from .app import SequoiaTool
from .utils import MessageBuilder, compare_values, log_dry_run

import logging
logger = logging.getLogger(__name__)


class ValidationFailed(Exception):
    """Raised on action validation failure."""
    pass


class Action(ABC):
    """Abstract class for changes to cluster state.

    Actions are the building blocks for creating a state migration pipeline.
    Action must be idempotent, i.e. safe to be executed multiple times.
    """

    @abstractmethod
    def execute(self, app: SequoiaTool) -> None:
        """Perform the mutation."""
        pass

    @abstractmethod
    def dry_run(self, app: SequoiaTool) -> None:
        """Simulate changes WITHOUT modifying the real state."""
        pass

    @abstractmethod
    def describe(self) -> str:
        """Return the human-readable summary of the changes."""
        pass

    def rollback(self, app: SequoiaTool) -> None:
        """(Optional) Revert the effect of execute()."""
        raise NotImplementedError("Rollback is not supported")

    def validate_prerequisites(self, app: SequoiaTool) -> None:
        """Validate feasibility of the action."""
        pass

    def validate_state(self, app: SequoiaTool) -> None:
        """Verify state for existing cluster."""
        pass

    @staticmethod
    def _log_validation_failure(error_prefix: str = ""):
        """Decorator to add logging to action validation methods."""
        def decorator(func):
            @wraps(func)
            def wrapper(self: Action, app: SequoiaTool) -> bool:
                try:
                    func(self, app)
                    return True
                except ValidationFailed as exc:
                    logging.error(f'{error_prefix}"{self.describe()}": {exc}')
                    return False
            return wrapper
        return decorator

    @_log_validation_failure("Prerequisites are not met for ")
    def check_prerequisites(self, app: SequoiaTool) -> bool:
        """`validate_prerequisites` with logging failure instead of raising."""
        self.validate_prerequisites(app)
        return True

    @_log_validation_failure("Unexpected state for ")
    def check_state(self, app: SequoiaTool) -> bool:
        """`validate_state` with logging failure instead of raising."""
        self.validate_state(app)
        return True


class ActionPlan():
    """Action sequence executor."""

    def __init__(self, actions: Sequence[Action], name: str) -> None:
        """Create action plan from an action sequence."""
        self._actions = actions
        self._name = name

    @property
    def name(self) -> str:
        """Action plan name for logging."""
        return self._name

    def execute_all(self, app: SequoiaTool) -> None:
        logging.info("Executing actions")

        for index, action in enumerate(self._actions):
            try:
                logging.info(f"+++ {action.describe()}")
                action.execute(app)

            except Exception as exc:
                logging.error(f'"{action.describe()}" failed: {exc}')
                if app.interaction.confirm("Roll back changes?"):
                    for action in reversed(self._actions[:index]):
                        try:
                            logging.info(f"--- {action.describe()}")
                            action.rollback(app)
                        except Exception as roll_exc:
                            logging.warning(
                                "Could not roll back"
                                f'"{action.describe()}": {roll_exc}')
                raise

        logging.info("Successfully executed all actions")

    def dry_run(self, app: SequoiaTool) -> None:
        logging.info("Started dry-run")

        for action in self._actions:
            logging.info(f"+++ {action.describe()}")
            action.dry_run(app)

        logging.info("Completed dry-run")

    def check_prerequisites(self, app: SequoiaTool) -> bool:
        """Verify action prerequisites are met.

        Inter-action dependencies are not supported.
        """
        logging.info("Started prerequisites check")

        # Square brackets provide check evaluation for all actions.
        if all([action.check_prerequisites(app) for action in self._actions]):
            logging.info("Completed prerequisites check")
            return True
        else:
            logging.info("Failed prerequisites check")
            return False

    def check_state(self, app: SequoiaTool) -> bool:
        """Verify cluster state match expectations.

        Inter-action dependencies are not supported.
        """
        logging.info("Started cluster state check")

        # See `check_prerequisites`.
        if all([action.check_state(app) for action in self._actions]):
            logging.info("Completed cluster state check")
            return True
        else:
            logging.info("Failed cluster state check")
            return False


class SetAttributeAction(Action):
    """Set YT object attribute."""

    # Use this to validate that value is not set.
    NON_EXISTING_KEY = "__not_existing"

    def __init__(
        self,
        path: str,
        value: Any,
        old_value: Any = None,
        remote: bool = False,
    ) -> None:
        if not path.count("@"):
            raise RuntimeError("Path must lead to an attribute")

        if path.endswith("@"):
            raise RuntimeError('Path must not end with "@"')

        self._path = path
        self._value = value
        self._old_value: Any = old_value
        self._remote = remote

    def yt_client(self, app: SequoiaTool) -> yt.YtClient:
        return app.remote_client if self._remote else app.ground_client

    def _try_get_current_value(self, app: SequoiaTool) -> Any:
        try:
            return self.yt_client(app).get(self._path)
        except yt.errors.YtResolveError:
            return None

    def _validate_current_value(self, app: SequoiaTool, expected: Any) -> None:
        current = self._try_get_current_value(app)
        if mismatches := compare_values(current, expected):
            raise ValidationFailed(f"Mismatches: [{"; ".join(mismatches)}]")

    @override
    def execute(self, app: SequoiaTool) -> None:
        self._old_value = self._try_get_current_value(app)
        if compare_values(self._old_value, self._value):
            app.ground_client.set(self._path, self._value)

    @override
    def rollback(self, app: SequoiaTool) -> None:
        if self._old_value is not None:
            self.yt_client(app).set(self._path, self._old_value)
        else:
            self.yt_client(app).remove(self._path)

    @override
    def describe(self) -> str:
        return f"Set value at {self._path}"

    @override
    def validate_prerequisites(self, app: SequoiaTool) -> None:
        dirname = yt.ypath_dirname(self._path)
        if not self.yt_client(app).exists(dirname):
            raise ValidationFailed(f"Parent path {dirname} doesn't exist")

        if self._old_value is not None:
            expected: Any = (
                self._old_value
                if self._old_value != self.NON_EXISTING_KEY
                else None)
            self._validate_current_value(app, expected)

    @override
    def validate_state(self, app: SequoiaTool) -> None:
        self._validate_current_value(app, self._value)

    @override
    def dry_run(self, app: SequoiaTool) -> None:
        old_value = self._try_get_current_value(app)
        if compare_values(old_value, self._value):
            log_dry_run(
                MessageBuilder(f"Would set {self._path}")
                .with_field("value", self._value)
                .with_field("old_value", old_value)
                .build(),
                logger)
        else:
            log_dry_run(
                MessageBuilder(f"Value is already set at {self._path}")
                .with_field("value", self._value)
                .build(),
                logger)


class CreateObjectActionBase(Action):
    def __init__(
        self,
        name: str,
        type: str,
        root_dir: str,
        attributes: Optional[dict[str, Any]] = None,
    ) -> None:
        self._name = name
        self._attributes = attributes or {}
        self._type = type
        self._root_dir = root_dir
        self._path = yt.ypath_join(self._root_dir, self._name)
        self._created = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> str:
        return self._path

    @property
    def parent_path(self) -> str:
        return self._root_dir

    def _do_create(self, app: SequoiaTool) -> None:
        raise NotImplementedError("Subclass must override this method")

    @override
    def execute(self, app: SequoiaTool) -> None:
        if not app.ground_client.exists(self._path):
            self._do_create(app)
            self._created = True
        else:
            self.check_state(app)

    @override
    def rollback(self, app: SequoiaTool) -> None:
        if self._created:
            app.ground_client.remove(self._path)

    @override
    def describe(self) -> str:
        return f'Create {self._type} "{self._name}"'

    @override
    def validate_state(self, app: SequoiaTool) -> None:
        if not app.ground_client.exists(self._path):
            raise ValidationFailed(f"{self._path} doesn't exist")

        attributes = cast(yson.YsonMap, app.ground_client.get(
            path=self._path + "/@",
            attributes=list(self._attributes.keys())))

        if mismatches := compare_values(attributes, self._attributes,
                                        patch_mode=True):
            # TODO(danilalexeev): Raise on incompatable schema.
            logging.warning(f"Attribute mismatches: [{"; ".join(mismatches)}]")

    @override
    def dry_run(self, app: SequoiaTool) -> None:
        if not app.ground_client.exists(self._path):
            log_dry_run(
                MessageBuilder(f'Would create {self._type} "{self._name}"')
                .with_field("path", self._path)
                .with_fields(self._attributes)
                .build(),
                logger)
        elif self.check_state(app):
            log_dry_run(
                f'{self._type.title()} "{self._name}" already exists.',
                logger)


class CreateObjectAction(CreateObjectActionBase):
    """Create YT object."""

    @override
    def _do_create(self, app: SequoiaTool) -> None:
        self._attributes.update({"name": self._name})
        app.ground_client.create(
            self._type,
            attributes=self._attributes)


class CreateNodeAction(CreateObjectActionBase):
    """Create YT Cypress node."""

    @override
    def _do_create(self, app: SequoiaTool) -> None:
        app.ground_client.create(
            self._type,
            self._path,
            attributes=self._attributes)


class CreateAccountAction(CreateObjectAction):
    def __init__(
        self,
        name: str,
        attributes: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            name,
            type="account",
            root_dir="//sys/accounts",
            attributes=attributes)


class CreateTabletCellBundleAction(CreateObjectAction):
    def __init__(
        self,
        name: str,
        attributes: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            name,
            type="tablet_cell_bundle",
            root_dir="//sys/tablet_cell_bundles",
            attributes=attributes)


class CreateTableAction(CreateNodeAction):
    """Create YT dynamic table."""

    def __init__(
        self,
        name: str,
        parent_path: str,
        schema: list[dict[str, Any]],
        attributes: Optional[dict[str, Any]] = None,
    ) -> None:
        extended_attributes = {
            **(attributes or {}),
            "dynamic": True,
            "schema": schema,
        }
        super().__init__(
            name,
            type="table",
            root_dir=parent_path,
            attributes=extended_attributes)

    @override
    def validate_prerequisites(self, app: SequoiaTool) -> None:
        if not app.ground_client.exists(self.parent_path):
            raise ValidationFailed(
                f"Parent path {self.parent_path} doesn't exist")


class CreateTabletCellsAction(Action):
    """Create specified number of YT tablet cells."""

    def __init__(self, tablet_cell_bundle: str, cell_count: int) -> None:
        self._bundle_name = tablet_cell_bundle
        self._count = cell_count

    def _get_current_count(self, app: SequoiaTool) -> int:
        path = yt.ypath_join("//sys/tablet_cell_bundles",
                             self._bundle_name,
                             "@tablet_cell_count")
        return cast(int, app.ground_client.get(path))

    def _get_remaining_count(self, app: SequoiaTool) -> int:
        return max(0, self._count - self._get_current_count(app))

    @override
    def execute(self, app: SequoiaTool) -> None:
        ids = []
        try:
            for _ in range(self._get_remaining_count(app)):
                id = app.ground_client.create(
                    type="tablet_cell",
                    attributes={"tablet_cell_bundle": self._bundle_name})
                ids.append(id)
        finally:
            if ids:
                logging.info(f"Created tablet cells: {ids}")

    @override
    def describe(self) -> str:
        return (f'Create tablet cells in the "{self._bundle_name}" bundle')

    @override
    def validate_state(self, app: SequoiaTool) -> None:
        current = self._get_current_count(app)
        if current != self._count:
            raise ValidationFailed(
                f"Tablet cell count mismatch: {current} != {self._count}")

    @override
    def dry_run(self, app: SequoiaTool) -> None:
        remaining = self._get_remaining_count(app)
        if remaining > 0:
            log_dry_run(f"Would create {remaining} tablet cells in the"
                        f'"{self._bundle_name}" bundle', logger)
        elif self.check_state(app):
            log_dry_run("Enough tablet cells exist in the "
                        f'"{self._bundle_name}" bundle', logger)


class MountTabletAction(Action):
    def __init__(
        self,
        path: str,
        unmount: bool,
        **kwargs,
    ) -> None:
        self._path = path
        self._unmount = unmount
        self._kwargs = kwargs
        self._executed = False

    @override
    def execute(self, app: SequoiaTool) -> None:
        op = (app.ground_client.unmount_table if self._unmount
              else app.ground_client.mount_table)
        op(self._path, **self._kwargs)
        self._executed = True

    @override
    def rollback(self, app: SequoiaTool) -> None:
        if not self._executed:
            return
        op = (app.ground_client.mount_table if self._unmount
              else app.ground_client.unmount_table)
        op(self._path)

    @override
    def describe(self) -> str:
        return f"{"Unmount" if self._unmount else "Mount"} table {self._path}"

    @override
    def validate_prerequisites(self, app: SequoiaTool) -> None:
        if not app.ground_client.exists(self._path):
            raise ValidationFailed(f"Table {self._path} doesn't exist")

    @override
    def validate_state(self, app: SequoiaTool) -> None:
        state_path = f"{self._path}/@tablet_state"
        expected_state = "unmounted" if self._unmount else "mounted"
        if app.ground_client.get(state_path) != expected_state:
            raise ValidationFailed(
                f"Table {self._path} is not {expected_state}")

    @override
    def dry_run(self, app: SequoiaTool) -> None:
        log_dry_run(
            MessageBuilder(
                f"Would {"unmount" if self._unmount else "mount"} "
                f"table {self._path}")
            .with_fields(self._kwargs)
            .build(),
            logger)


def run_action_plan(plan: ActionPlan, app: SequoiaTool) -> None:
    """Validate and execute an action plan."""
    logging.info(f'Started action plan "{plan.name}"')

    if not plan.check_prerequisites(app):
        if not app.interaction.confirm("(Unsafe) Ignore prerequisites check?"):
            raise ValidationFailed("Failed prerequisites check")

    try:
        if app.options.dry_run:
            plan.dry_run(app)
        else:
            plan.execute_all(app)
    except Exception as exc:
        logging.error(f'Action plan "{plan.name}" failed: {exc}')
        raise

    logging.info(f'Action plan "{plan.name}" completed')
