"""Companion-hosted resources: the user-facing base class and the process-wide store.

A companion resource is declared in the pipeline spec as a resource of class
``NYT::NFlow::NCompanion::TCompanionResource`` whose ``parameters`` name the
companion-side class under the ``companion_resource_class`` key. The worker
drives the hosted instance through ResourceExecute commands (init/unload);
computations reach it via ``ctx.get_resource(alias)`` where the alias comes
from the computation's ``required_resource_ids`` entry.
"""

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import yt.yson as yson

log = logging.getLogger(__name__)

# Key inside the resource spec's parameters naming the companion-side class;
# mirrors TCompanionResourceParameters on the worker side.
COMPANION_RESOURCE_CLASS_KEY = "companion_resource_class"

# In-band command statuses; string forms of EResourceExecuteStatus.
RES_OK = "RES_OK"
RES_ERROR = "RES_ERROR"
RES_RESOURCE_NOT_FOUND = "RES_RESOURCE_NOT_FOUND"
RES_RESOURCE_NOT_INITIALIZED = "RES_RESOURCE_NOT_INITIALIZED"
RES_UNSUPPORTED = "RES_UNSUPPORTED"
RES_STALE_RESOURCE_INCARNATION = "RES_STALE_RESOURCE_INCARNATION"

# EResourceCommand wire values.
COMMAND_INIT = 0
COMMAND_UNLOAD = 1


class ResourceContext:
    """Everything a resource sees at load and reconfigure time."""

    def __init__(
        self,
        resource_id: str,
        parameters: Dict[str, Any],
        dynamic_parameters: Dict[str, Any],
        dependencies: Dict[str, "FlowResource"],
    ):
        self.resource_id = resource_id
        self.parameters = parameters
        self.dynamic_parameters = dynamic_parameters
        self.dependencies = dependencies


class FlowResource:
    """Base class for companion-hosted resources.

    The worker drives the lifecycle; a resource is shared by every job that
    requires it, so it must tolerate concurrent use.
    """

    def load(self, context: ResourceContext) -> None:
        """Builds the instance from the spec parameters and the dependencies."""

    def reconfigure(self, context: ResourceContext) -> None:
        """Applies an updated dynamic spec to the live instance.

        The default keeps the instance as is; read the refreshed values from
        the context passed here.
        """

    def unload(self) -> None:
        """Releases whatever the instance holds outside the process.

        Runs once nothing holds the instance any more, and on an instance whose
        ``load`` raised partway, so it must tolerate partial initialization. An
        ``Exception`` it raises is logged and swallowed; anything else
        propagates.
        """


def resource_class_name(resource_class: type) -> str:
    """Spec-facing name of a resource class, referenced by the pipeline spec
    under the ``companion_resource_class`` parameter: the class's own
    ``flow_resource_class`` attribute if set (not inherited — each class names
    itself), else its fully-qualified ``module.qualname``.
    """
    name = resource_class.__dict__.get("flow_resource_class")
    if name:
        return name
    return f"{resource_class.__module__}.{resource_class.__qualname__}"


@dataclass(frozen=True)
class CompanionResourceInstanceReference:
    """Exact resource instance required by a job or another companion resource."""

    resource_id: str
    incarnation_id: str
    configuration_generation: int
    alias: Optional[str] = None


@dataclass
class CommandOutcome:
    """Outcome of one resource command; user-code failures travel in-band."""

    status: str = RES_OK
    error_message: str = ""


def _to_str(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _map_get(mapping: Dict[Any, Any], key: str, default: Any = None) -> Any:
    """Fetch by key from a parsed YSON map whose keys may be str or bytes."""
    if key in mapping:
        return mapping[key]
    return mapping.get(key.encode("utf-8"), default)


def _parse_reference(node: Dict[str, Any]) -> CompanionResourceInstanceReference:
    alias = _map_get(node, "alias")
    return CompanionResourceInstanceReference(
        resource_id=_to_str(_map_get(node, "resource_id")),
        incarnation_id=_to_str(_map_get(node, "incarnation_id")),
        configuration_generation=int(_map_get(node, "configuration_generation")),
        alias=_to_str(alias) if alias is not None else None,
    )


@dataclass
class _InitArg:
    """Parsed argument of the "init" command; field names mirror TInitResourceCommandArg."""

    spec: Dict[str, Any]
    dynamic_spec: Dict[str, Any]
    incarnation_id: str
    incarnation_generation: int
    configuration_generation: int
    dependencies: List[CompanionResourceInstanceReference]
    resource_revision: Any


def _parse_argument(argument: Optional[bytes]) -> Any:
    if not argument:
        raise ValueError("Resource command argument is required")
    parsed = yson.loads(argument)
    if not isinstance(parsed, dict):
        raise ValueError("Resource command argument must be a YSON map")
    return parsed


def _parse_incarnation_id(parsed: Dict[Any, Any]) -> str:
    """Rejects a missing incarnation id rather than letting it become the
    string "None": the store would publish under an id no reference can ever
    match, and every batch would be rejected forever with no error text.
    """
    incarnation_id = _map_get(parsed, "incarnation_id")
    if incarnation_id is None:
        raise ValueError("Argument does not carry an incarnation id")
    incarnation_id = _to_str(incarnation_id)
    if not incarnation_id:
        raise ValueError("Argument carries an empty incarnation id")
    return incarnation_id


def _parse_init_arg(argument: Optional[bytes]) -> _InitArg:
    parsed = _parse_argument(argument)
    return _InitArg(
        spec=_map_get(parsed, "spec"),
        dynamic_spec=_map_get(parsed, "dynamic_spec"),
        incarnation_id=_parse_incarnation_id(parsed),
        incarnation_generation=int(_map_get(parsed, "incarnation_generation", 0)),
        configuration_generation=int(_map_get(parsed, "configuration_generation", 0)),
        dependencies=[_parse_reference(node) for node in _map_get(parsed, "dependencies", [])],
        resource_revision=_map_get(parsed, "resource_revision"),
    )


def _extract_companion_resource_class(spec: Dict[str, Any]) -> str:
    parameters = _map_get(spec, "parameters") if isinstance(spec, dict) else None
    class_name = _map_get(parameters, COMPANION_RESOURCE_CLASS_KEY) if isinstance(parameters, dict) else None
    if class_name is None:
        raise ValueError(
            f"Resource spec parameters do not name a companion resource class "
            f"under key {COMPANION_RESOURCE_CLASS_KEY!r}"
        )
    return _to_str(class_name)


def _extract_parameters(spec: Any) -> Dict[str, Any]:
    if isinstance(spec, dict):
        parameters = _map_get(spec, "parameters")
        if isinstance(parameters, dict):
            return parameters
    return {}


@dataclass
class _AppliedSpecs:
    """Canonical YSON of the specs the current instance was initialized from.

    Both sides of every comparison are produced by the same serialization of
    the parsed argument, and within one incarnation the worker serializes the
    same spec objects, so byte equality is exact.
    """

    spec: bytes = b""
    dynamic_spec: bytes = b""
    resource_revision: bytes = b""


def _canonical_specs(arg: _InitArg) -> _AppliedSpecs:
    return _AppliedSpecs(
        spec=yson.dumps(arg.spec),
        dynamic_spec=yson.dumps(arg.dynamic_spec),
        resource_revision=yson.dumps(arg.resource_revision) if arg.resource_revision is not None else b"",
    )


_STATE_REGISTERED = "registered"
_STATE_INITIALIZED = "initialized"
_STATE_RECONFIGURING = "reconfiguring"
_STATE_RECONFIGURE_FAILED = "reconfigure_failed"

_STORE_SHUT_DOWN = "Companion resource store is shut down"


def _error_message(error: Exception) -> str:
    """Never-empty message for an in-band error status.

    ``str()`` of a bare ``ValueError()`` or a failed ``assert`` is empty, and a
    status without a message reaches the worker as a resource failure with no
    cause at all.
    """
    return str(error) or repr(error)


def _run_unload_hook(resource: FlowResource, resource_id: str) -> None:
    """Best-effort unload hook on an instance nobody holds any more."""
    try:
        resource.unload()
    except Exception:
        log.warning("Companion resource unload hook failed: (ResourceId: %s)", resource_id, exc_info=True)


class _ResourceHandle:
    """Reference-counted holder of one hosted instance.

    Keeps the instance usable until its last holder lets go, then runs the
    unload hook exactly once.
    """

    def __init__(
        self,
        resource: FlowResource,
        resource_id: str,
        dependency_handles: Optional[List["_ResourceHandle"]] = None,
    ):
        self.resource = resource
        self._resource_id = resource_id
        # Held for this instance's lifetime, so a dependency outlives everything
        # built from it.
        self._dependency_handles = dependency_handles or []
        self._lock = threading.Lock()
        self._references = 1
        self._unloaded = False

    def try_acquire(self) -> bool:
        """Takes a reference; fails once the instance has been dropped."""
        with self._lock:
            if self._references == 0:
                return False
            self._references += 1
            return True

    def release(self) -> None:
        """Drops a reference, running the unload hook on the last one."""
        with self._lock:
            self._references -= 1
            if self._references > 0 or self._unloaded:
                return
            self._unloaded = True
            dependency_handles, self._dependency_handles = self._dependency_handles, []
        # Outside the lock: the hook is user code and may block. The finally
        # keeps the dependencies from being stranded by whatever it raises.
        try:
            _run_unload_hook(self.resource, self._resource_id)
        finally:
            _release_all(dependency_handles)


def _release_all(handles: List[_ResourceHandle]) -> None:
    """Releases every handle, then reports the first failure: one escaping
    release must not strand the handles behind it.
    """
    failure: Optional[BaseException] = None
    for handle in handles:
        try:
            handle.release()
        except BaseException as e:  # noqa: B036 - re-raised once every handle is released.
            failure = failure or e
    if failure is not None:
        raise failure


class ResourceLease:
    """Keeps the instances a batch resolved usable until it releases them.

    Release it once the batch is done, whatever happened to the resources
    meanwhile.
    """

    def __init__(self, resources: Dict[str, FlowResource], handles: List[_ResourceHandle]):
        self.resources = resources
        self._handles = handles

    def release(self) -> None:
        handles, self._handles = self._handles, []
        _release_all(handles)

    def __enter__(self) -> "ResourceLease":
        return self

    def __exit__(self, *exception_info) -> None:
        self.release()


# Verdicts of the publish-side transitions; the store maps them to in-band statuses.
_VERDICT_PUBLISHED = "published"
_VERDICT_RETIRED = "retired"
_VERDICT_CLOSED = "closed"

# What applying an unload amounted to.
_UNLOAD_RECORDED = "recorded"
_UNLOAD_TOMBSTONED = "tombstoned"
_UNLOAD_NOOP = "noop"
_UNLOAD_RETIRED = "retired"

# Slow paths the init classification resolves to when it reaches no final outcome.
_INIT_DONE = "done"
_INIT_ADVANCE = "advance"
_INIT_CLEAN = "clean"
_INIT_RECONFIGURE = "reconfigure"


def _drop_outside_lock(dropped: List[Any]) -> None:
    """Sink for the references a transition removed from an entry: they are
    handed out of the critical section so that whatever finalizers they own run
    here, outside the entry lock.
    """
    dropped.clear()


@dataclass
class _InitDecision:
    """What one init means against the entry's current identity: either a
    final outcome, or the slow path to run — decided in the same critical
    section as the incarnation advance it may imply.
    """

    action: str
    outcome: Optional[CommandOutcome] = None
    detached: List["_ResourceHandle"] = field(default_factory=list)
    dropped: List[Any] = field(default_factory=list)


@dataclass
class _Entry:
    """Per-resource-id store entry.

    Every field is guarded by ``published_lock``; ``lifecycle_lock`` only
    admits one init at a time and guards no data. Fields change only inside
    the transition methods below, each of which makes its decision and every
    write that decision implies in one ``published_lock`` critical section:
    unload is applied concurrently with admitted inits, so a write sequenced
    by anything weaker races with it. User code never runs under the lock —
    each method returns the detached handles and dropped references for the
    caller to release once outside.
    """

    lifecycle_lock: threading.Lock = field(default_factory=threading.Lock)
    published_lock: threading.Lock = field(default_factory=threading.Lock)
    state: str = _STATE_REGISTERED
    handle: Optional[_ResourceHandle] = None
    incarnation_id: str = ""
    incarnation_generation: int = 0
    configuration_generation: int = 0
    has_incarnation: bool = False
    retired: bool = False
    applied_specs: Optional[_AppliedSpecs] = None
    dependency_references: List[CompanionResourceInstanceReference] = field(default_factory=list)
    context: Optional[ResourceContext] = None
    # Incarnation an admitted init is installing right now. An unload naming it
    # is a retirement of something not yet published, so the identity must span
    # the whole admitted init — in any gap that unload would look like a
    # mismatch and be dropped.
    in_flight_incarnation_id: Optional[str] = None
    # Retirements for incarnations that were not published yet; the publish
    # gates refuse them instead of hosting an instance nobody would retire.
    retired_incarnation_ids: Set[str] = field(default_factory=set)

    def try_admit(self, incarnation_id: str) -> bool:
        """Admits one init and announces which incarnation it installs, as one
        transition: admitted-but-anonymous would drop a concurrent unload of
        that incarnation as a mismatch. Taking ``lifecycle_lock`` inside
        ``published_lock`` inverts the usual order, which is safe only because
        the inner acquire never blocks.
        """
        with self.published_lock:
            if not self.lifecycle_lock.acquire(blocking=False):
                return False
            self.in_flight_incarnation_id = incarnation_id
            return True

    def conclude_init(self) -> None:
        with self.published_lock:
            self.in_flight_incarnation_id = None
            self.lifecycle_lock.release()

    def classify_init(self, resource_id: str, arg: _InitArg, incoming_specs: _AppliedSpecs) -> _InitDecision:
        # NB: Incarnation generations are monotone only within one companion
        # process lifetime: the store is in-memory and the worker-side
        # generation counter resets on worker restart. This is sound because
        # the companion process is always worker-managed, so a restarted
        # worker always talks to a freshly spawned companion with an empty
        # store.
        with self.published_lock:
            if self.has_incarnation and (
                arg.incarnation_generation < self.incarnation_generation
                or (
                    arg.incarnation_generation == self.incarnation_generation
                    and arg.incarnation_id != self.incarnation_id
                )
                or (arg.incarnation_id == self.incarnation_id and self.retired)
            ):
                return _InitDecision(
                    _INIT_DONE,
                    outcome=CommandOutcome(
                        status=RES_STALE_RESOURCE_INCARNATION,
                        error_message=(
                            f"Resource {resource_id!r} incarnation {arg.incarnation_id} is stale; "
                            f"current incarnation is {self.incarnation_id}"
                        ),
                    ),
                )

            if not self.has_incarnation or arg.incarnation_generation > self.incarnation_generation:
                detached = self._detach_locked()
                self.state = _STATE_REGISTERED
                self.incarnation_id = arg.incarnation_id
                self.incarnation_generation = arg.incarnation_generation
                self.configuration_generation = 0
                self.has_incarnation = True
                self.retired = False
                return _InitDecision(_INIT_ADVANCE, detached=detached, dropped=self._reset_applied_locked())

            if arg.configuration_generation < self.configuration_generation:
                if self.state == _STATE_INITIALIZED:
                    return _InitDecision(_INIT_DONE, outcome=CommandOutcome())
                return _InitDecision(
                    _INIT_DONE,
                    outcome=CommandOutcome(
                        status=RES_RESOURCE_NOT_INITIALIZED,
                        error_message=(
                            f"Resource {resource_id!r} is not initialized at configuration "
                            f"generation {self.configuration_generation}"
                        ),
                    ),
                )

            if self.applied_specs is not None and self.applied_specs.spec != incoming_specs.spec:
                return _InitDecision(
                    _INIT_DONE,
                    outcome=CommandOutcome(
                        status=RES_ERROR,
                        error_message=(
                            f"Static resource spec changed within resource {resource_id!r} "
                            f"incarnation {arg.incarnation_id}"
                        ),
                    ),
                )

            dependency_references_changed = arg.dependencies != self.dependency_references

            if arg.configuration_generation == self.configuration_generation:
                # Conflicts are detectable only against successfully applied
                # specs; after a failed init there is nothing to conflict with
                # and the retry must be allowed to rebuild.
                if self.applied_specs is not None:
                    if self.applied_specs.dynamic_spec != incoming_specs.dynamic_spec:
                        return _InitDecision(
                            _INIT_DONE,
                            outcome=CommandOutcome(
                                status=RES_ERROR,
                                error_message=(
                                    f"Resource {resource_id!r} incarnation {arg.incarnation_id} has "
                                    f"conflicting dynamic specs at configuration generation "
                                    f"{arg.configuration_generation}"
                                ),
                            ),
                        )
                    if self.applied_specs.resource_revision != incoming_specs.resource_revision:
                        return _InitDecision(
                            _INIT_DONE,
                            outcome=CommandOutcome(
                                status=RES_ERROR,
                                error_message=(
                                    f"Resource {resource_id!r} incarnation {arg.incarnation_id} has "
                                    f"conflicting revisions at configuration generation "
                                    f"{arg.configuration_generation}"
                                ),
                            ),
                        )
                if self.state == _STATE_INITIALIZED and not dependency_references_changed:
                    return _InitDecision(_INIT_DONE, outcome=CommandOutcome())
                return _InitDecision(_INIT_CLEAN)

            if dependency_references_changed or self.state != _STATE_INITIALIZED:
                return _InitDecision(_INIT_CLEAN)

            return _InitDecision(_INIT_RECONFIGURE)

    def apply_unload(self, incarnation_id: str) -> Tuple[str, List["_ResourceHandle"], List[Any]]:
        """Applies a retirement in one step, whatever else is running: records
        it for the init installing that incarnation, tombstones an entry that
        never had one, ignores a mismatch, or detaches the live instance.
        """
        with self.published_lock:
            if self.in_flight_incarnation_id == incarnation_id and (
                not self.has_incarnation or self.incarnation_id != incarnation_id
            ):
                # Retiring what an init is installing right now: it is not
                # published yet, so the record is what its publish gate will
                # refuse on.
                self.retired_incarnation_ids.add(incarnation_id)
                return _UNLOAD_RECORDED, [], []
            if not self.has_incarnation:
                self.state = _STATE_REGISTERED
                self.incarnation_id = incarnation_id
                self.has_incarnation = True
                self.retired = True
                return _UNLOAD_TOMBSTONED, [], self._reset_applied_locked()
            if self.retired or incarnation_id != self.incarnation_id:
                # Mismatching or already retired: a no-op, and recording it
                # would fence a future successor of that incarnation.
                return _UNLOAD_NOOP, [], []
            self.state = _STATE_REGISTERED
            detached = self._detach_locked()
            self.retired = True
            return _UNLOAD_RETIRED, detached, self._reset_applied_locked()

    def begin_clean_init(self) -> Tuple[List["_ResourceHandle"], List[Any]]:
        """Detaches the served instance before its replacement is built. The
        context goes with it: every way out before a new instance is published
        must leave the retired instance's dependency objects and parameters
        unreachable. ``applied_specs`` stays — it is this incarnation's
        static-spec history, and the immutability check needs it even when a
        replacement fails (the C++ clean-replacement path keeps it too).
        """
        with self.published_lock:
            detached = self._detach_locked()
            self.state = _STATE_REGISTERED
            dropped: List[Any] = [self.context]
            self.context = None
            return detached, dropped

    def publish(
        self,
        handle: "_ResourceHandle",
        arg: _InitArg,
        incoming_specs: _AppliedSpecs,
        context: ResourceContext,
        store_closed: Callable[[], bool],
    ) -> Tuple[str, List[Any]]:
        """Commits a built instance together with the metadata it was built
        from, or refuses without installing either. The closed store is checked
        first: the drain retires every entry, and a stopping store is the more
        accurate answer than a retired incarnation.
        """
        with self.published_lock:
            # Checked under the same lock the drain takes, so either the drain
            # sees this handle or this command sees the closed store.
            if store_closed():
                return _VERDICT_CLOSED, []
            if arg.incarnation_id in self.retired_incarnation_ids or (
                self.retired and self.has_incarnation and self.incarnation_id == arg.incarnation_id
            ):
                self.retired_incarnation_ids.discard(arg.incarnation_id)
                self.retired = True
                self.state = _STATE_REGISTERED
                return _VERDICT_RETIRED, self._reset_applied_locked()
            self.applied_specs = incoming_specs
            self.dependency_references = list(arg.dependencies)
            self.context = context
            self.handle = handle
            self.configuration_generation = arg.configuration_generation
            self.state = _STATE_INITIALIZED
            return _VERDICT_PUBLISHED, []

    def begin_reconfigure(
        self, store_closed: Callable[[], bool]
    ) -> Tuple[str, Optional["_ResourceHandle"], Optional[ResourceContext]]:
        """Takes a reference on the served instance and bars new acquisitions
        while the hook mutates it, so no batch starts against half-applied
        parameters (mirrors the C++ EState::Reconfiguring fence).
        """
        with self.published_lock:
            handle = self.handle
            if handle is None or not handle.try_acquire():
                # Only an unload or the drain takes the instance away between
                # the classification and here, and they are different answers.
                return (_VERDICT_CLOSED if store_closed() else _VERDICT_RETIRED), None, None
            self.state = _STATE_RECONFIGURING
            return _VERDICT_PUBLISHED, handle, self.context

    def fail_reconfigure(self) -> Tuple[List["_ResourceHandle"], List[Any]]:
        """Detaches after a failed hook: the entry serves nothing from here, so
        it must not keep the instance's context, dependencies and parameters
        reachable either.
        """
        with self.published_lock:
            detached = self._detach_locked()
            self.state = _STATE_RECONFIGURE_FAILED
            return detached, self._reset_applied_locked()

    def commit_reconfigure(
        self,
        handle: "_ResourceHandle",
        arg: _InitArg,
        incoming_specs: _AppliedSpecs,
        context: ResourceContext,
        store_closed: Callable[[], bool],
    ) -> Tuple[str, List["_ResourceHandle"], List[Any]]:
        """Publishes the reconfigured generation together with the metadata the
        hook applied, or refuses without restoring either: an entry whose
        instance went away while the hook ran keeps nothing of it.
        """
        with self.published_lock:
            # Checked first, as the clean-init publish does: the drain walks the
            # entries one by one, so a closed store can reach here while this
            # entry's handle is still intact — committing then would report OK
            # for a generation already unreachable. The drain owns the detach.
            if store_closed():
                return _VERDICT_CLOSED, [], []
            if self.handle is not handle:
                # The instance went away while the hook ran — and the store is
                # open, so only a retirement takes it away. Republishing would
                # mark the entry initialized over something already released.
                return _VERDICT_RETIRED, [], []
            if self.retired or arg.incarnation_id in self.retired_incarnation_ids:
                # Retired while the hook ran: republishing would host it with
                # nobody left to retire it.
                self.retired_incarnation_ids.discard(arg.incarnation_id)
                detached = self._detach_locked()
                self.retired = True
                self.state = _STATE_REGISTERED
                return _VERDICT_RETIRED, detached, self._reset_applied_locked()
            self.applied_specs = incoming_specs
            self.context = context
            self.configuration_generation = arg.configuration_generation
            self.state = _STATE_INITIALIZED
            return _VERDICT_PUBLISHED, [], []

    def drain(self) -> List["_ResourceHandle"]:
        """Retires whatever is published when the store shuts down. Only the
        served instance is dropped here; a command still running user code
        finishes against its own gates and releases the rest itself.
        """
        with self.published_lock:
            detached = self._detach_locked()
            self.state = _STATE_REGISTERED
            self.retired = True
            return detached

    def _detach_locked(self) -> List["_ResourceHandle"]:
        """Clears the published instance, returning the store's reference for
        the caller to release outside ``published_lock``: the last release runs
        user code.
        """
        handles: List["_ResourceHandle"] = []
        if self.handle is not None:
            handles.append(self.handle)
            self.handle = None
        return handles

    def _reset_applied_locked(self) -> List[Any]:
        """Forgets what this entry has applied, so the next init converges from
        scratch (mirrors the C++ TEntry::ResetApplied). Returns the dropped
        references — the context owns the dependency instances and the parsed
        parameters of the retired generation — for the caller to let go of
        outside the lock.
        """
        dropped: List[Any] = [self.applied_specs, self.dependency_references, self.context]
        self.applied_specs = None
        self.dependency_references = []
        self.context = None
        return dropped


class ResourceStore:
    """Process-wide store of the resources hosted inside this companion, keyed
    by resource id. Resources are process-scoped and shared by every job that
    requires them.
    """

    def __init__(self, resource_factories: Dict[str, Callable[[], FlowResource]]):
        self._resource_factories = dict(resource_factories)
        self._lock = threading.Lock()
        self._entries: Dict[str, _Entry] = {}
        # Bars late commands: handlers may still be running when the store is
        # drained.
        self._closed = False

    def execute(self, resource_id: str, command: int, argument: Optional[bytes]) -> CommandOutcome:
        """Dispatches one ResourceExecute command. User-code failures are
        reported in-band; exceptions escape only on companion bugs.
        """
        if command not in (COMMAND_INIT, COMMAND_UNLOAD):
            return CommandOutcome(
                status=RES_UNSUPPORTED,
                error_message=f"Unsupported resource command {command}",
            )
        # Refuses and creates under one lock: an entry created after the drain
        # would never be drained again.
        entry = self._get_or_create_entry(resource_id)
        if entry is None:
            return CommandOutcome(status=RES_ERROR, error_message=_STORE_SHUT_DOWN)
        if command == COMMAND_UNLOAD:
            # Unload is never admitted, and needs no admission: it runs no user code and is
            # one atomic transition. Making it contend is what once made retirement
            # deferrable — and therefore droppable — so it now always takes effect,
            # whatever else is running.
            return self._do_unload(resource_id, entry, argument)

        # Parsed before admission, so the identity the admission announces spans the whole
        # admitted init.
        try:
            arg = _parse_init_arg(argument)
            incoming_specs = _canonical_specs(arg)
        except Exception as e:
            log.warning("Companion resource init failed: (ResourceId: %s)", resource_id, exc_info=True)
            return CommandOutcome(status=RES_ERROR, error_message=_error_message(e))

        # Init admission never waits: this runs on the companion's small RPC pool and the
        # admission spans user hooks, so parking here would let one slow load plus a few
        # contenders occupy every worker and stall unrelated calls. Python cannot suspend
        # without holding its thread, so a command that cannot get in returns immediately.
        if not entry.try_admit(arg.incarnation_id):
            return CommandOutcome(
                status=RES_RESOURCE_NOT_INITIALIZED,
                error_message="Another command for this companion resource is still running",
            )
        try:
            return self._do_init(resource_id, entry, arg, incoming_specs)
        finally:
            entry.conclude_init()

    def _find_handle(self, reference: CompanionResourceInstanceReference) -> Optional[_ResourceHandle]:
        """Returns the handle of the initialized instance matching the
        reference exactly, or None. Cheap and non-blocking; safe on the batch
        hot path.
        """
        with self._lock:
            entry = self._entries.get(reference.resource_id)
        if entry is None:
            return None
        with entry.published_lock:
            if (
                entry.state == _STATE_INITIALIZED
                and entry.has_incarnation
                and entry.incarnation_id == reference.incarnation_id
                and entry.configuration_generation == reference.configuration_generation
            ):
                return entry.handle
            return None

    def acquire(self, references: List[CompanionResourceInstanceReference]) -> Optional[ResourceLease]:
        """Resolves the aliased references to their exact initialized instances
        and keeps every one of them alive until the lease is released.

        Returns None when any reference (aliased or not) no longer matches its
        initialized instance — an in-band retryable condition.
        """
        resources: Dict[str, FlowResource] = {}
        handles: List[_ResourceHandle] = []
        for reference in references:
            handle = self._find_handle(reference)
            if handle is None or not handle.try_acquire():
                _release_all(handles)
                return None
            handles.append(handle)
            if reference.alias is not None:
                resources[reference.alias] = handle.resource
        return ResourceLease(resources, handles)

    def shutdown(self) -> None:
        """Drops the store's reference to every hosted instance.

        Call it when the serving generation goes away, so nothing it built
        outlives it unloaded. Idempotent, and never waits: an instance a batch
        still holds is torn down once that batch releases it.
        """
        with self._lock:
            self._closed = True
            entries = list(self._entries.values())
            self._entries.clear()

        failure: Optional[BaseException] = None
        for entry in entries:
            # Still worth draining: a batch may hold the entry it took just
            # before the map was cleared, and a command still running user code
            # finishes against its own gates.
            detached = entry.drain()
            # Finish the drain whatever a hook raises, then report the first
            # failure: nothing can reach these entries again.
            try:
                _release_all(detached)
            except BaseException as e:  # noqa: B036 - re-raised once the drain completes.
                failure = failure or e

        if failure is not None:
            raise failure

    def _get_or_create_entry(self, resource_id: str) -> Optional[_Entry]:
        """Returns the entry of the resource, creating it on first use, or None
        once the store is closed.
        """
        with self._lock:
            if self._closed:
                return None
            entry = self._entries.get(resource_id)
            if entry is None:
                entry = _Entry()
                self._entries[resource_id] = entry
            return entry

    def _do_init(
        self,
        resource_id: str,
        entry: _Entry,
        arg: _InitArg,
        incoming_specs: _AppliedSpecs,
    ) -> CommandOutcome:
        try:
            decision = entry.classify_init(resource_id, arg, incoming_specs)
            _drop_outside_lock(decision.dropped)
            _release_all(decision.detached)
            if decision.outcome is not None:
                return decision.outcome
            if decision.action == _INIT_ADVANCE:
                log.info(
                    "Advancing companion resource incarnation: "
                    "(ResourceId: %s, IncarnationId: %s, IncarnationGeneration: %s, ConfigurationGeneration: %s)",
                    resource_id,
                    arg.incarnation_id,
                    arg.incarnation_generation,
                    arg.configuration_generation,
                )
                return self._initialize_clean_instance(resource_id, entry, arg, incoming_specs)
            if decision.action == _INIT_CLEAN:
                return self._initialize_clean_instance(resource_id, entry, arg, incoming_specs)
            return self._apply_reconfigure(resource_id, entry, arg, incoming_specs)
        except Exception as e:
            log.warning("Companion resource init failed: (ResourceId: %s)", resource_id, exc_info=True)
            return CommandOutcome(status=RES_ERROR, error_message=_error_message(e))

    def _do_unload(self, resource_id: str, entry: _Entry, argument: Optional[bytes]) -> CommandOutcome:
        try:
            incarnation_id = _parse_incarnation_id(_parse_argument(argument))
        except Exception as e:
            return CommandOutcome(status=RES_ERROR, error_message=_error_message(e))

        _action, detached, dropped = entry.apply_unload(incarnation_id)
        _drop_outside_lock(dropped)
        if detached:
            log.info(
                "Companion resource unloaded: (ResourceId: %s, IncarnationId: %s)",
                resource_id,
                incarnation_id,
            )
        # Bars new acquisitions immediately; the hook itself waits for the
        # batches that already hold the instance.
        _release_all(detached)
        return CommandOutcome()

    def _initialize_clean_instance(
        self,
        resource_id: str,
        entry: _Entry,
        arg: _InitArg,
        incoming_specs: _AppliedSpecs,
    ) -> CommandOutcome:
        class_name = _extract_companion_resource_class(arg.spec)
        factory = self._resource_factories.get(class_name)
        if factory is None:
            return CommandOutcome(
                status=RES_RESOURCE_NOT_FOUND,
                error_message=(
                    f"Companion has no factory for resource class {class_name!r}; "
                    f"declare it via Pipeline.add_resource"
                ),
            )

        detached, dropped = entry.begin_clean_init()
        _drop_outside_lock(dropped)
        _release_all(detached)

        # Hold a reference on every dependency for as long as the instance
        # built from it lives, so a retired dependency is torn down only after
        # its dependents are.
        dependencies: Dict[str, FlowResource] = {}
        dependency_handles: List[_ResourceHandle] = []
        missing_dependency_ids: List[str] = []
        for reference in arg.dependencies:
            handle = self._find_handle(reference)
            if handle is None or not handle.try_acquire():
                missing_dependency_ids.append(reference.resource_id)
                continue
            dependency_handles.append(handle)
            alias = reference.alias if reference.alias is not None else reference.resource_id
            dependencies[alias] = handle.resource
        if missing_dependency_ids:
            _release_all(dependency_handles)
            return CommandOutcome(
                status=RES_RESOURCE_NOT_INITIALIZED,
                error_message=(
                    f"Companion dependencies {missing_dependency_ids} are not initialized "
                    f"for resource {resource_id!r}"
                ),
            )

        context = ResourceContext(
            resource_id=resource_id,
            parameters=_extract_parameters(arg.spec),
            dynamic_parameters=_extract_parameters(arg.dynamic_spec),
            dependencies=dependencies,
        )
        resource: Optional[FlowResource] = None
        built = False
        try:
            # Construction shares the load's cleanup scope: the worker retries
            # init, so a failure of either must leave nothing behind.
            resource = factory()
            log.info(
                "Loading companion resource: "
                "(ResourceId: %s, ResourceClass: %s, IncarnationId: %s, ConfigurationGeneration: %s)",
                resource_id,
                class_name,
                arg.incarnation_id,
                arg.configuration_generation,
            )
            resource.load(context)
            built = True
        finally:
            # In a finally, not an except clause: a BaseException escaping user code must
            # not strand the half-built instance or the dependency references this attempt
            # acquired, since no handle owns them yet. The exception itself propagates.
            if not built:
                try:
                    if resource is not None:
                        _run_unload_hook(resource, resource_id)
                finally:
                    _release_all(dependency_handles)

        # The handle takes over the dependency references from here on.
        handle = _ResourceHandle(resource, resource_id, dependency_handles)
        # Publish or refuse in the one section that unload also takes: a retirement is
        # therefore either visible at the gate, or applied to the handle published there.
        verdict, dropped = entry.publish(handle, arg, incoming_specs, context, lambda: self._closed)
        _drop_outside_lock(dropped)
        if verdict == _VERDICT_RETIRED:
            handle.release()
            log.info(
                "Companion resource retired by the unload that arrived while it loaded: "
                "(ResourceId: %s, IncarnationId: %s)",
                resource_id,
                arg.incarnation_id,
            )
            return CommandOutcome(status=RES_STALE_RESOURCE_INCARNATION, error_message="Resource was unloaded")
        if verdict == _VERDICT_CLOSED:
            # Drained while this command ran user code: the entry is
            # unreachable, so publishing here would strand the instance.
            handle.release()
            return CommandOutcome(status=RES_ERROR, error_message=_STORE_SHUT_DOWN)
        log.info(
            "Companion resource initialized: (ResourceId: %s, IncarnationId: %s, ConfigurationGeneration: %s)",
            resource_id,
            arg.incarnation_id,
            arg.configuration_generation,
        )
        return CommandOutcome()

    def _apply_reconfigure(
        self,
        resource_id: str,
        entry: _Entry,
        arg: _InitArg,
        incoming_specs: _AppliedSpecs,
    ) -> CommandOutcome:
        verdict, handle, previous_context = entry.begin_reconfigure(lambda: self._closed)
        if handle is None:
            if verdict == _VERDICT_CLOSED:
                return CommandOutcome(status=RES_ERROR, error_message=_STORE_SHUT_DOWN)
            return CommandOutcome(status=RES_STALE_RESOURCE_INCARNATION, error_message="Resource was unloaded")
        assert previous_context is not None
        context = ResourceContext(
            resource_id=resource_id,
            parameters=previous_context.parameters,
            dynamic_parameters=_extract_parameters(arg.dynamic_spec),
            dependencies=previous_context.dependencies,
        )
        try:
            handle.resource.reconfigure(context)
        except Exception as e:
            log.warning(
                "Companion resource reconfigure failed: (ResourceId: %s)",
                resource_id,
                exc_info=True,
            )
            detached, dropped = entry.fail_reconfigure()
            _drop_outside_lock(dropped)
            _release_all(detached)
            return CommandOutcome(status=RES_ERROR, error_message=_error_message(e))
        finally:
            handle.release()

        # SDK-hosted resources do not track revisions, so by contract the
        # switch to the delivered dynamic spec is instant and the incoming
        # generation is published right away (cf. TResourceStore's
        # revision-gated commit for revision-tracking C++ resources).
        verdict, detached, dropped = entry.commit_reconfigure(
            handle, arg, incoming_specs, context, lambda: self._closed
        )
        _drop_outside_lock(dropped)
        if verdict == _VERDICT_CLOSED:
            return CommandOutcome(status=RES_ERROR, error_message=_STORE_SHUT_DOWN)
        if verdict == _VERDICT_RETIRED:
            _release_all(detached)
            return CommandOutcome(status=RES_STALE_RESOURCE_INCARNATION, error_message="Resource was unloaded")
        log.info(
            "Companion resource reconfigured: (ResourceId: %s, ConfigurationGeneration: %s)",
            resource_id,
            arg.configuration_generation,
        )
        return CommandOutcome()
