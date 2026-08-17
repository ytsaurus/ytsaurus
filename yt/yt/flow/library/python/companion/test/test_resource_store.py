"""Tests for the companion resource store; mirrors the C++ resource_store_ut cases."""

import threading
import time

import pytest

import yt.yson as yson

from yt.yt.flow.library.python.companion.computation import Computation, RowFunction
from yt.yt.flow.library.python.companion.context import PipelineContext
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.resource import (
    COMMAND_INIT,
    COMMAND_UNLOAD,
    CompanionResourceInstanceReference,
    FlowResource,
    ResourceStore,
    RES_ERROR,
    RES_OK,
    RES_RESOURCE_NOT_FOUND,
    RES_RESOURCE_NOT_INITIALIZED,
    RES_STALE_RESOURCE_INCARNATION,
    RES_UNSUPPORTED,
)
from yt.yt.flow.library.python.companion.service import CompanionRequestProcessor

INCARNATION_A = "1-2-3-4"
INCARNATION_B = "5-6-7-8"


class TrackingResource(FlowResource):
    """Records lifecycle calls and current parameters."""

    instances = []

    def __init__(self, fail_load=False, fail_reconfigure=False):
        self.loaded = False
        self.unloaded = False
        self.reconfigure_count = 0
        self.context = None
        self.fail_load = fail_load
        self.fail_reconfigure = fail_reconfigure
        TrackingResource.instances.append(self)

    def load(self, context):
        if self.fail_load:
            raise RuntimeError("load failed")
        self.loaded = True
        self.context = context

    def reconfigure(self, context):
        if self.fail_reconfigure:
            raise RuntimeError("reconfigure failed")
        self.reconfigure_count += 1
        self.context = context

    def unload(self):
        self.unloaded = True


def _make_store(factories=None):
    TrackingResource.instances = []
    if factories is None:
        factories = {"TrackingResource": TrackingResource}
    return ResourceStore(factories)


def _init_arg(
    incarnation_id=INCARNATION_A,
    incarnation_generation=1,
    configuration_generation=0,
    parameters=None,
    dynamic_parameters=None,
    dependencies=None,
    resource_class="TrackingResource",
):
    spec_parameters = {"companion_resource_class": resource_class}
    spec_parameters.update(parameters or {})
    arg = {
        "spec": {"resource_class_name": "WorkerProxy", "parameters": spec_parameters},
        "dynamic_spec": {"parameters": dynamic_parameters or {}},
        "incarnation_id": incarnation_id,
        "incarnation_generation": incarnation_generation,
        "configuration_generation": configuration_generation,
        "dependencies": dependencies or [],
    }
    return yson.dumps(arg)


def _unload_arg(incarnation_id=INCARNATION_A):
    return yson.dumps({"incarnation_id": incarnation_id})


def _reference(resource_id, incarnation_id=INCARNATION_A, configuration_generation=0, alias=None):
    return CompanionResourceInstanceReference(
        resource_id=resource_id,
        incarnation_id=incarnation_id,
        configuration_generation=configuration_generation,
        alias=alias,
    )


def _acquired_resource(store, reference):
    """Instance the store serves for the reference, or None, through the
    production acquire path — which, unlike a bare lookup, also requires the
    handle to still hold a live reference.
    """
    probe = CompanionResourceInstanceReference(
        resource_id=reference.resource_id,
        incarnation_id=reference.incarnation_id,
        configuration_generation=reference.configuration_generation,
        alias="probe",
    )
    lease = store.acquire([probe])
    if lease is None:
        return None
    try:
        return lease.resources["probe"]
    finally:
        lease.release()


def _dependency_node(resource_id, incarnation_id=INCARNATION_A, configuration_generation=0, alias=None):
    node = {
        "resource_id": resource_id,
        "incarnation_id": incarnation_id,
        "configuration_generation": configuration_generation,
    }
    if alias is not None:
        node["alias"] = alias
    return node


class TestResourceStore:
    def test_init_once_and_converge_no_op(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert len(TrackingResource.instances) == 1
        assert TrackingResource.instances[0].loaded

        # The very same init converges without rebuilding the instance.
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert len(TrackingResource.instances) == 1

        resource = _acquired_resource(store, _reference("r"))
        assert resource is TrackingResource.instances[0]

    def test_load_receives_parameters(self):
        store = _make_store()
        argument = _init_arg(parameters={"greeting": "hello"}, dynamic_parameters={"suffix": "v1"})
        assert store.execute("r", COMMAND_INIT, argument).status == RES_OK
        context = TrackingResource.instances[0].context
        assert context.resource_id == "r"
        assert context.parameters["greeting"] == "hello"
        assert context.dynamic_parameters["suffix"] == "v1"

    def test_converge_on_changed_dynamic_spec(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg(dynamic_parameters={"v": "1"})).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}),
        )
        assert outcome.status == RES_OK
        # The same instance was reconfigured, not replaced.
        assert len(TrackingResource.instances) == 1
        assert TrackingResource.instances[0].reconfigure_count == 1

        # The old generation reference no longer matches; the new one does.
        assert _acquired_resource(store, _reference("r", configuration_generation=0)) is None
        assert _acquired_resource(store, _reference("r", configuration_generation=1)) is not None

    def test_static_spec_change_rejected(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg(parameters={"a": "1"})).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(configuration_generation=1, parameters={"a": "2"}),
        )
        assert outcome.status == RES_ERROR
        assert "Static resource spec changed" in outcome.error_message

    def test_conflicting_dynamic_specs_at_same_generation_rejected(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg(dynamic_parameters={"v": "1"})).status == RES_OK
        outcome = store.execute("r", COMMAND_INIT, _init_arg(dynamic_parameters={"v": "2"}))
        assert outcome.status == RES_ERROR
        assert "conflicting dynamic specs" in outcome.error_message

    def test_unload_bars_new_acquisition(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert _acquired_resource(store, _reference("r")) is None
        assert TrackingResource.instances[0].unloaded

        # Unload is idempotent.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK

    def test_retired_incarnation_cannot_be_revived(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        outcome = store.execute("r", COMMAND_INIT, _init_arg())
        assert outcome.status == RES_STALE_RESOURCE_INCARNATION

    def test_reinit_after_unload_creates_fresh_instance(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2),
        )
        assert outcome.status == RES_OK
        assert len(TrackingResource.instances) == 2
        assert _acquired_resource(store, _reference("r", incarnation_id=INCARNATION_B)) is not None

    def test_out_of_order_init_converges_to_newest_incarnation(self):
        store = _make_store()
        assert (
            store.execute(
                "r",
                COMMAND_INIT,
                _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2),
            ).status
            == RES_OK
        )
        # A late init of the older incarnation must not displace the newer one.
        outcome = store.execute("r", COMMAND_INIT, _init_arg(incarnation_generation=1))
        assert outcome.status == RES_STALE_RESOURCE_INCARNATION
        assert _acquired_resource(store, _reference("r", incarnation_id=INCARNATION_B)) is not None

    def test_late_unload_cannot_retire_successor(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert (
            store.execute(
                "r",
                COMMAND_INIT,
                _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2),
            ).status
            == RES_OK
        )
        # A late unload of the older incarnation is a no-op.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg(INCARNATION_A)).status == RES_OK
        assert _acquired_resource(store, _reference("r", incarnation_id=INCARNATION_B)) is not None

    def test_mismatching_unload_does_not_fence_future_successor(self):
        store = _make_store()
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg(INCARNATION_A)).status == RES_OK
        # The tombstone fences only its own incarnation.
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2),
        )
        assert outcome.status == RES_OK

    def test_unload_before_init_creates_tombstone(self):
        store = _make_store()
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        outcome = store.execute("r", COMMAND_INIT, _init_arg())
        assert outcome.status == RES_STALE_RESOURCE_INCARNATION

    def test_configuration_generations_converge(self):
        store = _make_store()
        assert (
            store.execute(
                "r",
                COMMAND_INIT,
                _init_arg(configuration_generation=2, dynamic_parameters={"v": "2"}),
            ).status
            == RES_OK
        )
        # An older generation converges without touching the newer state.
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(configuration_generation=1, dynamic_parameters={"v": "1"}),
        )
        assert outcome.status == RES_OK
        assert _acquired_resource(store, _reference("r", configuration_generation=2)) is not None
        assert len(TrackingResource.instances) == 1

    def test_failed_reconfigure_quarantines_and_recreates_resource(self):
        store = _make_store({"TrackingResource": lambda: TrackingResource(fail_reconfigure=True)})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}),
        )
        assert outcome.status == RES_ERROR
        assert _acquired_resource(store, _reference("r")) is None
        # The retry of the same init rebuilds a fresh instance.
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}),
        )
        assert outcome.status == RES_OK
        assert len(TrackingResource.instances) == 2
        assert _acquired_resource(store, _reference("r", configuration_generation=1)) is not None

    def test_unknown_class(self):
        store = _make_store()
        outcome = store.execute("r", COMMAND_INIT, _init_arg(resource_class="Unknown"))
        assert outcome.status == RES_RESOURCE_NOT_FOUND

    def test_unknown_command(self):
        store = _make_store()
        outcome = store.execute("r", 100, _init_arg())
        assert outcome.status == RES_UNSUPPORTED

    def test_malformed_argument(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, None).status == RES_ERROR
        assert store.execute("r", COMMAND_INIT, b"[]").status == RES_ERROR
        assert store.execute("r", COMMAND_UNLOAD, b"{}").status == RES_ERROR

    def test_failed_load_is_retryable(self):
        fail = {"value": True}

        def factory():
            return TrackingResource(fail_load=fail["value"])

        store = _make_store({"TrackingResource": factory})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_ERROR
        assert _acquired_resource(store, _reference("r")) is None
        fail["value"] = False
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert _acquired_resource(store, _reference("r")) is not None

    def test_dependency_alias_resolution(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(dependencies=[_dependency_node("dep", alias="my_dep")]),
        )
        assert outcome.status == RES_OK
        dependent = TrackingResource.instances[1]
        assert dependent.context.dependencies["my_dep"] is TrackingResource.instances[0]

        # Without an alias the dependency is exposed under its resource id.
        outcome = store.execute(
            "r2",
            COMMAND_INIT,
            _init_arg(dependencies=[_dependency_node("dep")]),
        )
        assert outcome.status == RES_OK
        assert TrackingResource.instances[2].context.dependencies["dep"] is TrackingResource.instances[0]

    def test_missing_and_stale_dependencies_are_rejected(self):
        store = _make_store()
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(dependencies=[_dependency_node("dep")]),
        )
        assert outcome.status == RES_RESOURCE_NOT_INITIALIZED
        assert "dep" in outcome.error_message

        # A dependency at a different configuration generation is stale.
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(dependencies=[_dependency_node("dep", configuration_generation=5)]),
        )
        assert outcome.status == RES_RESOURCE_NOT_INITIALIZED

    def test_dependency_reference_change_recreates_dependent(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        assert (
            store.execute(
                "r",
                COMMAND_INIT,
                _init_arg(dependencies=[_dependency_node("dep")]),
            ).status
            == RES_OK
        )

        # Advance the dependency, then re-init the dependent with the new reference.
        assert (
            store.execute(
                "dep",
                COMMAND_INIT,
                _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}),
            ).status
            == RES_OK
        )
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(
                configuration_generation=1,
                dependencies=[_dependency_node("dep", configuration_generation=1)],
            ),
        )
        assert outcome.status == RES_OK
        # The dependent was rebuilt against the advanced dependency instance.
        dependent = TrackingResource.instances[-1]
        assert dependent.loaded
        assert dependent.context.dependencies["dep"] is _acquired_resource(
            store, _reference("dep", configuration_generation=1)
        )

    def test_concurrent_init_loads_once(self):
        loading = threading.Event()
        proceed = threading.Event()

        class BlockingResource(TrackingResource):
            def load(self, context):
                loading.set()
                proceed.wait(timeout=10)
                super().load(context)

        store = _make_store({"TrackingResource": BlockingResource})
        outcomes = []

        first = threading.Thread(target=lambda: outcomes.append(store.execute("r", COMMAND_INIT, _init_arg())))
        first.start()
        assert loading.wait(timeout=10)

        # Admission does not wait, so a command arriving while another one runs is refused
        # with the retryable status rather than parking a thread of the RPC pool.
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_RESOURCE_NOT_INITIALIZED

        proceed.set()
        first.join(timeout=10)

        # The worker's retry then converges on the instance the first init built.
        assert outcomes[0].status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        assert len(TrackingResource.instances) == 1

    def test_acquire_resolves_aliases(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        lease = store.acquire(
            [
                _reference("r", alias="view"),
                _reference("r"),
            ]
        )
        assert set(lease.resources.keys()) == {"view"}
        lease.release()

        assert store.acquire([_reference("r", configuration_generation=7)]) is None

    def test_unload_waits_for_the_batches_holding_the_instance(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        resource = TrackingResource.instances[0]

        lease = store.acquire([_reference("r", alias="view")])
        assert lease.resources["view"] is resource

        # Retiring the instance bars new acquisitions at once, but the batch
        # still holding it keeps a usable object.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert store.acquire([_reference("r", alias="view")]) is None
        assert not resource.unloaded

        lease.release()
        assert resource.unloaded

    def test_replacing_instance_waits_for_the_batches_holding_it(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        old = TrackingResource.instances[0]
        lease = store.acquire([_reference("r", alias="view")])

        # A fresh incarnation replaces the instance under a running batch.
        assert (
            store.execute(
                "r",
                COMMAND_INIT,
                _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2),
            ).status
            == RES_OK
        )
        assert not old.unloaded
        assert lease.resources["view"] is old

        lease.release()
        assert old.unloaded

    def test_dependency_outlives_its_dependent(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("dep")])).status == RES_OK
        dependency, dependent = TrackingResource.instances[0], TrackingResource.instances[1]

        # Retiring the dependency must not tear it down while a dependent that
        # captured it is still alive.
        assert store.execute("dep", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert not dependency.unloaded

        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert dependent.unloaded
        assert dependency.unloaded

    def test_dependency_is_unloaded_after_its_dependent_whatever_the_lease_order(self):
        order = []

        class OrderedResource(TrackingResource):
            def unload(self):
                order.append(self.context.resource_id)
                super().unload()

        store = _make_store({"TrackingResource": OrderedResource})
        # "a_pool" sorts before "z_service", and the worker sorts a job's
        # references by resource id, so the lease releases the dependency first.
        assert store.execute("a_pool", COMMAND_INIT, _init_arg()).status == RES_OK
        assert (
            store.execute("z_service", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("a_pool")])).status
            == RES_OK
        )

        lease = store.acquire([_reference("a_pool", alias="dep"), _reference("z_service", alias="svc")])
        assert store.execute("z_service", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert store.execute("a_pool", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert order == []

        lease.release()
        # The dependent must be torn down before the dependency it was built
        # from, however the lease happened to walk its handles.
        assert order == ["z_service", "a_pool"]

    def test_failing_factory_releases_acquired_dependencies(self):
        def factory():
            if fail["value"]:
                raise RuntimeError("factory failed")
            return TrackingResource()

        fail = {"value": False}
        store = _make_store({"TrackingResource": factory})
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        dependency = TrackingResource.instances[0]

        # The dependency is acquired before the instance is built, so a factory
        # that raises must not strand that reference: the worker retries init.
        fail["value"] = True
        for _ in range(3):
            outcome = store.execute("r", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("dep")]))
            assert outcome.status == RES_ERROR

        assert store.execute("dep", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        assert dependency.unloaded

    def test_shutdown_unloads_every_hosted_resource(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("dep")])).status == RES_OK
        dependency, dependent = TrackingResource.instances[0], TrackingResource.instances[1]

        store.shutdown()
        assert dependent.unloaded
        assert dependency.unloaded
        assert store.acquire([_reference("r")]) is None

        # Idempotent: a second shutdown finds nothing left to release.
        store.shutdown()

    def test_shutdown_defers_to_the_batch_still_holding_a_lease(self):
        store = _make_store()
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        resource = TrackingResource.instances[0]
        lease = store.acquire([_reference("r", alias="view")])

        store.shutdown()
        assert not resource.unloaded

        lease.release()
        assert resource.unloaded

    def test_init_finishing_after_shutdown_does_not_strand_the_instance(self):
        drained = threading.Event()

        class LateResource(TrackingResource):
            def load(self, context):
                # Stand in for a command still running user code when the
                # serving generation is torn down.
                store.shutdown()
                drained.set()
                super().load(context)

        store = _make_store({"TrackingResource": LateResource})
        outcome = store.execute("r", COMMAND_INIT, _init_arg())

        assert drained.is_set()
        assert outcome.status == RES_ERROR
        # The entry is unreachable by then, so publishing would strand the
        # instance: it must be torn down instead.
        assert TrackingResource.instances[0].unloaded
        assert store.acquire([_reference("r")]) is None

    def test_commands_are_refused_once_the_store_is_shut_down(self):
        store = _make_store()
        store.shutdown()

        # A late command must not repopulate the store it was just drained from.
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_ERROR
        assert TrackingResource.instances == []
        assert store.acquire([_reference("r")]) is None

    def test_reconfigure_running_at_shutdown_keeps_its_instance_alive(self):
        observed = {}

        class LateReconfigureResource(TrackingResource):
            def reconfigure(self, context):
                # Stand in for a hook still running when the serving generation
                # is torn down.
                store.shutdown()
                observed["unloaded_during_hook"] = self.unloaded
                super().reconfigure(context)

        store = _make_store({"TrackingResource": LateReconfigureResource})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        resource = TrackingResource.instances[0]

        outcome = store.execute("r", COMMAND_INIT, _init_arg(configuration_generation=1))

        # User code must never run against an instance already unloaded.
        assert observed["unloaded_during_hook"] is False
        assert resource.reconfigure_count == 1
        # Nothing to publish afterwards: the entry is unreachable.
        assert outcome.status == RES_ERROR
        assert resource.unloaded
        assert store.acquire([_reference("r", configuration_generation=1)]) is None

    def test_reconfigure_overtaken_by_a_blocked_drain_is_refused(self):
        unloading = threading.Event()
        proceed_unload = threading.Event()
        reconfiguring = threading.Event()
        proceed_reconfigure = threading.Event()

        class BlockingUnloadResource(TrackingResource):
            def unload(self):
                unloading.set()
                proceed_unload.wait(timeout=10)
                super().unload()

        class BlockingReconfigureResource(TrackingResource):
            def reconfigure(self, context):
                reconfiguring.set()
                proceed_reconfigure.wait(timeout=10)
                super().reconfigure(context)

        store = _make_store(
            {
                "BlockingUnload": BlockingUnloadResource,
                "BlockingReconfigure": BlockingReconfigureResource,
            }
        )
        assert store.execute("a", COMMAND_INIT, _init_arg(resource_class="BlockingUnload")).status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg(resource_class="BlockingReconfigure")).status == RES_OK

        outcomes = []
        reconfigure = threading.Thread(
            target=lambda: outcomes.append(
                store.execute(
                    "r",
                    COMMAND_INIT,
                    _init_arg(resource_class="BlockingReconfigure", configuration_generation=1),
                )
            )
        )
        reconfigure.start()
        assert reconfiguring.wait(timeout=10)

        shutdown = threading.Thread(target=store.shutdown)
        shutdown.start()
        # The drain walks entries in creation order, so it blocks in "a"'s
        # unload hook while "r" is still undrained and the store already closed.
        assert unloading.wait(timeout=10)

        proceed_reconfigure.set()
        reconfigure.join(timeout=10)
        proceed_unload.set()
        shutdown.join(timeout=10)

        # The commit must see the closed store even though its own handle is
        # still intact: OK here would report a configuration generation that is
        # already unreachable from the closed store.
        assert outcomes[0].status == RES_ERROR
        assert all(instance.unloaded for instance in TrackingResource.instances)

    def test_lease_releases_every_handle_even_when_a_hook_escapes(self):
        class ExplodingResource(TrackingResource):
            def unload(self):
                super().unload()
                raise BaseException("hook escaped")  # noqa: TRY002

        store = _make_store({"TrackingResource": ExplodingResource})
        assert store.execute("a", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("b", COMMAND_INIT, _init_arg()).status == RES_OK
        lease = store.acquire([_reference("a", alias="a"), _reference("b", alias="b")])

        # The lease still holds both instances, so the drain defers to it and
        # no hook runs here.
        store.shutdown()
        assert not any(instance.unloaded for instance in TrackingResource.instances)

        with pytest.raises(BaseException, match="hook escaped"):
            lease.release()

        # The first escaping hook must not cost the other resource its teardown.
        assert all(instance.unloaded for instance in TrackingResource.instances)

    def test_shutdown_drains_every_entry_even_when_a_hook_escapes(self):
        class ExplodingResource(TrackingResource):
            def unload(self):
                super().unload()
                raise BaseException("hook escaped")  # noqa: TRY002

        store = _make_store({"TrackingResource": ExplodingResource})
        assert store.execute("a", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("b", COMMAND_INIT, _init_arg()).status == RES_OK

        with pytest.raises(BaseException, match="hook escaped"):
            store.shutdown()

        # The first escaping hook must not cost the remaining entries theirs.
        assert all(instance.unloaded for instance in TrackingResource.instances)

    def test_failed_load_unloads_the_half_built_instance(self):
        store = _make_store({"TrackingResource": lambda: TrackingResource(fail_load=True)})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_ERROR
        # The worker retries init forever, so anything the failed load opened
        # has to be released instead of piling up per attempt.
        assert TrackingResource.instances[0].unloaded

    def test_reconfigure_bars_acquisitions_while_it_runs(self):
        observed = {}

        class ObservingResource(TrackingResource):
            def reconfigure(self, context):
                # The published generation is the one a batch would still be
                # acquiring without the fence; the target one is unacquirable
                # either way, since it is published only on success.
                observed["published"] = store.acquire([_reference("r", configuration_generation=0)])
                observed["target"] = store.acquire([_reference("r", configuration_generation=1)])
                super().reconfigure(context)

        store = _make_store({"TrackingResource": ObservingResource})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}),
        )
        assert outcome.status == RES_OK
        # No batch may start against half-applied parameters.
        assert observed["published"] is None
        assert observed["target"] is None
        assert store.acquire([_reference("r", configuration_generation=1)]) is not None

    def test_error_message_is_never_empty(self):
        class SilentlyFailingResource(TrackingResource):
            def load(self, context):
                raise ValueError()

        store = _make_store({"TrackingResource": SilentlyFailingResource})
        outcome = store.execute("r", COMMAND_INIT, _init_arg())
        assert outcome.status == RES_ERROR
        # An empty message would reach the worker as a resource failure with no
        # cause at all.
        assert outcome.error_message


class _ResourceReadingFunction(RowFunction):
    """Records the instance user code is handed, per message."""

    def __init__(self):
        self.seen = []

    def on_message(self, message, output, ctx):
        self.seen.append(ctx.get_resource("view"))


def _get_proto_module():
    try:
        from yt.yt.flow.library.python.companion._proto_compat import ensure_proto_imports

        ensure_proto_imports()
        from yt.flow.library.cpp.companion.proto import companion_service_pb2
        from yt.flow.library.cpp.common.proto import message_pb2

        class ProtoModule:
            TReqProcessBatch = companion_service_pb2.TReqProcessBatch
            TResponseData = companion_service_pb2.TResponseData
            TNewTimer = companion_service_pb2.TNewTimer
            TState = companion_service_pb2.TState
            TStateItem = companion_service_pb2.TStateItem
            TMessage = message_pb2.TMessage

        return ProtoModule
    except ImportError:
        pytest.skip("Proto modules not available")


class TestProcessBatchResourceFence:
    def _make_processor(self):
        TrackingResource.instances = []
        pipeline_ctx = PipelineContext()
        self.function = _ResourceReadingFunction()
        pipeline_ctx.register_computation(Computation(computation_id="mapper", process_function=self.function))
        pipeline_ctx.register_resource_class("TrackingResource", TrackingResource)
        return CompanionRequestProcessor(pipeline_ctx, JobContext())

    def _make_request(self, proto, configuration_generation=0, messages=0):
        request = proto.TReqProcessBatch()
        request.request_id.first = 1
        request.job_id.first = 2
        request.computation_id = "mapper"
        request.job_info.spec = b"{}"
        request.job_info.dynamic_spec = b"{}"
        stream = request.job_info.streams.add()
        stream.stream_id = "input"
        stream.stream_spec_id = 0
        stream.schema = b"[]"
        reference = request.job_info.companion_resources.add()
        reference.resource_id = "r"
        # INCARNATION_A = "1-2-3-4": Parts32[3]=1, [2]=2, [1]=3, [0]=4.
        reference.incarnation_id.first = (3 << 32) | 4
        reference.incarnation_id.second = (1 << 32) | 2
        reference.configuration_generation = configuration_generation
        reference.alias = "view"
        # Wire-protocol encoding of an empty UnversionedRow: version=0, value_count=0.
        empty_row = b"\x00\x00"
        for index in range(messages):
            message = request.messages.add()
            message.message.message_id = f"m-{index}"
            message.message.system_timestamp = 1
            message.message.stream_spec_id = 0
            message.message.payload = empty_row
            message.key = empty_row
        return request

    def test_uninitialized_resource_rejects_batch_in_band(self):
        proto = _get_proto_module()
        processor = self._make_processor()
        result = processor.process_batch(self._make_request(proto), proto)
        assert result["status"] == "RS_RESOURCE_NOT_INITIALIZED"

    def test_initialized_resource_is_visible_to_user_code(self):
        proto = _get_proto_module()
        processor = self._make_processor()
        outcome = processor.resource_store.execute("r", COMMAND_INIT, _init_arg())
        assert outcome.status == RES_OK

        result = processor.process_batch(self._make_request(proto, messages=2), proto)

        assert result["status"] == "RS_OK"
        # The batch must reach user code: a message-less batch would pass the
        # status check without ever calling get_resource.
        assert self.function.seen == [TrackingResource.instances[0]] * 2

    def test_stale_reference_rejects_batch_in_band(self):
        proto = _get_proto_module()
        processor = self._make_processor()
        assert processor.resource_store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        result = processor.process_batch(self._make_request(proto, configuration_generation=3), proto)
        assert result["status"] == "RS_RESOURCE_NOT_INITIALIZED"


class TestLifecycleSerialization:
    def test_contended_command_is_refused_as_retryable(self):
        loading = threading.Event()
        proceed = threading.Event()

        class BlockingResource(TrackingResource):
            def load(self, context):
                loading.set()
                proceed.wait(timeout=10)
                super().load(context)

        store = _make_store({"TrackingResource": BlockingResource})
        outcomes = []

        def run():
            outcomes.append(store.execute("r", COMMAND_INIT, _init_arg()))

        first = threading.Thread(target=run)
        first.start()
        assert loading.wait(timeout=10)
        try:
            # The RPC pool is small and the admission spans user code, so a second command
            # must not park a worker until the first hook returns.
            refused = store.execute("r", COMMAND_INIT, _init_arg())
        finally:
            proceed.set()
            first.join(timeout=10)

        # Retryable in-band: the worker re-sends init and converges.
        assert refused.status == RES_RESOURCE_NOT_INITIALIZED
        assert outcomes[0].status == RES_OK
        assert len(TrackingResource.instances) == 1

    def test_unload_drops_the_retired_context(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("dep")])).status == RES_OK
        entry = store._entries["r"]
        assert entry.context is not None

        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK

        # The entry outlives the instance, so a retained context would keep the retired
        # dependency instances and parsed parameters reachable for the process's life.
        assert entry.context is None


class TestLifecycleAdmission:
    def _gated_store(self, loading, proceed):
        class BlockingResource(TrackingResource):
            def load(self, context):
                loading.set()
                proceed.wait(timeout=10)
                super().load(context)

        return _make_store({"TrackingResource": BlockingResource})

    def test_contenders_never_park_on_the_running_command(self):
        loading = threading.Event()
        proceed = threading.Event()
        store = self._gated_store(loading, proceed)
        first = threading.Thread(target=lambda: store.execute("r", COMMAND_INIT, _init_arg()))
        first.start()
        assert loading.wait(timeout=10)

        try:
            # Every contender must answer while the first hook is still running: on the
            # companion's small RPC pool, parking here would starve unrelated calls.
            started = time.monotonic()
            outcomes = [store.execute("r", COMMAND_INIT, _init_arg()) for _ in range(3)]
            elapsed = time.monotonic() - started
        finally:
            proceed.set()
            first.join(timeout=10)

        assert [outcome.status for outcome in outcomes] == [RES_RESOURCE_NOT_INITIALIZED] * 3
        assert elapsed < 1.0
        assert len(TrackingResource.instances) == 1

    def test_unload_refused_admission_still_retires_the_instance(self):
        loading = threading.Event()
        proceed = threading.Event()
        store = self._gated_store(loading, proceed)
        outcomes = []
        first = threading.Thread(target=lambda: outcomes.append(store.execute("r", COMMAND_INIT, _init_arg())))
        first.start()
        assert loading.wait(timeout=10)

        # The worker delivers retirement once, so an unload that cannot be admitted must
        # not be dropped: the load in flight has to honour it instead of publishing.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        proceed.set()
        first.join(timeout=10)

        assert outcomes[0].status == RES_STALE_RESOURCE_INCARNATION
        assert _acquired_resource(store, _reference("r")) is None
        assert TrackingResource.instances[0].unloaded

    def test_failed_replacement_drops_the_retired_context(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("dep")])).status == RES_OK
        entry = store._entries["r"]
        assert entry.context is not None

        # A replacement that detaches and then fails leaves the entry serving nothing, so
        # it must not keep the retired instance's dependencies reachable either.
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2, resource_class="Missing"),
        )
        assert outcome.status == RES_RESOURCE_NOT_FOUND
        assert entry.context is None


class TestPendingRetirement:
    def test_unload_during_reconfigure_still_retires_the_instance(self):
        reconfiguring = threading.Event()
        proceed = threading.Event()

        class BlockingReconfigureResource(TrackingResource):
            def reconfigure(self, context):
                reconfiguring.set()
                proceed.wait(timeout=10)
                super().reconfigure(context)

        store = _make_store({"TrackingResource": BlockingReconfigureResource})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        outcomes = []
        reconfigure = threading.Thread(
            target=lambda: outcomes.append(
                store.execute("r", COMMAND_INIT, _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}))
            )
        )
        reconfigure.start()
        assert reconfiguring.wait(timeout=10)

        # Retirement arrives while the hook runs; republishing afterwards would leave the
        # instance hosted with nobody left to unload it.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        proceed.set()
        reconfigure.join(timeout=10)

        assert outcomes[0].status == RES_STALE_RESOURCE_INCARNATION
        assert _acquired_resource(store, _reference("r", configuration_generation=1)) is None
        assert TrackingResource.instances[0].unloaded

    def test_unload_during_reconfigure_keeps_no_metadata(self):
        reconfiguring = threading.Event()
        proceed = threading.Event()

        class BlockingReconfigureResource(TrackingResource):
            def reconfigure(self, context):
                reconfiguring.set()
                proceed.wait(timeout=10)
                super().reconfigure(context)

        store = _make_store({"TrackingResource": BlockingReconfigureResource})
        assert store.execute("r", COMMAND_INIT, _init_arg()).status == RES_OK
        entry = store._entries["r"]
        outcomes = []
        reconfigure = threading.Thread(
            target=lambda: outcomes.append(
                store.execute("r", COMMAND_INIT, _init_arg(configuration_generation=1, dynamic_parameters={"v": "2"}))
            )
        )
        reconfigure.start()
        assert reconfiguring.wait(timeout=10)

        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        proceed.set()
        reconfigure.join(timeout=10)

        # The interrupted commit must not restore what the retirement dropped: a retired
        # entry serving nothing would keep the context and the user's object graph
        # reachable for the process's life.
        assert outcomes[0].status == RES_STALE_RESOURCE_INCARNATION
        assert entry.context is None
        assert entry.applied_specs is None
        assert entry.dependency_references == []

    def test_late_unload_of_another_incarnation_does_not_erase_a_pending_one(self):
        loading = threading.Event()
        proceed = threading.Event()

        class BlockingResource(TrackingResource):
            def load(self, context):
                loading.set()
                proceed.wait(timeout=10)
                super().load(context)

        store = _make_store({"TrackingResource": BlockingResource})
        outcomes = []
        first = threading.Thread(target=lambda: outcomes.append(store.execute("r", COMMAND_INIT, _init_arg())))
        first.start()
        assert loading.wait(timeout=10)

        assert store.execute("r", COMMAND_UNLOAD, _unload_arg()).status == RES_OK
        # A late unload for a different incarnation must not displace the retirement the
        # loading incarnation still has to honour.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg(incarnation_id=INCARNATION_B)).status == RES_OK
        proceed.set()
        first.join(timeout=10)

        assert outcomes[0].status == RES_STALE_RESOURCE_INCARNATION
        assert _acquired_resource(store, _reference("r")) is None

    def test_same_incarnation_replacement_losing_a_dependency_drops_the_context(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        assert store.execute("r", COMMAND_INIT, _init_arg(dependencies=[_dependency_node("dep")])).status == RES_OK
        entry = store._entries["r"]
        assert entry.context is not None

        # Same incarnation, changed dependency references: the entry detaches its instance
        # and then fails on the unavailable dependency, so it must keep nothing of it.
        outcome = store.execute(
            "r",
            COMMAND_INIT,
            _init_arg(dependencies=[_dependency_node("absent")]),
        )
        assert outcome.status == RES_RESOURCE_NOT_INITIALIZED
        assert entry.context is None


class TestRetirementHandoff:
    def _store_recording_after_publish(self, unloads):
        """Store whose load records an unload after the entry is published but before the
        command releases it — the window a publish-gate check cannot cover.
        """

        class PublishingResource(TrackingResource):
            def load(self, context):
                super().load(context)
                unloads.append(True)

        return _make_store({"TrackingResource": PublishingResource})

    def test_unload_is_applied_without_admission_while_a_command_runs(self):
        loading = threading.Event()
        proceed = threading.Event()
        observed = {}

        class BlockingResource(TrackingResource):
            def load(self, context):
                loading.set()
                proceed.wait(timeout=10)
                super().load(context)

        store = _make_store({"TrackingResource": BlockingResource})
        first = threading.Thread(target=lambda: store.execute("r", COMMAND_INIT, _init_arg()))
        first.start()
        assert loading.wait(timeout=10)

        # Retirement is never deferred: it takes effect in its own call, even though an
        # init holds the entry, so there is no interval in which it could be dropped.
        outcome = store.execute("r", COMMAND_UNLOAD, _unload_arg())
        observed["retired_immediately"] = store._entries["r"].retired
        proceed.set()
        first.join(timeout=10)

        assert outcome.status == RES_OK
        assert observed["retired_immediately"] is True
        assert _acquired_resource(store, _reference("r")) is None
        assert TrackingResource.instances[0].unloaded

    def test_contended_unload_of_another_incarnation_is_not_recorded(self):
        loading = threading.Event()
        proceed = threading.Event()

        class BlockingResource(TrackingResource):
            def load(self, context):
                loading.set()
                proceed.wait(timeout=10)
                super().load(context)

        store = _make_store({"TrackingResource": BlockingResource})
        first = threading.Thread(target=lambda: store.execute("r", COMMAND_INIT, _init_arg()))
        first.start()
        assert loading.wait(timeout=10)

        # Mismatching unload: a no-op for the admitted path, so recording it would fence a
        # future successor — the contract test_mismatching_unload_does_not_fence... pins.
        assert store.execute("r", COMMAND_UNLOAD, _unload_arg(incarnation_id=INCARNATION_B)).status == RES_OK
        proceed.set()
        first.join(timeout=10)

        assert _acquired_resource(store, _reference("r")) is not None
        # And the successor is still allowed to initialize later.
        assert (
            store.execute("r", COMMAND_INIT, _init_arg(incarnation_id=INCARNATION_B, incarnation_generation=2)).status
            == RES_OK
        )

    def test_failed_replacement_keeps_the_static_spec_history(self):
        store = _make_store()
        assert store.execute("dep", COMMAND_INIT, _init_arg()).status == RES_OK
        original = _init_arg(parameters={"a": "1"}, dependencies=[_dependency_node("dep")])
        assert store.execute("r", COMMAND_INIT, original).status == RES_OK

        # The replacement detaches and then fails on the missing dependency.
        assert (
            store.execute(
                "r", COMMAND_INIT, _init_arg(parameters={"a": "1"}, dependencies=[_dependency_node("absent")])
            ).status
            == RES_RESOURCE_NOT_INITIALIZED
        )

        # A retry may not smuggle in a changed static spec under the same incarnation: the
        # failure released the retired instance, not the incarnation's spec history.
        outcome = store.execute("r", COMMAND_INIT, _init_arg(parameters={"a": "2"}))
        assert outcome.status == RES_ERROR
        assert "Static resource spec changed" in outcome.error_message
