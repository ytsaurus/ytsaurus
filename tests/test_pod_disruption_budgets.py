from .conftest import (
    DEFAULT_POD_SET_SPEC,
    are_pods_assigned,
    assert_over_time,
    attach_pod_set_to_disruption_budget,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
)

from yp.common import YtResponseError, YpNoSuchObjectError, wait

from yt.yson import YsonEntity

from yt.wrapper.retries import run_with_retries

from yt.packages.six.moves import xrange

import pytest
import random
import time as time_module


def time_struct_to_timestamp(time_struct):
    seconds = time_struct.get("seconds", 0)
    nanos = time_struct.get("nanos", 0)
    nano_multiplier = 10 ** 9
    assert 0 <= seconds
    assert 0 <= nanos < nano_multiplier
    return seconds * nano_multiplier + nanos


@pytest.mark.usefixtures("yp_env_configurable")
class TestPodDisruptionBudgets(object):
    YP_MASTER_CONFIG = dict(
        scheduler=dict(
            disable_stage=dict(
                run_pod_disruption_budget_controller=True,
            ),
        ),
    )

    def test_attributes(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        pod_disruption_budget_id = yp_client.create_object("pod_disruption_budget")

        attributes = yp_client.get_object(
            "pod_disruption_budget",
            pod_disruption_budget_id,
            selectors=[
                "/spec",
                "/status",
                "/spec/max_pods_unavailable",
                "/spec/max_pod_disruptions_between_syncs",
                "/status/allowed_pod_disruptions",
                "/status/last_update_message",
                "/status/last_update_time",
            ],
        )
        assert attributes[0] == dict()
        assert attributes[1]["allowed_pod_disruptions"] == 0 and \
            attributes[1]["last_update_time"]["seconds"] > 0 and \
            len(attributes[1]["last_update_message"]) > 0
        assert isinstance(attributes[2], YsonEntity)
        assert isinstance(attributes[3], YsonEntity)
        assert attributes[4] == 0
        assert len(attributes[5]) > 0
        assert attributes[6]["seconds"] > 0

        yp_client.update_object(
            "pod_disruption_budget",
            pod_disruption_budget_id,
            set_updates=[dict(
                path="/spec/max_pods_unavailable",
                value=1,
            )],
        )
        assert yp_client.get_object(
            "pod_disruption_budget",
            pod_disruption_budget_id,
            selectors=["/spec/max_pods_unavailable"],
        )[0] == 1

        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "pod_disruption_budget",
                pod_disruption_budget_id,
                set_updates=[dict(
                    path="/status/allowed_pod_disruptions",
                    value=1,
                )],
            )

        for field_name in ("max_pods_unavailable", "max_pod_disruptions_between_syncs"):
            yp_client.create_object(
                "pod_disruption_budget",
                attributes=dict(spec={field_name: 1}),
            )
            with pytest.raises(YtResponseError):
                yp_client.create_object(
                    "pod_disruption_budget",
                    attributes=dict(spec={field_name: -1}),
                )

            with pytest.raises(YtResponseError):
                yp_client.update_object(
                    "pod_disruption_budget",
                    pod_disruption_budget_id,
                    set_updates=[dict(
                        path="/spec/" + field_name,
                        value=-1,
                    )],
                )

    def test_status_update_time_monotonicity(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        class Time(object):
            def _get_time(self):
                return yp_client.get_object(
                    "pod_disruption_budget",
                    self._pod_disruption_budget_id,
                    selectors=["/status/last_update_time"],
                )[0]

            def __init__(self, pod_disruption_budget_id):
                self._pod_disruption_budget_id = pod_disruption_budget_id
                self._time = self._get_time()

            def validate_equality(self):
                assert self._time == self._get_time()

            def validate_increase(self):
                new_time = self._get_time()
                assert time_struct_to_timestamp(self._time) < time_struct_to_timestamp(new_time)
                self._time = new_time

        def wait_for_time_increase():
            time_module.sleep(5)

        pod_disruption_budget_id = yp_client.create_object("pod_disruption_budget")
        time = Time(pod_disruption_budget_id)

        pod_set_id = yp_client.create_object("pod_set")
        time.validate_equality()

        wait_for_time_increase()
        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[dict(
                path="/spec/pod_disruption_budget_id",
                value=pod_disruption_budget_id,
            )],
        )

        time.validate_increase()

        pod_disruption_budget_id2 = yp_client.create_object("pod_disruption_budget")
        time2 = Time(pod_disruption_budget_id2)

        wait_for_time_increase()
        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[dict(
                path="/spec/pod_disruption_budget_id",
                value=pod_disruption_budget_id2,
            )],
        )

        time.validate_increase()
        time2.validate_increase()

        wait_for_time_increase()
        yp_client.remove_object("pod_set", pod_set_id)

        time.validate_equality()
        time2.validate_increase()

    def test_permission_to_create(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        yp_client.create_object("user", attributes=dict(meta=dict(id="u1")))
        yp_env_configurable.sync_access_control()

        with yp_env_configurable.yp_instance.create_client(config=dict(user="u1")) as yp_client1:
            with pytest.raises(YtResponseError):
                yp_client1.create_object("pod_disruption_budget")

            yp_client.update_object(
                "schema",
                "pod_disruption_budget",
                set_updates=[dict(
                    path="/meta/acl/end",
                    value=dict(
                        permissions=["create"],
                        subjects=["u1"],
                        action="allow",
                    ),
                )],
            )
            yp_env_configurable.sync_access_control()

            yp_client1.create_object("pod_disruption_budget")

    def test_pod_sets_budgeting(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        # Pod disruption budget without budgeting pod sets can be removed.
        pod_disruption_budget_id = yp_client.create_object("pod_disruption_budget")
        yp_client.remove_object("pod_disruption_budget", pod_disruption_budget_id)
        with pytest.raises(YpNoSuchObjectError):
            yp_client.get_object(
                "pod_disruption_budget",
                pod_disruption_budget_id,
                selectors=["/spec/max_pods_unavailable"],
            )

        def _create_pod_set(pod_disruption_budget_id=None):
            if pod_disruption_budget_id is None:
                attributes = None
            else:
                attributes = dict(spec=dict(pod_disruption_budget_id=pod_disruption_budget_id))
            return yp_client.create_object("pod_set", attributes=attributes)

        def _get_pod_set_disruption_budget(pod_set_id):
            return yp_client.get_object(
                "pod_set",
                pod_set_id,
                selectors=["/spec/pod_disruption_budget_id"],
            )[0]

        def _update_pod_set_disruption_budget(pod_set_id, pod_disruption_budget_id):
            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[dict(
                    path="/spec/pod_disruption_budget_id",
                    value=pod_disruption_budget_id,
                )],
            )

        # Pod disruption budget with budgeting pod sets cannot be removed.
        pod_disruption_budget_id = yp_client.create_object("pod_disruption_budget")
        for _ in xrange(2):
            pod_set_id = _create_pod_set(pod_disruption_budget_id)
            with pytest.raises(YtResponseError):
                yp_client.remove_object("pod_disruption_budget", pod_disruption_budget_id)
            assert _get_pod_set_disruption_budget(pod_set_id) == pod_disruption_budget_id

        # Pod set cannot be budgeted by non existent pod disruption budget.
        nonexistent_id = "nonexistent"
        with pytest.raises(YpNoSuchObjectError):
            _create_pod_set(nonexistent_id)

        # Pod set disruption budget is optional.
        pod_set_id = _create_pod_set()
        assert _get_pod_set_disruption_budget(pod_set_id) == ""

        # Pod set disruption budget can be updated.
        _update_pod_set_disruption_budget(pod_set_id, pod_disruption_budget_id)
        _update_pod_set_disruption_budget(pod_set_id, yp_client.create_object("pod_disruption_budget"))


def get_status(yp_client, pod_disruption_budget_id, timestamp=None):
    return yp_client.get_object(
        "pod_disruption_budget",
        pod_disruption_budget_id,
        selectors=["/status"],
        timestamp=timestamp,
    )[0]


def get_last_update_time(*args, **kwargs):
    return time_struct_to_timestamp(get_status(*args, **kwargs)["last_update_time"])


def get_allowed_pod_disruptions(*args, **kwargs):
    return get_status(*args, **kwargs)["allowed_pod_disruptions"]


def validate_controller_liveness(yp_client):
    pod_disruption_budget_id = yp_client.create_object(
        "pod_disruption_budget",
        attributes=dict(spec=dict(
            max_pods_unavailable=5,
            max_pod_disruptions_between_syncs=2,
        )),
    )
    wait(lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == 2)
    last_update_time = get_last_update_time(yp_client, pod_disruption_budget_id)
    assert_over_time(lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == 2)
    wait(lambda: get_last_update_time(yp_client, pod_disruption_budget_id) > last_update_time)


def validate_controller_lifelessness(yp_client):
    pod_disruption_budget_id = yp_client.create_object("pod_disruption_budget")
    last_update_time = get_last_update_time(yp_client, pod_disruption_budget_id)
    assert_over_time(lambda: get_last_update_time(yp_client, pod_disruption_budget_id) == last_update_time)


def request_pod_eviction_forcefully(yp_client, pod_id, validate_disruption_budget=False):
    def impl():
        return yp_client.request_pod_eviction(
            pod_id,
            "Test",
            validate_disruption_budget=validate_disruption_budget,
        )
    # Bypass conflicts with pod disruption budget controller.
    return run_with_retries(impl, exceptions=(YtResponseError,))


def create_pods(yp_client, pod_counts, pod_disruption_budget_id):
    assert len(pod_counts) > 0
    pod_set_ids = []
    pod_ids = []
    for pod_count in pod_counts:
        assert pod_count > 0

        pod_set_id = yp_client.create_object("pod_set", attributes=dict(spec=DEFAULT_POD_SET_SPEC))
        attach_pod_set_to_disruption_budget(yp_client, pod_set_id, pod_disruption_budget_id)

        current_pod_ids = []
        for _ in xrange(pod_count):
            current_pod_ids.append(create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(enable_scheduling=True),
            ))

        pod_set_ids.append(pod_set_id)
        pod_ids.extend(current_pod_ids)
    return pod_set_ids, pod_ids


@pytest.mark.usefixtures("yp_env")
class TestPodDisruptionBudgetController(object):
    def test_scheduler_disable_stage_reconfiguration(self, yp_env):
        yp_client = yp_env.yp_client

        validate_controller_liveness(yp_client)
        yp_env.set_cypress_config_patch(dict(scheduler=dict(disable_stage=dict(
            run_pod_disruption_budget_controller=True,
        ))))
        orchid = yp_env.create_orchid_client()
        def is_controller_disabled():
            config = orchid.get(orchid.get_instances()[0], "/config")
            return config["scheduler"].get("disable_stage", {}) \
                .get("run_pod_disruption_budget_controller", None) == True
        wait(is_controller_disabled)
        validate_controller_lifelessness(yp_client)

    def test_options_reconfiguration(self, yp_env):
        yp_client = yp_env.yp_client

        updates_per_iteration = 3
        update_concurrency = 1
        yp_env.set_cypress_config_patch(dict(scheduler=dict(pod_disruption_budget_controller=dict(
            updates_per_iteration=updates_per_iteration,
            update_concurrency=update_concurrency,
        ))))
        orchid = yp_env.create_orchid_client()
        def is_controller_configured():
            config = orchid.get(orchid.get_instances()[0], "/config")
            controller_config = config["scheduler"]["pod_disruption_budget_controller"]
            return controller_config["updates_per_iteration"] == updates_per_iteration and \
                controller_config["update_concurrency"] == update_concurrency
        wait(is_controller_configured)
        validate_controller_liveness(yp_client)

    def test_not_assigned_pods(self, yp_env):
        yp_client = yp_env.yp_client
        pod_disruption_budget_id = yp_client.create_object(
            "pod_disruption_budget",
            attributes=dict(spec=dict(
                max_pods_unavailable=10,
                max_pod_disruptions_between_syncs=3,
            )),
        )
        _, _ = create_pods(yp_client, (1, 2, 2, 1), pod_disruption_budget_id)
        def validate(lower_bound, upper_bound):
            def check():
                value = get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id)
                return lower_bound <= value <= upper_bound
            wait(check)
            assert_over_time(check)
        validate(3, 3)
        _, _ = create_pods(yp_client, (1, 1), pod_disruption_budget_id)
        validate(2, 2)
        _, _ = create_pods(yp_client, (10, ), pod_disruption_budget_id)
        validate(0, 0)
        create_nodes(yp_client, 10)
        # At least 10 pods should be assigned.
        validate(2, 3)

    def test_evicted_pods(self, yp_env):
        yp_client = yp_env.yp_client

        parameters = (
            (3, 2, 5),
            (5, 6, 4),
        )
        # i-th value of each list corresponds to the expected value of allowed_pod_disruptions after i requested evictions.
        expected_value_per_eviction_per_case = (
            (2, 2, 1, 0, 0, 0),
            (5, 4, 3, 2, 1),
        )
        for case_index in xrange(len(parameters)):
            max_pods_unavailable, max_pod_disruptions_between_syncs, pod_count = parameters[case_index]
            expected_value_per_eviction = expected_value_per_eviction_per_case[case_index]

            assert len(expected_value_per_eviction) == pod_count + 1

            pod_disruption_budget_id = yp_client.create_object(
                "pod_disruption_budget",
                attributes=dict(spec=dict(
                    max_pods_unavailable=max_pods_unavailable,
                    max_pod_disruptions_between_syncs=max_pod_disruptions_between_syncs,
                )),
            )

            assert pod_count >= 2
            first_pod_set_pod_count = pod_count // 2
            second_pod_set_pod_count = pod_count - first_pod_set_pod_count

            pod_set_ids, pod_ids = create_pods(
                yp_client,
                pod_counts=(first_pod_set_pod_count, second_pod_set_pod_count),
                pod_disruption_budget_id=pod_disruption_budget_id,
            )
            assert len(pod_ids) == pod_count

            create_nodes(yp_client, 10)
            wait(lambda: are_pods_assigned(yp_client, pod_ids))

            def check(value):
                return get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == value

            wait(lambda: check(expected_value_per_eviction[0]))

            random.shuffle(pod_ids)
            for eviction_index in xrange(len(pod_ids)):
                request_pod_eviction_forcefully(yp_client, pod_ids[eviction_index])
                value = expected_value_per_eviction[eviction_index + 1]
                wait(lambda: check(value))
                assert_over_time(lambda: check(value))

    def test_pod_disruption_budget_controller_profiling(self, yp_env):
        yp_client = yp_env.yp_client
        orchid = yp_env.create_orchid_client()

        def _prepare_pod_set_with_disruption_budget(pod_count):
            pod_disruption_budget_id = yp_client.create_object(
                "pod_disruption_budget",
                attributes=dict(spec=dict(
                    max_pods_unavailable=2,
                    max_pod_disruptions_between_syncs=1,
                )),
            )
            pod_set_id = create_pod_set(yp_client)
            attach_pod_set_to_disruption_budget(yp_client, pod_set_id, pod_disruption_budget_id)
            pod_ids = [
                create_pod_with_boilerplate(
                    yp_client,
                    pod_set_id,
                    spec=dict(enable_scheduling=True),
                )
                for _ in xrange(pod_count)
            ]
            return pod_set_id, pod_ids

        start_time = time_module.time()

        def _get_update_lags():
            master_instance = orchid.get_instances()[0]
            samples = orchid.get(master_instance, "/profiling/scheduler/pod_disruption_budget_controller/update_lag")
            return [sample["value"] for sample in samples]

        NODE_COUNT = 2
        POD_SET_COUNT = 10
        POD_COUNT_PER_POD_SET = 5

        node_ids = create_nodes(yp_client, NODE_COUNT)
        pod_set_ids = []
        pod_ids = []

        for _ in xrange(POD_SET_COUNT):
            new_pod_set_id, new_pod_ids = _prepare_pod_set_with_disruption_budget(POD_COUNT_PER_POD_SET)
            pod_set_ids.append(new_pod_set_id)
            pod_ids.extend(new_pod_ids)

        assert len(pod_set_ids) == POD_SET_COUNT
        assert len(pod_ids) == POD_SET_COUNT * POD_COUNT_PER_POD_SET

        wait(lambda: are_pods_assigned(yp_client, pod_ids))
        time_module.sleep(30)

        update_lags = _get_update_lags()

        update_lag_silly_threshold = (time_module.time() - start_time) * 1000000
        assert all(map(lambda value: value >= 0 and value <= update_lag_silly_threshold, update_lags))

        positive_update_lags = list(filter(lambda value: value > 0, update_lags))
        assert len(positive_update_lags) > 0


def get_pod_eviction_state(yp_client, pod_id):
    return yp_client.get_object(
        "pod",
        pod_id,
        selectors=["/status/eviction/state"],
    )[0]


@pytest.mark.usefixtures("yp_env")
class TestPodDisruptionBudgetIntegrationWithEvictionRequest(object):
    def test_request_eviction_by_hfsm_do_not_validate_budget(self, yp_env):
        yp_client = yp_env.yp_client

        pod_disruption_budget_id = yp_client.create_object(
            "pod_disruption_budget",
            attributes=dict(spec=dict(
                max_pods_unavailable=0,
                max_pod_disruptions_between_syncs=0,
            )),
        )

        node_id = create_nodes(yp_client, 1)[0]
        pod_counts = (1,)
        _, pod_ids = create_pods(yp_client, pod_counts, pod_disruption_budget_id)
        wait(lambda: are_pods_assigned(yp_client, pod_ids))
        assert_over_time(lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == 0)

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")
        wait(lambda: get_pod_eviction_state(yp_client, pod_ids[0]) == "requested")

    def test_validate_disruption_budget_option(self, yp_env):
        yp_client = yp_env.yp_client

        # Expect to successfully request pod eviction with and without
        # budget validation for the given number of iterations,
        # and to exceed disruption budget after that.
        iteration_count = 1
        pod_disruption_budget_limit = iteration_count * 2

        pod_disruption_budget_id = yp_client.create_object(
            "pod_disruption_budget",
            attributes=dict(spec=dict(
                max_pods_unavailable=pod_disruption_budget_limit,
                max_pod_disruptions_between_syncs=pod_disruption_budget_limit,
            )),
        )
        pod_count = iteration_count * 2 + 1
        node_count = pod_count
        create_nodes(yp_client, node_count)
        _, pod_ids = create_pods(yp_client, (pod_count,), pod_disruption_budget_id)
        wait(lambda: are_pods_assigned(yp_client, pod_ids))
        wait(lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == pod_disruption_budget_limit)

        for i in xrange(2 * iteration_count):
            validate_disruption_budget = bool(i % 2)

            request_pod_eviction_forcefully(
                yp_client,
                pod_ids[i],
                validate_disruption_budget=validate_disruption_budget,
            )
            assert get_pod_eviction_state(yp_client, pod_ids[i]) == "requested"
            assert_over_time(
                lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) \
                    == pod_disruption_budget_limit - (i + 1),
            )

        extra_pod_id = pod_ids[2 * iteration_count]

        with pytest.raises(YtResponseError):
            request_pod_eviction_forcefully(
                yp_client,
                extra_pod_id,
                validate_disruption_budget=True,
            )

        request_pod_eviction_forcefully(
            yp_client,
            extra_pod_id,
            validate_disruption_budget=False,
        )
        assert get_pod_eviction_state(yp_client, extra_pod_id) == "requested"
        assert_over_time(lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == 0)

    def test_budget_update(self, yp_env):
        yp_client = yp_env.yp_client

        pod_disruption_budget_id = yp_client.create_object(
            "pod_disruption_budget",
            attributes=dict(spec=dict(
                max_pods_unavailable=3,
                max_pod_disruptions_between_syncs=3,
            )),
        )

        create_nodes(yp_client, 1)
        pod_counts = (1,)
        _, pod_ids = create_pods(yp_client, pod_counts, pod_disruption_budget_id)
        wait(lambda: are_pods_assigned(yp_client, pod_ids))
        wait(lambda: get_allowed_pod_disruptions(yp_client, pod_disruption_budget_id) == 3)

        commit_timestamp = request_pod_eviction_forcefully(yp_client, pod_ids[0])["commit_timestamp"]
        assert get_allowed_pod_disruptions(
            yp_client,
            pod_disruption_budget_id,
            timestamp=commit_timestamp,
        ) == 2
