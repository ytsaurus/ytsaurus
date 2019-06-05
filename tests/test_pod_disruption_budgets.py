from yp.common import YtResponseError, YpNoSuchObjectError

from yt.yson import YsonEntity

from yt.packages.six.moves import xrange

import pytest
import time as time_module


@pytest.mark.usefixtures("yp_env")
class TestPodDisruptionBudgets(object):
    def test_attributes(self, yp_env):
        yp_client = yp_env.yp_client

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

    def test_status_update_time_monotonicity(self, yp_env):
        yp_client = yp_env.yp_client

        class Time(object):
            def _get_time(self):
                return yp_client.get_object(
                    "pod_disruption_budget",
                    self._pod_disruption_budget_id,
                    selectors=["/status/last_update_time"],
                )[0]

            def _get_time_pair(self, time):
                return (time.get("seconds", 0), time.get("nanos", 0))

            def __init__(self, pod_disruption_budget_id):
                self._pod_disruption_budget_id = pod_disruption_budget_id
                self._time = self._get_time()

            def validate_equality(self):
                assert self._time == self._get_time()

            def validate_increase(self):
                new_time = self._get_time()
                assert self._get_time_pair(self._time) < self._get_time_pair(new_time)
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

    def test_permission_to_create(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes=dict(meta=dict(id="u1")))
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config=dict(user="u1")) as yp_client1:
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
            yp_env.sync_access_control()

            yp_client1.create_object("pod_disruption_budget")

    def test_pod_sets_budgeting(self, yp_env):
        yp_client = yp_env.yp_client

        # Pod disruption budget without budgeting pod sets can be removed.
        pod_disruption_budget_id = yp_client.create_object("pod_disruption_budget")
        yp_client.remove_object("pod_disruption_budget", pod_disruption_budget_id)
        with pytest.raises(YpNoSuchObjectError):
            yp_client.get_object(
                "pod_disruption_budget",
                pod_disruption_budget_id,
                selectors=["/spec/max_pods_unavailable"],
            )

        def create_pod_set(pod_disruption_budget_id=None):
            if pod_disruption_budget_id is None:
                attributes = None
            else:
                attributes = dict(spec=dict(pod_disruption_budget_id=pod_disruption_budget_id))
            return yp_client.create_object("pod_set", attributes=attributes)

        def get_pod_set_disruption_budget(pod_set_id):
            return yp_client.get_object(
                "pod_set",
                pod_set_id,
                selectors=["/spec/pod_disruption_budget_id"],
            )[0]

        def update_pod_set_disruption_budget(pod_set_id, pod_disruption_budget_id):
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
            pod_set_id = create_pod_set(pod_disruption_budget_id)
            with pytest.raises(YtResponseError):
                yp_client.remove_object("pod_disruption_budget", pod_disruption_budget_id)
            assert get_pod_set_disruption_budget(pod_set_id) == pod_disruption_budget_id

        # Pod set cannot be budgeted by non existent pod disruption budget.
        nonexistent_id = "nonexistent"
        with pytest.raises(YpNoSuchObjectError):
            create_pod_set(nonexistent_id)

        # Pod set disruption budget is optional.
        pod_set_id = create_pod_set()
        assert get_pod_set_disruption_budget(pod_set_id) == ""

        # Pod set disruption budget can be updated.
        update_pod_set_disruption_budget(pod_set_id, pod_disruption_budget_id)
        update_pod_set_disruption_budget(pod_set_id, yp_client.create_object("pod_disruption_budget"))
