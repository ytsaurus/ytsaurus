from base import ClickHouseTestBase, Clique, QueryFailedError

from helpers import get_scheduling_options

from yt_commands import (get, write_table, authors, raises_yt_error, abort_job, update_op_parameters, print_debug,
                         create, sync_create_cells, create_user, create_group, add_member)

from yt.common import wait

from yt_helpers import Profiler

import yt.environment.init_operation_archive as init_operation_archive

import time
import threading
import pytest


class TestClickHouseHttpProxy(ClickHouseTestBase):
    DELTA_PROXY_CONFIG = {
        "clickhouse": {
            "operation_cache": {
                "refresh_time": 100,
            },
            "permission_cache": {
                "refresh_time": 100,
                "expire_after_failed_update_time": 100,
            },
            "force_enqueue_profiling": True,
        },
    }

    def setup(self):
        self._setup()

    def _get_proxy_metric(self, metric_name):
        return Profiler.at_proxy(self.Env.get_http_proxy_address()).counter(metric_name)

    @authors("evgenstf")
    def test_instance_choice(self):
        with Clique(5, spec={"alias": "*test_alias"}) as clique:
            for job_cookie in range(5):
                proxy_response = clique.make_query_via_proxy(
                    "select * from system.clique", database="*test_alias@" + str(job_cookie)
                )
                for instance_response in proxy_response:
                    assert (
                        instance_response["self"] == 1
                        if instance_response["job_cookie"] == job_cookie
                        else instance_response["self"] == 0
                    )

                proxy_response = clique.make_query_via_proxy(
                    "select * from system.clique", database=clique.op.id + "@" + str(job_cookie)
                )
                for instance_response in proxy_response:
                    assert (
                        instance_response["self"] == 1
                        if instance_response["job_cookie"] == job_cookie
                        else instance_response["self"] == 0
                    )

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select * from system.clique", database="*test_alias@aaa")

    @authors("dakovalkov")
    def test_http_proxy(self):
        with Clique(1) as clique:
            proxy_response = clique.make_query_via_proxy("select * from system.clique")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response == response

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 1

            clique.resize(0, [jobs[0]])
            clique.resize(1)

            proxy_response = clique.make_query_via_proxy("select * from system.clique")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response == response

    @authors("dakovalkov")
    def test_ban_dead_instance_in_proxy(self):
        patch = {
            "yt": {
                "discovery": {
                    # Set big value to prevent unlocking node.
                    "transaction_timeout": 1000000,
                }
            }
        }

        cache_missed_count = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed_count")
        force_update_count = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, config_patch=patch) as clique:
            wait(lambda: clique.get_active_instance_count() == 2)

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            abort_job(jobs[0])
            wait(lambda: len(clique.op.get_running_jobs()) == 1)

            for instance in clique.get_active_instances():
                if str(instance) == jobs[0]:
                    continue
                else:
                    assert clique.make_direct_query(instance, "select 1") == [{"1": 1}]

            proxy_responses = []
            for i in range(50):
                print_debug("Iteration:", i)
                proxy_responses += [clique.make_query_via_proxy("select 1", full_response=True)]
                assert proxy_responses[i].status_code == 200
                assert proxy_responses[i].json()["data"] == proxy_responses[i - 1].json()["data"]
                time.sleep(0.05)
                if banned_count.get_delta(verbose=True) == 1:
                    break

            assert proxy_responses[0].json()["data"] == [{"1": 1}]
            assert clique.get_active_instance_count() == 2

        assert cache_missed_count.get_delta(verbose=True) == 1
        assert force_update_count.get_delta(verbose=True) == 1
        assert banned_count.get_delta(verbose=True) == 1

    @authors("dakovalkov")
    def test_ban_stopped_instance_in_proxy(self):
        patch = {
            "interruption_graceful_timeout": 100000,
        }

        cache_missed_count = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed_count")
        force_update_count = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, config_patch=patch) as clique:
            # Add clique into the cache.
            proxy_responses = []
            proxy_responses += [clique.make_query_via_proxy("select 1", full_response=True)]

            instances = clique.get_active_instances()
            assert len(instances) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            self._signal_instance(instances[0].attributes["pid"], "INT")

            with raises_yt_error(QueryFailedError):
                clique.make_direct_query(instances[0], "select 1")
            assert clique.make_direct_query(instances[1], "select 1") == [{"1": 1}]

            for i in range(50):
                print_debug("Iteration:", i)
                proxy_responses += [clique.make_query_via_proxy("select 1", full_response=True)]
                assert proxy_responses[i + 1].status_code == 200
                assert proxy_responses[i].json()["data"] == proxy_responses[i + 1].json()["data"]
                time.sleep(0.05)
                if banned_count.get_delta(verbose=True) == 1:
                    break

            assert proxy_responses[0].json()["data"] == [{"1": 1}]
            assert clique.get_active_instance_count() == 1

        assert cache_missed_count.get_delta(verbose=True) == 1
        assert force_update_count.get_delta(verbose=True) == 1
        assert banned_count.get_delta(verbose=True) == 1

    @authors("dakovalkov")
    @pytest.mark.skipif(True, reason="whatever")
    def test_clique_availability(self):
        create("table", "//tmp/table", attributes={"schema": [{"name": "i", "type": "int64"}]})
        write_table("//tmp/table", [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}])
        patch = {
            "interruption_graceful_timeout": 600,
        }

        cache_missed_counter = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed_count")
        force_update_counter = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, max_failed_job_count=2, config_patch=patch) as clique:
            running = True

            def pinger():
                while running:
                    full_response = clique.make_query_via_proxy('select * from "//tmp/table"', full_response=True)
                    print_debug(full_response)
                    print_debug(full_response.json())
                    assert full_response.status_code == 200
                    response = sorted(full_response.json()["data"])
                    assert response == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]
                    time.sleep(0.1)

            ping_thread = threading.Thread(target=pinger)
            ping_thread.start()
            time.sleep(1)

            instances = clique.get_active_instances()
            assert len(instances) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))

            self._signal_instance(instances[0].attributes["pid"], "INT")

            wait(lambda: clique.get_active_instance_count() == 1, iter=10)
            clique.resize(2)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 2
            assert new_instances != instances

            assert ping_thread.is_alive()
            running = False
            ping_thread.join()

        assert cache_missed_counter.get_delta(verbose=True) == 1
        assert force_update_counter.get_delta(verbose=True) == 1
        assert banned_count.get_delta(verbose=True) == 1

    @authors("max42")
    def test_database_specification(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        with Clique(1, spec={"alias": "*alias"}) as clique:
            assert clique.make_query_via_proxy("select 1 as a", database="*alias")[0] == {"a": 1}
            assert clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0] == {"a": 1}

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*alia")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="**")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="")

            with raises_yt_error(1915):  # NoSuchOperation
                clique.make_query_via_proxy("select 1 as a", database="1-2-3-4")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="1-2-3-x")

            clique.op.suspend()

            wait(lambda: get(clique.op.get_path() + "/@suspended"))
            time.sleep(1)

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*alias")

            clique.op.resume()

            wait(lambda: not get(clique.op.get_path() + "/@suspended"))
            time.sleep(1)

            assert clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0] == {"a": 1}

        wait(lambda: clique.get_active_instance_count() == 0)

        time.sleep(1)

        with raises_yt_error(QueryFailedError):
            assert clique.make_query_via_proxy("select 1 as a", database="*alias")[0] == {"a": 1}

        with raises_yt_error(QueryFailedError):
            assert clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0] == {"a": 1}

    @authors("dakovalkov")
    def test_expect_100_continue(self):
        headers = {"Expect": "100-continue"}
        with Clique(1) as clique:
            assert clique.make_query_via_proxy("select 1 as a", headers=headers)[0] == {"a": 1}

    @authors("max42")
    def test_operation_acl_validation(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_group("g")
        add_member("u1", "g")
        add_member("u2", "g")

        allow_g = {"subjects": ["g"], "action": "allow", "permissions": ["read"]}
        deny_u2 = {"subjects": ["u2"], "action": "deny", "permissions": ["read"]}

        with Clique(1, spec={"alias": "*alias", "acl": [allow_g, deny_u2]}) as clique:
            for user in ("u1", "root"):
                assert clique.make_query_via_proxy("select 1 as a", user=user)[0] == {"a": 1}
            for user in ("u2", "u3"):
                with raises_yt_error(901):  # AuthorizationError
                    assert clique.make_query_via_proxy("select 1 as a", user=user)

            update_op_parameters(clique.op.id, parameters={"acl": [allow_g]})
            time.sleep(1)

            for user in ("u1", "u2", "root"):
                assert clique.make_query_via_proxy("select 1 as a", user=user)[0] == {"a": 1}
            for user in ("u3",):
                with raises_yt_error(901):  # AuthorizationError
                    assert clique.make_query_via_proxy("select 1 as a", user=user)

            update_op_parameters(clique.op.id, parameters={"acl": []})
            time.sleep(1)

            for user in ("root",):
                assert clique.make_query_via_proxy("select 1 as a", user=user)[0] == {"a": 1}
            for user in ("u1", "u2", "u3"):
                with raises_yt_error(901):  # AuthorizationError
                    assert clique.make_query_via_proxy("select 1 as a", user=user)
