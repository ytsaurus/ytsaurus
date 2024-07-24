from base import ClickHouseTestBase, Clique, QueryFailedError

from helpers import get_scheduling_options

from yt_commands import (get, write_table, authors, raises_yt_error, abort_job, update_op_parameters, print_debug,
                         create, sync_create_cells, create_user, create_group, add_member, ls,
                         create_access_control_object_namespace, create_access_control_object, make_ace, set as yt_set)

from yt.common import wait

from yt_helpers import profiler_factory, write_log_barrier, read_structured_log

import yt.environment.init_operations_archive as init_operations_archive

from yt_env_setup import Restarter, SCHEDULERS_SERVICE

import os
import pytest
import signal
import threading
import time
import random
import string


def _job_id_of_instance_picked_for_the_query(clique: Clique, **kwargs) -> str:
    return clique.make_query_via_proxy(
        "select job_id from system.clique where self = 1", **kwargs)[0]["job_id"]


def _job_cookie_of_instance_picked_for_the_query(clique: Clique, **kwargs) -> int:
    return clique.make_query_via_proxy(
        "select job_cookie from system.clique where self = 1", **kwargs)[0]["job_cookie"]


def _generate_session_id() -> str:
    return ''.join(random.choices(string.ascii_lowercase, k=10))


class TestClickHouseHttpProxy(ClickHouseTestBase):
    ENABLE_TLS = True
    ENABLE_CHYT_HTTPS_PROXIES = True

    DELTA_PROXY_CONFIG = {
        "clickhouse": {
            "operation_cache": {
                "refresh_time": 100,
            },
            "permission_cache": {
                "refresh_time": 100,
                "expire_after_failed_update_time": 100,
            },
            "operation_id_update_period": 100,
            "populate_user_with_token": True,
        },
    }

    @classmethod
    def setup_class(cls, test_name=None, run_id=None):
        super().setup_class(test_name=test_name, run_id=run_id)
        Clique.proxy_https_address = cls._get_proxy_https_address()
        Clique.chyt_https_address = cls._get_chyt_https_address()

    def _get_proxy_metric(self, metric_name):
        return profiler_factory().at_http_proxy(self.Env.get_http_proxy_address()).counter(metric_name)

    @authors("evgenstf", "barykinni")
    def test_instance_choice(self):
        with Clique(5, alias="test_alias") as clique:
            for job_cookie in range(5):
                for add_asterisk in [False, True]:  # alias can be written with or without an asterisk
                    database_alias = "*" * int(add_asterisk) + "test_alias"

                    proxy_response = clique.make_query_via_proxy(
                        "select * from system.clique", database=database_alias + "@" + str(job_cookie)
                    )

                    for instance_response in proxy_response:
                        assert (
                            instance_response["self"] == 1
                            if instance_response["job_cookie"] == job_cookie
                            else instance_response["self"] == 0
                        )

                with raises_yt_error(QueryFailedError):  # operation-id is no longer supported
                    clique.make_query_via_proxy(
                        "select * from system.clique", database=clique.op.id + "@" + str(job_cookie)
                    )

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select * from system.clique", database="*test_alias@aaa")

    @authors("dakovalkov", "gudqeit")
    @pytest.mark.parametrize("discovery_version", [1, 2])
    @pytest.mark.parametrize("has_alias", [True, False])
    def test_http_proxy_simple(self, discovery_version, has_alias):
        patch = {
            "yt": {
                "discovery": {
                    "version": discovery_version,
                }
            }
        }
        alias = "*ch_alias_{}".format(discovery_version) if has_alias else None

        with Clique(1, config_patch=patch, alias=alias) as clique:
            proxy_response = clique.make_query_via_proxy("select * from system.clique")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response == response

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 1

            print_debug("Aborting job", jobs[0])
            abort_job(jobs[0])
            clique.wait_instance_count(1, unwanted_jobs=jobs, wait_discovery_sync=True)

            proxy_response = clique.make_query_via_proxy("select * from system.clique")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response == response

    @authors("dakovalkov")
    def test_ban_dead_instance_in_proxy(self):
        patch = {
            "yt": {
                "discovery": {
                    # Set big value to prevent node disappearing from discovery group.
                    "lease_timeout": 50000,
                }
            }
        }

        cache_missed_count = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed_count")
        force_update_count = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, config_patch=patch) as clique:
            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 2

            clique.op.suspend()
            print_debug("Aborting job", jobs[0])
            abort_job(jobs[0])
            wait(lambda: len(clique.op.get_running_jobs()) == 1)
            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            clique.op.resume()

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
        assert force_update_count.get_delta(verbose=True) == 0
        assert banned_count.get_delta(verbose=True) == 1

    @authors("dakovalkov")
    def test_ban_stopped_instance_in_proxy(self):
        patch = {
            "graceful_interruption_delay": 100000,
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

            clique.resize(1)

            alive_instances = clique.get_active_instances()
            assert set(map(str, alive_instances)).issubset(set(map(str, instances)))

            # We want instances[0] be dead and instances[1] be alive.
            if str(alive_instances[0]) == str(instances[0]):
                instances[0], instances[1] = instances[1], instances[0]

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
        assert force_update_count.get_delta(verbose=True) == 0
        assert banned_count.get_delta(verbose=True) == 1

    @authors("dakovalkov")
    @pytest.mark.skipif(True, reason="whatever")
    def test_clique_availability(self):
        create("table", "//tmp/table", attributes={"schema": [{"name": "i", "type": "int64"}]})
        write_table("//tmp/table", [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}])
        patch = {
            "graceful_interruption_delay": 600,
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
                    response = sorted(full_response.json()["data"], key=lambda row: row["i"])
                    assert response == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]
                    time.sleep(0.1)

            ping_thread = threading.Thread(target=pinger)
            ping_thread.start()
            time.sleep(1)

            instances = clique.get_active_instances()
            assert len(instances) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))

            self._signal_instance(instances[0].attributes["pid"], signal.SIGINT)

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

    @authors("max42", "barykinni")
    def test_database_specification(self):
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        with Clique(1, alias="*alias") as clique:
            assert clique.make_query_via_proxy("select 1 as a", database="*alias")[0] == {"a": 1}
            assert clique.make_query_via_proxy("select 1 as a", database="alias")[0] == {"a": 1}

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0]

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*alia")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="**")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="")

            with raises_yt_error(QueryFailedError):
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

        wait(lambda: clique.get_active_instance_count() == 0)

        time.sleep(1)

        with raises_yt_error(QueryFailedError):
            assert clique.make_query_via_proxy("select 1 as a", database="*alias")[0] == {"a": 1}

    @authors("dakovalkov")
    def test_expect_100_continue(self):
        headers = {"Expect": "100-continue"}
        with Clique(1) as clique:
            assert clique.make_query_via_proxy("select 1 as a", headers=headers)[0] == {"a": 1}

    @authors("max42")
    def test_operation_acl_validation(self):
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
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

    @authors("gudqeit")
    def test_clique_works_without_scheduler(self):
        patch = {
            "yt": {
                "discovery": {
                    "version": 2
                }
            }
        }
        create_user("u1")
        create_access_control_object_namespace(name="chyt")
        create_access_control_object(name="ch_alias", namespace="chyt")
        acl = [make_ace("allow", "u1", "use")]
        yt_set("//sys/access_control_object_namespaces/chyt/ch_alias/principal/@acl", acl)
        with Clique(1, config_patch=patch, alias="*ch_alias") as clique:
            # TODO(gudqeit): this attribute should become unused and must be removed after we stop supporting discovery v1 in HTTP proxy.
            yt_set(
                "//sys/strawberry/chyt/ch_alias/@strawberry_persistent_state",
                {
                    "yt_operation_id": clique.op.id,
                    "yt_operation_state": "running",
                }
            )
            time.sleep(1)
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                assert clique.make_query_via_proxy("select 1 as a", user="u1") == [{"a": 1}]

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1", user="u2")

    @authors("barykinni")
    def test_legacy_endpoint(self):
        with Clique(1) as clique:
            assert clique.make_query_via_proxy("SELECT 1 AS a", endpoint="/query") == [{"a": 1}]
            assert clique.make_query_via_proxy("SELECT 1 AS a", endpoint="/query", https_proxy=True) == [{"a": 1}]

    @authors("barykinni")
    def test_chyt_proxy(self):
        with Clique(1) as clique:
            assert clique.make_query_via_proxy("SELECT 1 AS a", endpoint="/", chyt_proxy=True) == [{"a": 1}]
            assert clique.make_query_via_proxy("SELECT 1 AS a", endpoint="/", chyt_proxy=True, https_proxy=True) == [{"a": 1}]

    @authors("gudqeit")
    def test_http_proxy_banned_username(self):
        patch = {
            "yt": {
                "user_name_blacklist": "banned_user"
            }
        }
        create_user("banned_user")
        access_control_entry = {"subjects": ["banned_user"], "action": "allow", "permissions": ["read"]}

        with Clique(1, spec={"alias": "*alias", "acl": [access_control_entry]}, config_patch=patch) as clique:
            response = clique.make_query_via_proxy('select 1', full_response=True, user="banned_user")
            assert response.status_code == 403
            assert "X-ClickHouse-Server-Display-Name" in response.headers

    @authors("barykinni")
    def test_http_proxy_authorization_via_x_click_house_key_header(self):
        username = "simple-dimple"
        create_user(username)

        allowance = {"subjects": [username], "action": "allow", "permissions": ["read"]}

        with Clique(1, spec={"acl": [allowance]}) as clique:
            # We expect token to be used as a username.

            correct_auth_response = clique.make_query_via_proxy(
                "select currentUser()", headers={"x-ClickHouse-Key": username})

            assert correct_auth_response == [{"currentUser()": username}]

            invalid_key = "mismatched"
            with raises_yt_error(900):  # user "mismatched" doesn't exist
                clique.make_query_via_proxy("select currentUser()", headers={"x-ClickHouse-Key": invalid_key})

    @authors("barykinni")
    def test_same_instance_for_the_same_session_id(self):
        session_id = _generate_session_id()

        with Clique(2) as clique:
            NUM_REQUESTS = 10

            picked_instances = [_job_id_of_instance_picked_for_the_query(clique, session_id=session_id)
                                for _ in range(NUM_REQUESTS)]

            assert len(set(picked_instances)) == 1, "when session_id is provided, proxy must pick same instance every time"

    @authors("barykinni")
    def test_job_cookie_dominates_session_id(self):
        with Clique(2, alias="*alias") as clique:
            NUM_REQUESTS = 10
            origin_job_cookie = _job_cookie_of_instance_picked_for_the_query(clique)

            for session_id in map(str, range(NUM_REQUESTS)):
                current_job_cookie = _job_cookie_of_instance_picked_for_the_query(
                    clique, database=f"alias@{origin_job_cookie}", session_id=session_id)

                assert current_job_cookie == origin_job_cookie, "when job-cookie is provided, proxy must pick exact instance regardless of session-id"

    @authors("barykinni")
    def test_sticky_cookie_uniform_distribution(self):
        """
        If sessions are being uniformly distributed among 3 instances
        then almost certainly it would take no more than 60 sessions
        to pick every instance at least once

        (Probability of not picking some instance in 60 sessions is less than 1e-10)

        However in most cases it will take signifficantly less amount of sessions to fulfill an expectation
        """
        NUM_INSTANCES = 3
        MAX_NUM_SESSIONS = 60

        with Clique(NUM_INSTANCES) as clique:
            picked_instances = set()
            for session_id in map(str, range(MAX_NUM_SESSIONS)):
                hit_instance = _job_id_of_instance_picked_for_the_query(clique, session_id=session_id)
                picked_instances.add(hit_instance)

                if len(picked_instances) == NUM_INSTANCES:
                    return

            assert False, "sessions must be distributed uniformly among instances"

    @authors("barykinni")
    def test_rotation_after_instance_fault(self):
        NUM_INSTANCES = 3
        NUM_SESSIONS = 12
        SESSION_IDS = list(map(str, range(NUM_SESSIONS)))

        def get_distribution(clique):
            return {session_id: _job_cookie_of_instance_picked_for_the_query(clique, session_id=session_id)
                    for session_id in SESSION_IDS}

        with Clique(NUM_INSTANCES) as clique:
            initial_distribution = get_distribution(clique)

            clique.resize(NUM_INSTANCES - 1)

            update_distribution = get_distribution(clique)

            rotations = [initial_distribution[session_id]
                         for session_id in SESSION_IDS
                         if initial_distribution[session_id] != update_distribution[session_id]]  # instance changed

            assert len(set(rotations)) <= 1, "only sessions attached to the killed instance should rotate"

    @authors("barykinni")
    def test_query_in_url(self):
        with Clique(1) as clique:
            assert clique.make_query_via_proxy("", settings={"query": "SELECT 1 AS a"}) == [{"a": 1}]


class TestClickHouseProxyStructuredLog(ClickHouseTestBase):
    DELTA_PROXY_CONFIG = {
        "clickhouse": {
            "operation_cache": {
                "refresh_time": 100,
            },
            "permission_cache": {
                "refresh_time": 100,
                "expire_after_failed_update_time": 100,
            },
        },
    }

    def setup_method(self, method):
        super(TestClickHouseProxyStructuredLog, self).setup_method(method)

        proxy_orchid = "//sys/http_proxies/" + ls("//sys/http_proxies")[0] + "/orchid"
        self.proxy_address = get(proxy_orchid + "/@remote_addresses/default")

        self.proxy_log_file = self.path_to_run + "/logs/http-proxy-0.chyt.yson.log"

    @classmethod
    def modify_proxy_config(cls, configs):
        assert len(configs) == 1

        configs[0]["logging"]["rules"].append(
            {
                "min_level": "info",
                "writers": ["chyt"],
                "include_categories": ["ClickHouseProxyStructured", "Barrier"],
                "message_format": "structured",
            }
        )
        configs[0]["logging"]["writers"]["chyt"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/http-proxy-0.chyt.yson.log"),
            "format": "yson",
        }

    @authors("dakovalkov")
    def test_structured_log(self):

        headers = {
            "x-ReQuEsT-Id": "1234-request-id-4321",
            "X-Yql-OpErAtIOn-Id": "abcd-1234-yql",
            "X-DataLens-Real-User": "dakovalkov",
        }

        with Clique(1) as clique:

            from_barrier = write_log_barrier(self.proxy_address)

            clique.make_query_via_proxy("select 1", headers=headers)
            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("invalid query")
            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1", database='*invalid_database')

            to_barrier = write_log_barrier(self.proxy_address)

            log_entries = read_structured_log(self.proxy_log_file, from_barrier=from_barrier, to_barrier=to_barrier)
            print_debug(log_entries)
            assert len(log_entries) == 3

            def check_log_entry(actual, expected):
                for column, expected_value in expected.items():
                    assert column in actual and actual[column] == expected_value

            check_log_entry(log_entries[0], {
                "authenticated_user": "root",
                "http_method": "post",
                "clique_id": clique.op.id,
                "http_code": 200,
                "datalens_real_user": "dakovalkov",
                "x_request_id": "1234-request-id-4321",
                "yql_operation_id": "abcd-1234-yql",
            })

            check_log_entry(log_entries[1], {
                "authenticated_user": "root",
                "http_method": "post",
                "clique_id": clique.op.id,
                "http_code": 400,
                # query failed, but there were no errors on proxy side, so error_code is None.
            })

            check_log_entry(log_entries[2], {
                "authenticated_user": "root",
                "http_method": "post",
                "clique_alias": "invalid_database",  # alias is expected not to have an asterisk
                "http_code": 400,
                "error_code": 1,
            })
