import yp.retries as yp_retries

from yt.wrapper.errors import YtRetriableError as ChaosMonkeyError

from yt.packages.six.moves import xrange

import pytest

from copy import deepcopy
import itertools
import random


class ControllableChaosMonkey(object):
    def __init__(self):
        self.set_values([False])

    def set_values(self, values):
        self._values = itertools.cycle(values)

    def __call__(self):
        return next(self._values)


def get_default_client_config():
    return {
        "request_timeout": 1000,
        "master_discovery_expiration_time": 10 ** 9,
        "retries": yp_retries.get_default_retries_config(),
    }


@pytest.mark.usefixtures("yp_env")
class TestRetries(object):
    def init_yp(self, yp_env):
        yp_client = yp_env.yp_client

        self.pod_set_id = yp_client.create_object("pod_set")
        self.pod_ids = []
        for _ in xrange(10):
            attributes = {"meta": {"pod_set_id": self.pod_set_id}}
            self.pod_ids.append(yp_client.create_object("pod", attributes=attributes))

    def do_test_deterministic(self, yp_env, transport, callback, check_result, config):
        retries_config = config["retries"]

        chaos_monkey = ControllableChaosMonkey()
        retries_config["_CHAOS_MONKEY_FACTORY"] = lambda: chaos_monkey

        retries_config["count"] = 1
        chaos_monkey.set_values([True, False, False])
        with yp_env.yp_instance.create_client(
            config=config, transport=transport
        ) as yp_flaky_client:
            with pytest.raises(ChaosMonkeyError):
                callback(yp_flaky_client)

        retries_config["count"] = 2
        chaos_monkey.set_values([True, False, False])
        with yp_env.yp_instance.create_client(
            config=config, transport=transport
        ) as yp_flaky_client_with_retries:
            check_result(callback(yp_flaky_client_with_retries))

    def do_test_pseudo_random(self, yp_env, transport, callback, check_result, config):
        retries_config = config["retries"]

        RETRIES_COUNT = 5
        retries_config["count"] = RETRIES_COUNT

        chaos_monkey = ControllableChaosMonkey()
        retries_config["_CHAOS_MONKEY_FACTORY"] = lambda: chaos_monkey

        def generate(count):
            result = []
            for _ in xrange(count):
                result.append(random.choice([False, True]))
            return result

        with yp_env.yp_instance.create_client(config=config, transport=transport) as yp_client:
            # Prepare YP master discovery info.
            yp_client.select_objects("pod", selectors=[""])

            for _ in xrange(5):
                chaos_monkey_values = generate(RETRIES_COUNT)
                chaos_monkey.set_values(chaos_monkey_values)
                if all(chaos_monkey_values):
                    with pytest.raises(ChaosMonkeyError):
                        callback(yp_client)
                else:
                    check_result(callback(yp_client))

    def do_test(self, yp_env, transport, callback, check_result, config=None):
        if config is None:
            config = get_default_client_config()
        else:
            config = deepcopy(config)

        self.init_yp(yp_env)
        self.do_test_deterministic(yp_env, transport, callback, check_result, config=config)
        self.do_test_pseudo_random(yp_env, transport, callback, check_result, config=config)

    def do_test_select_objects(self, yp_env, transport, config=None):
        def callback(yp_client):
            return yp_client.select_objects("pod", selectors=["/meta/id"], limit=10)

        def check_result(result):
            assert sorted(self.pod_ids) == sorted(map(lambda l: l[0], result))

        self.do_test(yp_env, transport, callback, check_result, config=config)

    @pytest.mark.parametrize("transport", ["http", "grpc"])
    def test_select_objects(self, yp_env, transport):
        self.do_test_select_objects(yp_env, transport)

    @pytest.mark.parametrize("transport", ["http", "grpc"])
    def test_get_object(self, yp_env, transport):
        def callback(yp_client):
            return yp_client.get_object("pod", self.pod_ids[0], selectors=["/meta/id"])

        def check_result(result):
            assert [self.pod_ids[0]] == result

        self.do_test(yp_env, transport, callback, check_result)

    @pytest.mark.parametrize("transport", ["http", "grpc"])
    def test_get_masters(self, yp_env, transport):
        def callback(yp_client):
            return yp_client.get_masters()

        def check_result(result):
            assert len(result["master_infos"]) == 1

        self.do_test(yp_env, transport, callback, check_result)

    def test_rounded_up_to_request_timeout_retries(self, yp_env):
        config = get_default_client_config()
        config["retries"]["backoff"] = {"policy": "rounded_up_to_request_timeout"}
        self.do_test_select_objects(yp_env, "grpc", config=config)
