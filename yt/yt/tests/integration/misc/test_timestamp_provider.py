from yt_env_setup import YTEnvSetup

from yt_commands import authors, commit_transaction, exists, generate_timestamp, get, ls, start_transaction, wait

##################################################################


class TestTimestampProvider(YTEnvSetup):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_TIMESTAMP_PROVIDERS = 1

    @classmethod
    def modify_timestamp_providers_configs(cls, timestamp_providers_configs, clock_configs, yt_configs):
        for config in timestamp_providers_configs[0]:
            config["cypress_registrar"] = {
                "update_period": 100,
            }

        return True

    @authors("gritukan")
    def test_generate_timestamp(self):
        t1 = generate_timestamp()
        t2 = generate_timestamp()
        assert t2 > t1

    @authors("gritukan", "babenko")
    def test_tx(self):
        tx = start_transaction()
        commit_transaction(tx)

    @authors("aleksandra-zh")
    def test_cypress_registration(self):
        wait(lambda: exists("//sys/timestamp_proxies"))
        wait(lambda: get("//sys/timestamp_proxies/@count") > 0)
        address = ls("//sys/timestamp_proxies")[0]
        expiration_time = get(f"//sys/timestamp_proxies/{address}/@expiration_time")
        wait(lambda: get(f"//sys/timestamp_proxies/{address}/@expiration_time") != expiration_time)
        assert get(f"//sys/timestamp_proxies/{address}/orchid/service/name") == "timestamp_provider"


##################################################################


class TestTimestampProviderClocks(TestTimestampProvider):
    ENABLE_MULTIDAEMON = False
    NUM_CLOCKS = 1
