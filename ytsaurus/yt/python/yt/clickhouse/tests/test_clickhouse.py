# -*- coding: utf-8 -*-

from __future__ import print_function

from yt.testlib import authors, check_rows_equality, set_config_option

import yt.wrapper as yt
import yt.clickhouse as chyt

try:
    from yt.packages.six import PY3
    from yt.packages.six.moves import map as imap
except ImportError:
    from six import PY3
    from six.moves import map as imap

from yt.test_helpers import wait

from yt.clickhouse.test_helpers import get_clickhouse_server_config, get_host_paths

import yt.environment.arcadia_interop as arcadia_interop

import pytest
import os.path
import copy
import sys
import subprocess

import yatest

HOST_PATHS = get_host_paths(arcadia_interop, ["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer",
                                              "ytserver-dummy"])

DEFAULTS = {
    "memory_config": {
        "footprint": 1 * 1024**3,
        "clickhouse": int(2.5 * 1024**3),
        "reader": 1 * 1024**3,
        "uncompressed_block_cache": 0,
        "compressed_block_cache": 0,
        "chunk_meta_cache": 0,
        "log_tailer": 0,
        "watchdog_oom_watermark": 0,
        "watchdog_window_oom_watermark": 0,
        "clickhouse_watermark": 1 * 1024**3,
        "memory_limit": int((1 + 2.5 + 1 + 1) * 1024**3),
        "max_server_memory_usage": int((1 + 2.5 + 1) * 1024**3),
    },
    "host_ytserver_clickhouse_path": HOST_PATHS["ytserver-clickhouse"],
    "host_clickhouse_trampoline_path": HOST_PATHS["clickhouse-trampoline"],
    "host_ytserver_log_tailer_path": HOST_PATHS["ytserver-log-tailer"],
    "cpu_limit": 1,
    "enable_monitoring": False,
    "clickhouse_config": {},
    "max_instance_count": 100,
    "enable_job_tables": True,
    "cypress_log_tailer_config_path": "//sys/clickhouse/log_tailer_config",
    "log_tailer_table_attribute_patch": {"primary_medium": "default"},
    "log_tailer_tablet_count": 1,
    "skip_version_compatibility_validation": True,
}


class ClickhouseTestBase(object):
    def _setup(self):
        ytrecipe = os.environ.get("YT_OUTPUT") is not None
        if ytrecipe:
            pytest.skip()

        yt.create("document", "//sys/clickhouse/defaults", recursive=True, attributes={"value": DEFAULTS}, force=True)
        yt.create("map_node", "//home/clickhouse-kolkhoz", recursive=True, force=True)
        yt.link("//home/clickhouse-kolkhoz", "//sys/clickhouse/kolkhoz", recursive=True, ignore_existing=True)
        yt.create("document", "//sys/clickhouse/log_tailer_config", attributes={"value": get_clickhouse_server_config()}, force=True)
        if yt.get("//sys/tablet_cells/@count") == 0:
            cell_id = yt.create("tablet_cell", attributes={"size": 1})
        else:
            cell_id = yt.list("//sys/tablet_cells")[0]
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")
        yt.create("user", attributes={"name": "yt-clickhouse-cache"}, force=True)
        yt.create("user", attributes={"name": "yt-clickhouse"}, force=True)
        if "superusers" not in yt.get("//sys/users/yt-clickhouse/@member_of_closure"):
            yt.add_member("yt-clickhouse", "superusers")


@pytest.mark.usefixtures("yt_env")
class TestClickhouseFromHost(ClickhouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_execute(self):
        content = [{"a": i} for i in range(4)]
        yt.create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        yt.write_table("//tmp/t", content)
        chyt.start_clique(1, alias="*c")
        check_rows_equality(chyt.execute("select 1", "*c"),
                            [{"1": 1}])
        check_rows_equality(chyt.execute("select * from `//tmp/t`", "*c"),
                            content,
                            ordered=False)
        check_rows_equality(chyt.execute("select avg(a) from `//tmp/t`", "*c"),
                            [{"avg(a)": 1.5}])

        def check_lines(lhs, rhs):
            def decode_as_utf8(smth):
                if PY3 and isinstance(smth, bytes):
                    return smth.decode("utf-8")
                return smth

            lhs = list(imap(decode_as_utf8, lhs))
            rhs = list(imap(decode_as_utf8, rhs))
            assert lhs == rhs

        check_lines(chyt.execute("select * from `//tmp/t`", "*c", format="TabSeparated"),
                    ["0", "1", "2", "3"])
        # By default, ClickHouse quotes all int64 and uint64 to prevent precision loss.
        check_lines(chyt.execute("select a, a * a from `//tmp/t`", "*c", format="JSONEachRow"),
                    ['{"a":"0","multiply(a, a)":"0"}',
                     '{"a":"1","multiply(a, a)":"1"}',
                     '{"a":"2","multiply(a, a)":"4"}',
                     '{"a":"3","multiply(a, a)":"9"}'])
        check_lines(chyt.execute("select a, a * a from `//tmp/t`", "*c", format="JSONEachRow",
                                 settings={"output_format_json_quote_64bit_integers": False}),
                    ['{"a":0,"multiply(a, a)":0}',
                     '{"a":1,"multiply(a, a)":1}',
                     '{"a":2,"multiply(a, a)":4}',
                     '{"a":3,"multiply(a, a)":9}'])

    @authors("dakovalkov")
    def test_settings_in_execute(self):
        chyt.start_clique(1, alias="*d")
        # String ClickHouse setting.
        check_rows_equality(chyt.execute("select getSetting('distributed_product_mode') as s", "*d",
                                         settings={"distributed_product_mode": "global"}),
                            [{"s": "global"}])
        # Int ClickHouse setting.
        check_rows_equality(chyt.execute("select getSetting('http_zlib_compression_level') as s", "*d",
                                         settings={"http_zlib_compression_level": 8}),
                            [{"s": 8}])
        # String CHYT setting.
        check_rows_equality(chyt.execute("select getSetting('chyt.random_string_setting') as s", "*d",
                                         settings={"chyt.random_string_setting": "random_string"}),
                            [{"s": "random_string"}])
        # Int CHYT setting.
        # ClickHouse does not know the type of custom settings, so string is expected.
        check_rows_equality(chyt.execute("select getSetting('chyt.random_int_setting') as s", "*d",
                                         settings={"chyt.random_int_setting": 123}),
                            [{"s": "123"}])
        # Binary string setting.
        check_rows_equality(chyt.execute("select getSetting('chyt.binary_string_setting') as s", "*d",
                                         settings={"chyt.binary_string_setting": "\x00\x01\x02\x03\x04"}),
                            [{"s": "\x00\x01\x02\x03\x04"}])

    @authors("max42")
    def test_unicode_in_query(self):
        chyt.start_clique(1, alias="*f")
        assert list(chyt.execute(u"select 'юникод' as s", "*f")) == [{"s": u"юникод"}]

    @authors("max42")
    def test_cli_simple(self):
        yt.create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"},
                                                             {"name": "b", "type": "string"}]})
        yt.write_table("//tmp/t", [{"a": 1, "b": "foo"},
                                   {"a": 2, "b": "bar"}])

        alias = "*e1"

        cmd = yatest.common.runtime.build_path("yt/python/yt/wrapper/bin/yt_make/yt") + " " + "clickhouse"
        env = {
            "CMD": cmd,
            "YT_PROXY": yt.config.config["proxy"]["url"],
            "CHYT_ALIAS": alias,
        }

        chyt.start_clique(1, alias=alias)

        print("Env:", env, file=sys.stderr)

        test_binary = yatest.common.source_path("yt/python/yt/clickhouse/tests/test_cli.sh")

        with open("shell_output", "w") as output:
            proc = subprocess.Popen(["/bin/bash", test_binary], env=env, stdout=output, stderr=output)
            proc.communicate()

        sys.stderr.write(open("shell_output").read())

        assert proc.returncode == 0

    @authors("dakovalkov")
    def test_format(self):
        def get_content(column):
            return [{column: str(i)} for i in range(4)]

        yt.create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        yt.write_table("//tmp/t", get_content("a"))

        chyt.start_clique(1, alias="*format")

        format_error = r"Do not specify FORMAT"

        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t" FORMAT JSON', "*format"), get_content("a"))
        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t" FoRmAt Protobuf', "*format"), get_content("a"))
        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t"     fOrMaT   Regexp   ', "*format"), get_content("a"))
        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t"  format Pretty; ;; ; ;; ', "*format"), get_content("a"))

        # TODO(dakovalkov): Tricky cases which are difficult to handle:
        # chyt.execute('SELECT * FROM "//tmp/t" FORMAT "JSON"')
        # chyt.execute('SELECT * FROM "//tmp/t" FORMAT `JSON`')

        check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t";;;', "*format"), get_content("a"))
        check_rows_equality(chyt.execute('SELECT a AS format FROM "//tmp/t"', "*format"), get_content("format"))
        check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t" WHERE a != \'FORMAT JSON\'', "*format"), get_content("a"))
        # 'JSON' is an alias for the column 'format'.
        check_rows_equality(chyt.execute('SELECT format JSON FROM (SELECT a AS format FROM "//tmp/t")', "*format"), get_content("JSON"))
        check_rows_equality(chyt.execute('SELECT a AS xformat FROM "//tmp/t" ORDER BY xformat ASC', "*format"), get_content("xformat"))

        # TODO(dakovalkov): Tricky cases which are difficult to handle (YQL fails on them as well):
        # chyt.execute('SELECT a AS format FROM "//tmp/t" ORDER BY format DESC', "*format")
        # Here 'JSON' is an alias for the subquery 'format'
        # chyt.execute('WITH format AS (SELECT * FROM "//tmp/t") SELECT * FROM format JSON', "*format")

    @authors("gudqeit")
    def test_abort_existing_clique(self):
        o1 = chyt.start_clique(1, alias="*f")
        with pytest.raises(yt.YtError, match=r"There is already an operation with alias"):
            chyt.start_clique(1, alias="*f")
        o2 = chyt.start_clique(1, alias="*f", abort_existing=True)
        assert o1.id != o2.id


@pytest.mark.usefixtures("yt_env")
class TestNonTrivialClient(ClickhouseTestBase):
    def setup(self):
        self._setup()
        yt.set("//sys/clickhouse/log_tailer_config/log_tailer", {"log_files": [{"ttl": 604800000, "path": "clickhouse.log"}]})

    @authors("max42")
    def test_non_trivial_client(self):
        # We ruin global proxy config to make sure start_clique uses only provided client.
        client = yt.YtClient(config=copy.deepcopy(yt.config.config))
        print("Patching global config", file=sys.stderr)
        with set_config_option("proxy/url", "invalid_url_due_to_forgotten_client", final_action=lambda: print("Reverting global config")):
            chyt.start_clique(1, alias="*e", client=client)
            print("Clique successfully started", file=sys.stderr)


# Waiting for real ytserver-clickhouse upload is too long, so we upload fake binary instead.
@pytest.mark.usefixtures("yt_env")
class TestClickhouseFromCypress(ClickhouseTestBase):
    def _turbo_write_file(self, destination, path):
        upload_client = yt.YtClient(config=yt.config.get_config(client=None))
        upload_client.config["proxy"]["content_encoding"] = "identity"
        upload_client.config["write_parallel"]["enable"] = False
        upload_client.config["write_retries"]["chunk_size"] = 4 * 1024**3

        yt.create("file", destination, attributes={"replication_factor": 1, "executable": True}, recursive=True)
        upload_client.write_file(destination,
                                 open(path, "rb"),
                                 filename_hint=os.path.basename(path),
                                 file_writer={
                                     "enable_early_finish": True,
                                     "min_upload_replication_factor": 1,
                                     "upload_replication_factor": 1,
                                     "send_window_size": 4 * 1024**3,
                                     "sync_on_close": False,
                                 })

    def setup(self):
        self._setup()
        for destination_bin, source_bin in (
                ("ytserver-clickhouse", "ytserver-dummy"),
                ("clickhouse-trampoline", "clickhouse-trampoline"),
                ("ytserver-log-tailer", "ytserver-dummy"),
        ):
            self._turbo_write_file("//sys/bin/{0}/{0}".format(destination_bin), HOST_PATHS[source_bin])
            yt.remove("//sys/clickhouse/defaults/host_" + destination_bin.replace("-", "_") + "_path")
        yt.set("//sys/bin/ytserver-log-tailer/ytserver-log-tailer/@yt_version", "")

    @authors("max42")
    def test_fake_chyt(self):
        chyt.start_clique(1, alias="*c", wait_for_instances=False)
