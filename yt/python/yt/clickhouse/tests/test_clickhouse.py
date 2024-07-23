# -*- coding: utf-8 -*-

from __future__ import print_function

from .conftest import get_tests_sandbox

from yt.testlib import authors, check_rows_equality

import yt.wrapper as yt
import yt.clickhouse as chyt

try:
    from yt.packages.six import PY3
    from yt.packages.six.moves import map as imap
except ImportError:
    from six import PY3
    from six.moves import map as imap

from yt.test_helpers import wait

from yt.clickhouse.test_helpers import (get_host_paths, create_symlinks_for_chyt_binaries,
                                        create_controller_init_cluster_config, create_controller_one_shot_run_config, create_clique_speclet)

import yt.environment.arcadia_interop as arcadia_interop

import pytest
import os.path
import sys
import subprocess
import random
import string

import yatest

HOST_PATHS = get_host_paths(arcadia_interop, ["ytserver-clickhouse", "clickhouse-trampoline", "chyt-controller"])


def wait_for_clique_started(alias):
    discovery_path = ""
    if yt.exists("//sys/discovery_servers"):
        discovery_path = "//sys/discovery_servers/" + yt.list("//sys/discovery_servers")[0]
    else:
        discovery_path = "//sys/primary_masters/" + yt.list("//sys/primary_masters")[0]

    discovery_path += f"/orchid/discovery_server/chyt/{alias}/@members"

    def is_clique_started():
        try:
            members = yt.list(discovery_path)
            return len(members) > 0
        except yt.errors.YtResolveError:
            return False

    wait(is_clique_started)


def get_work_dir():
    return get_tests_sandbox() + "/test_clickhouse_cli"


@pytest.mark.usefixtures("yt_env")
class TestClickhouseCli(object):
    def setup_class(self):
        os.mkdir(get_work_dir())
        create_symlinks_for_chyt_binaries(HOST_PATHS, get_work_dir())

    def setup(self):
        work_dir = get_work_dir()
        proxy = yt.config.config["proxy"]["url"]

        self.alias = "ch_test_" + "".join(random.choices(string.ascii_lowercase, k=10))

        os.environ["YT_PROXY"] = proxy

        init_cluster_config_path = create_controller_init_cluster_config(proxy, work_dir)
        run_config_path = create_controller_one_shot_run_config(proxy, work_dir)
        speclet_path = create_clique_speclet(work_dir, {})

        yatest.common.execute(
            [
                HOST_PATHS["chyt-controller"],
                "--config-path", init_cluster_config_path,
                "init-cluster",
                "--log-to-stderr",
            ],
            cwd=work_dir,
            wait=True,
        )

        yatest.common.execute(
            [
                HOST_PATHS["chyt-controller"],
                "--config-path", run_config_path,
                "one-shot-run",
                "--alias", self.alias,
                "--speclet-path", speclet_path,
                "--log-to-stderr",
            ],
            cwd=work_dir,
            wait=True,
        )

        wait_for_clique_started(self.alias)

    @authors("max42")
    def test_execute(self):
        content = [{"a": i} for i in range(4)]
        yt.create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        yt.write_table("//tmp/t", content)
        check_rows_equality(chyt.execute("select 1", self.alias),
                            [{"1": 1}])
        check_rows_equality(chyt.execute("select * from `//tmp/t`", self.alias),
                            content,
                            ordered=False)
        check_rows_equality(chyt.execute("select avg(a) from `//tmp/t`", self.alias),
                            [{"avg(a)": 1.5}])

        def check_lines(lhs, rhs):
            def decode_as_utf8(smth):
                if PY3 and isinstance(smth, bytes):
                    return smth.decode("utf-8")
                return smth

            lhs = list(imap(decode_as_utf8, lhs))
            rhs = list(imap(decode_as_utf8, rhs))
            assert lhs == rhs

        check_lines(chyt.execute("select * from `//tmp/t`", self.alias, format="TabSeparated"),
                    ["0", "1", "2", "3"])
        # By default, ClickHouse quotes all int64 and uint64 to prevent precision loss.
        check_lines(chyt.execute("select a, a * a from `//tmp/t`", self.alias, format="JSONEachRow"),
                    ['{"a":"0","multiply(a, a)":"0"}',
                     '{"a":"1","multiply(a, a)":"1"}',
                     '{"a":"2","multiply(a, a)":"4"}',
                     '{"a":"3","multiply(a, a)":"9"}'])
        check_lines(chyt.execute("select a, a * a from `//tmp/t`", self.alias, format="JSONEachRow",
                                 settings={"output_format_json_quote_64bit_integers": False}),
                    ['{"a":0,"multiply(a, a)":0}',
                     '{"a":1,"multiply(a, a)":1}',
                     '{"a":2,"multiply(a, a)":4}',
                     '{"a":3,"multiply(a, a)":9}'])

    @authors("dakovalkov")
    def test_settings_in_execute(self):
        # String ClickHouse setting.
        check_rows_equality(chyt.execute("select getSetting('distributed_product_mode') as s", self.alias,
                                         settings={"distributed_product_mode": "global"}),
                            [{"s": "global"}])
        # Int ClickHouse setting.
        check_rows_equality(chyt.execute("select getSetting('http_zlib_compression_level') as s", self.alias,
                                         settings={"http_zlib_compression_level": 8}),
                            [{"s": 8}])
        # String CHYT setting.
        check_rows_equality(chyt.execute("select getSetting('chyt.random_string_setting') as s", self.alias,
                                         settings={"chyt.random_string_setting": "random_string"}),
                            [{"s": "random_string"}])
        # Int CHYT setting.
        # ClickHouse does not know the type of custom settings, so string is expected.
        check_rows_equality(chyt.execute("select getSetting('chyt.random_int_setting') as s", self.alias,
                                         settings={"chyt.random_int_setting": 123}),
                            [{"s": "123"}])
        # Binary string setting.
        check_rows_equality(chyt.execute("select getSetting('chyt.binary_string_setting') as s", self.alias,
                                         settings={"chyt.binary_string_setting": "\x00\x01\x02\x03\x04"}),
                            [{"s": "\x00\x01\x02\x03\x04"}])

    @authors("max42")
    def test_unicode_in_query(self):
        assert list(chyt.execute(u"select 'юникод' as s", self.alias)) == [{"s": u"юникод"}]

    @authors("max42")
    def test_cli_simple(self):
        yt.create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"},
                                                             {"name": "b", "type": "string"}]})
        yt.write_table("//tmp/t", [{"a": 1, "b": "foo"},
                                   {"a": 2, "b": "bar"}])

        cmd = yatest.common.runtime.build_path("yt/python/yt/wrapper/bin/yt_make/yt") + " " + "clickhouse"
        env = {
            "CMD": cmd,
            "YT_PROXY": yt.config.config["proxy"]["url"],
            "CHYT_ALIAS": self.alias,
        }

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

        format_error = r"Do not specify FORMAT"

        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t" FORMAT JSON', self.alias), get_content("a"))
        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t" FoRmAt Protobuf', self.alias), get_content("a"))
        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t"     fOrMaT   Regexp   ', self.alias), get_content("a"))
        with pytest.raises(yt.YtError, match=format_error):
            check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t"  format Pretty; ;; ; ;; ', self.alias), get_content("a"))

        # TODO(dakovalkov): Tricky cases which are difficult to handle:
        # chyt.execute('SELECT * FROM "//tmp/t" FORMAT "JSON"')
        # chyt.execute('SELECT * FROM "//tmp/t" FORMAT `JSON`')

        check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t";;;', self.alias), get_content("a"))
        check_rows_equality(chyt.execute('SELECT a AS format FROM "//tmp/t"', self.alias), get_content("format"))
        check_rows_equality(chyt.execute('SELECT * FROM "//tmp/t" WHERE a != \'FORMAT JSON\'', self.alias), get_content("a"))
        # 'JSON' is an alias for the column 'format'.
        check_rows_equality(chyt.execute('SELECT format JSON FROM (SELECT a AS format FROM "//tmp/t")', self.alias), get_content("JSON"))
        check_rows_equality(chyt.execute('SELECT a AS xformat FROM "//tmp/t" ORDER BY xformat ASC', self.alias), get_content("xformat"))

        # TODO(dakovalkov): Tricky cases which are difficult to handle (YQL fails on them as well):
        # chyt.execute('SELECT a AS format FROM "//tmp/t" ORDER BY format DESC', self.alias)
        # Here 'JSON' is an alias for the subquery 'format'
        # chyt.execute('WITH format AS (SELECT * FROM "//tmp/t") SELECT * FROM format JSON', self.alias)
