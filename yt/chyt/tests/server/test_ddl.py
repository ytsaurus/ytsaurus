from helpers import get_object_attribute_cache_config

from yt_commands import (authors, raises_yt_error, create, exists, write_table)

from base import ClickHouseTestBase, Clique, QueryFailedError

import time


class TestClickHouseDdl(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("evgenstf")
    def test_drop_nonexistent_table(self):
        patch = get_object_attribute_cache_config(500, 500, None)
        with Clique(1, config_patch=patch) as clique:
            assert not exists("//tmp/t")
            assert clique.make_query('exists "//tmp/t"') == [{"result": 0}]
            with raises_yt_error(QueryFailedError):
                assert clique.make_query('drop table "//tmp/t"')

    @authors("evgenstf")
    def test_drop_table(self):
        patch = get_object_attribute_cache_config(500, 500, None)
        with Clique(1, config_patch=patch) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            assert clique.make_query('select * from "//tmp/t"') == [{"a": "2012-12-12 20:00:00"}]
            clique.make_query('drop table "//tmp/t"')
            time.sleep(1)
            assert not exists("//tmp/t")
            assert clique.make_query('exists "//tmp/t"') == [{"result": 0}]

    @authors("gudqeit")
    def test_error_rename_table(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            create("table", "//tmp/s", attributes={"schema": [{"name": "a", "type": "string"}]})
            assert exists("//tmp/t") and exists("//tmp/s")
            with raises_yt_error(QueryFailedError):
                clique.make_query('rename table "//tmp/t" to "//tmp/s"')
                assert clique.make_query('select * from "//tmp/s"') == []

            with raises_yt_error(QueryFailedError):
                clique.make_query('rename table "//tmp/tt" to "//tmp/ss"')
                assert not exists("//tmp/tt") and not exists("//tmp/ss")

    @authors("gudqeit")
    def test_rename_table(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            assert exists("//tmp/t")
            clique.make_query('rename table "//tmp/t" to "//tmp/tt"')
            assert not exists("//tmp/t")
            assert clique.make_query('select * from "//tmp/tt"') == [{"a": "2012-12-12 20:00:00"}]

    @authors("gudqeit")
    def test_error_exchange_tables(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            assert exists("//tmp/t")
            with raises_yt_error(QueryFailedError):
                clique.make_query('exchange tables "//tmp/t" and "//tmp/s"')
                assert not exists("//tmp/s")

    @authors("gudqeit")
    def test_exchange_tables(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            create("table", "//tmp/s", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/s", [{"a": "string"}])
            clique.make_query('exchange tables "//tmp/t" and "//tmp/s"')
            assert clique.make_query('select * from "//tmp/t"') == [{"a": "string"}]
            assert clique.make_query('select * from "//tmp/s"') == [{"a": "2012-12-12 20:00:00"}]
