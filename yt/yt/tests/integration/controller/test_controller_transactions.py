
from yt_env_setup import YTEnvSetup
from yt_commands import (authors, create, write_table, merge, raises_yt_error)

from time import sleep


class TestControllerTransactions(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("coteeq")
    def test_consistent_locks(self):
        create(
            "table",
            "//tmp/in",
            attributes={"schema": [
                {"name": "x", "type": "int64", "sort_order": "ascending"},
                {"name": "y", "type": "int64"},
            ]},
        )

        def try_write(val):
            write_table(
                "<append=true>//tmp/in",
                [
                    {"x": val, "y": 2 * val}
                ]
            )
        try_write(val=0)

        op = merge(
            in_="//tmp/in",
            out="//tmp/in",
            mode="sorted",
            combine_chunks=True,
            spec={
                "force_transform": True,
                "testing": {
                    "delay_inside_prepare": "3s",
                },
            },
            track=False,
        )
        sleep(1)
        try_write(val=10)

        with raises_yt_error("has changed between taking input and output locks"):
            op.track()
