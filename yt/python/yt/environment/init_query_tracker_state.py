#!/usr/bin/python3

from yt.wrapper import YtClient, config

from yt.environment.migrationlib import TableInfo, Migration, Conversion

import argparse
import logging

DEFAULT_BUNDLE_NAME = "default"
SYS_BUNDLE_NAME = "sys"
DEFAULT_STATE_PATH = "//sys/query_tracker"
DEFAULT_SHARD_COUNT = 1

INITIAL_TABLE_INFOS = {
    "active_queries": TableInfo(
        [
            ("query_id", "string"),
        ],
        [
            ("engine", "string", {"lock": "client"}),
            ("query", "string", {"lock": "client"}),
            ("settings", "any", {"lock": "client"}),
            ("user", "string", {"lock": "client"}),
            ("start_time", "timestamp", {"lock": "client"}),
            ("filter_factors", "string", {"lock": "client"}),
            ("state", "string", {"lock": "common"}),
            ("incarnation", "int64", {"lock": "query_tracker"}),
            ("ping_time", "timestamp", {"lock": "query_tracker"}),
            ("assigned_tracker", "string", {"lock": "query_tracker"}),
            ("progress", "any", {"lock": "query_tracker"}),
            ("error", "any", {"lock": "query_tracker"}),
            ("result_count", "int64", {"lock": "query_tracker"}),
            ("finish_time", "timestamp", {"lock": "common"}),
            ("abort_request", "any", {"lock": "client"}),
            ("annotations", "any", {"lock": "client"}),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
    "finished_queries": TableInfo(
        [
            ("query_id", "string"),
        ],
        [
            ("engine", "string"),
            ("query", "string"),
            ("settings", "any"),
            ("user", "string"),
            ("start_time", "timestamp"),
            ("state", "string"),
            ("progress", "any"),
            ("error", "any"),
            ("result_count", "int64"),
            ("finish_time", "timestamp"),
            ("annotations", "any"),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
    "finished_queries_by_start_time": TableInfo(
        [("start_time", "timestamp"), ("query_id", "string")],
        [
            ("engine", "string"),
            ("user", "string"),
            ("state", "string"),
            ("filter_factors", "string"),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
    "finished_query_results": TableInfo(
        [
            ("query_id", "string"),
            ("result_index", "int64"),
        ],
        [
            ("error", "any"),
            ("schema", "any"),
            ("data_statistics", "any"),
            ("rowset", "string"),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
}

INITIAL_VERSION = 0
TRANSFORMS = {}
ACTIONS = {}

TRANSFORMS[1] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
            },
        )
    )
]

TRANSFORMS[2] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
            },
        )
    ),
    Conversion(
        "finished_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string"),
                ("query", "string"),
                ("files", "any"),
                ("settings", "any"),
                ("user", "string"),
                ("start_time", "timestamp"),
                ("state", "string"),
                ("progress", "any"),
                ("error", "any"),
                ("result_count", "int64"),
                ("finish_time", "timestamp"),
                ("annotations", "any"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
            },
        ),
    )
]

TRANSFORMS[3] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        )
    ),
    Conversion(
        "finished_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string"),
                ("query", "string"),
                ("files", "any"),
                ("settings", "any"),
                ("user", "string"),
                ("start_time", "timestamp"),
                ("state", "string"),
                ("progress", "any"),
                ("error", "any"),
                ("result_count", "int64"),
                ("finish_time", "timestamp"),
                ("annotations", "any"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
    ),
    Conversion(
        "finished_queries_by_start_time",
        table_info=TableInfo(
            [
                ("start_time", "timestamp"),
                ("query_id", "string")
            ],
            [
                ("engine", "string"),
                ("user", "string"),
                ("state", "string"),
                ("filter_factors", "string"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
    ),
    Conversion(
        "finished_query_results",
        table_info=TableInfo(
            [
                ("query_id", "string"),
                ("result_index", "int64"),
            ],
            [
                ("error", "any"),
                ("schema", "any"),
                ("data_statistics", "any"),
                ("rowset", "string"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        )
    ),
]

# NB(mpereskokova): don't forget to update min_required_state_version at yt/yt/server/query_tracker/config.cpp and state at yt/yt/ytlib/query_tracker_client/records/query.yaml

MIGRATION = Migration(
    initial_table_infos=INITIAL_TABLE_INFOS,
    initial_version=INITIAL_VERSION,
    transforms=TRANSFORMS,
    actions=ACTIONS,
)


def get_latest_version():
    """ Get latest version of the query tracker state migration """
    return MIGRATION.get_latest_version()


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Transform query tracker state")
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--state-path", type=str, default=DEFAULT_STATE_PATH)
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-version", type=int)
    group.add_argument("--latest", action="store_true")
    return parser


def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])

    target_version = args.target_version
    if args.latest:
        target_version = MIGRATION.get_latest_version()

    MIGRATION.run(
        client=client,
        tables_path=args.state_path,
        target_version=target_version,
        shard_count=args.shard_count,
        force=args.force,
    )


if __name__ == "__main__":
    main()
