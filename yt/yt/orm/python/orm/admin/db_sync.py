from yt.orm.admin.db_manager import get_yt_cluster_name, DbManager

from yt.yt_sync.scenario.registry import list_known_scenarios
from yt.yt_sync import get_yt_client_factory, Settings, YtSync
from yt.yt_sync.core import DefaultTableFilter, TableNameFilter, TableFilterBase
from yt.yt_sync.lock import YtSyncLock

import yt.yson as yson

from typing import Any


def make_table_settings(
    table: str, generated_description: dict[str, Any], db_manager: DbManager
) -> dict[str, Any]:
    table: yson.YsonUnicode = yson.YsonUnicode(table)
    table.attributes = {
        "schema": generated_description["schema"],
        **generated_description.get("attributes", {}),
    }
    db_manager.make_table_attributes(table)

    return {
        "path": db_manager.get_table_path(table),
        "master": {
            "cluster": get_yt_cluster_name(db_manager.yt_client),
            "attributes": table.attributes,
        },
    }


def run(yt_proxy, orm_path, data_model_traits, scenario, **kwargs):
    # See docs for settings for all available settings.
    settings = Settings(
        db_type=Settings.REPLICATED_DB,
        ensure_folders=kwargs.get("ensure_folders", False),
        parallel_processing_for_unmounted=kwargs.get("force_parallel_execution", False),
    )
    settings.batch_size_for_parallel[scenario] = kwargs.get("parallel_batch_size", 0)

    # Creates factory for YT client wrapper, supporting dry run mode.
    # YT client credentials must be passed explicitly via param token
    # or YT_TOKEN environment variable should be set.
    # Invoke with dry_run=False for real changes.
    yt_client_factory = get_yt_client_factory(dry_run=kwargs.get("dry_run", True))

    # Lock guard to prevent run several YtSync processes simultaneously.
    db_lock = YtSyncLock(yt_client_factory(yt_proxy), orm_path)

    table_filter_param: list[str] = kwargs.get("table_filter", [])
    table_filter: TableFilterBase = TableNameFilter(table_filter_param) if table_filter_param else DefaultTableFilter()

    yt_sync = YtSync(
        yt_client_factory=yt_client_factory,
        settings=settings,
        lock=db_lock,
        scenario_name=scenario,
        table_filter=table_filter
    )

    actual_db_version = data_model_traits.get_actual_db_version()
    db_manager = DbManager(
        yt_client_factory(yt_proxy),
        orm_path,
        data_model_traits,
        actual_db_version,
    )

    tables = data_model_traits.get_yt_schema(actual_db_version)["all_tables"]
    for table, generated_description in tables.items():
        table_settings = make_table_settings(table, generated_description, db_manager)
        yt_sync.add_desired_table(table_settings)

    # All real job is here.
    yt_sync.sync()


def register_command(parser):
    scenarios = parser.add_argument_group("scenarios", description="YtSync scenarios to execute")
    scenarios = scenarios.add_mutually_exclusive_group(required=True)
    for name, scenario in list_known_scenarios().items():
        scenarios.add_argument(
            f"--{name.replace('_', '-')}",
            action="store_const",
            const=name,
            dest="scenario",
            help=scenario.SCENARIO_DESCRIPTION,
        )

    parser.add_argument(
        "--commit",
        dest="dry_run",
        default=True,
        action="store_false",
        help="Apply changes",
    )
    parser.add_argument(
        "--ensure-folders", default=False, action="store_true", help="Ensure folders exist"
    )
    parser.add_argument(
        "--force-parallel-execution",
        default=False,
        action="store_true",
        help="Run YtSync actions in parallel (use with care)",
    )
    parser.add_argument(
        "--parallel-batch-size",
        default=0,
        type=int,
        help="Batch size for parallel table processing. 0 - unlimited, process in parallel all affected tabled (use with care). Default is 0",
    )
    parser.add_argument(
        "--log-tablet-count-changes",
        default=True,
        action="store_true",
        help="Log tablet count changes in diff",
    )
    parser.add_argument(
        "--table-filter",
        nargs="*",
        default=[],
        required=False,
        help="Apply changes only on given tables",
    )
