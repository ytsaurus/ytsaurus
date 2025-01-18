import argparse
import sys
from typing import Callable

import yt.yson as yson
from yt.yt_sync.core.client import get_yt_client_factory
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.lock import DbLockBase
from yt.yt_sync.lock import DummyLock
from yt.yt_sync.lock import YtSyncLock
from yt.yt_sync.scenario import list_known_scenarios
from yt.yt_sync.sync import YtSync

from .description import Description
from .description import StageDescription
from .description import StageEntityType
from .details import DescriptionDetails
from .details import SpecialNames
from .log import DefaultLogSetup
from .log import LogSetup


def _parse_args(name: str, description: DescriptionDetails, log_setup: LogSetup) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"YtSync runner for {name}")
    log_setup.add_args(parser)

    if len(all_stages := description.get_all_stages()) > 1:
        parser.add_argument("-S", "--stage", choices=all_stages, required=True, help="Stage")

    for entity_type in StageEntityType:
        if all_entities := description.get_all(entity_type):
            parser.add_argument(
                f"--{entity_type.value}s",
                choices=SpecialNames.values() + all_entities,
                default=[SpecialNames.ALL],
                nargs="*",
                help=(
                    f"{entity_type.value.upper()}s to process "
                    f"(default is 'all', 'none' to skip all {entity_type.value}s)"
                ),
            )

    parser.add_argument(
        "--scenario",
        choices=list_known_scenarios(),
        default=None,
        help="Scenario to run ('dump_diff' is default for dry run, 'ensure_heavy' otherwise)",
    )
    parser.add_argument(
        "--scenario-params",
        type=str,
        required=False,
        default=dict(),
        help="Scenario params in YSON format (see https://ytsaurus.tech/docs/ru/user-guide/storage/yson)",
    )
    parser.add_argument(
        "--parallel-factor",
        default=-1,
        type=int,
        help="How many tables process simultaneously on cluster (0 for unlimited, default is from settings)",
    )
    parser.add_argument("--commit", action="store_false", default=True, dest="dry_run", help="Apply changes")
    return parser.parse_args()


def _ensure_args(args: argparse.Namespace, description: DescriptionDetails):
    if args.scenario is None:
        args.scenario = "dump_diff" if args.dry_run else "ensure_heavy"
    if not hasattr(args, "stage"):
        args.stage = description.get_all_stages()[0]
    for entity_type in StageEntityType:
        if not hasattr(args, f"{entity_type.value}s"):
            setattr(args, f"{entity_type.value}s", [SpecialNames.ALL])
    for entity_type in StageEntityType:
        description.validate_filter(entity_type, args.stage, getattr(args, f"{entity_type.value}s"))


def _ensure_settings(settings: Settings, args: argparse.Namespace):
    if args.parallel_factor == -1:
        return
    if args.parallel_factor == 0:
        settings.parallel_processing_for_unmounted = True
        settings.batch_size_for_parallel[args.scenario] = 0
        return
    if args.parallel_factor == 1:
        settings.parallel_processing_for_unmounted = False
        settings.batch_size_for_parallel[args.scenario] = 1
        return
    if args.parallel_factor > 1:
        settings.parallel_processing_for_unmounted = True
        settings.batch_size_for_parallel[args.scenario] = args.parallel_factor
        return
    raise RuntimeError(f"Unsupported value for --parallel-factor {args.parallel_factor}")


def _get_lock(args: argparse.Namespace, yt_client: YtClientProxy, stage: StageDescription) -> DbLockBase:
    if args.scenario in ("dump_diff", "dump_spec") or args.dry_run:
        return DummyLock()
    if stage.locks:
        return YtSyncLock(yt_client, *stage.locks)
    return DummyLock()


def default_settings(chaos: bool = False) -> Settings:
    return Settings(
        db_type=Settings.CHAOS_DB if chaos else Settings.REPLICATED_DB,
        ensure_folders=True,
        use_deprecated_spec_format=False,
    )


def run_yt_sync(
    name: str,
    description: Description,
    settings: Settings | None = None,
    yt_client_factory_producer: Callable[[bool], YtClientFactory] | None = None,
    log_setup: LogSetup | None = None,
    exit_on_finish: bool = True,
) -> int:
    description_details = DescriptionDetails(description)

    if not description_details.get_all_stages():
        raise RuntimeError("At least one stage required in description")

    logger_setup = log_setup or DefaultLogSetup()
    args = _parse_args(name, description_details, logger_setup)
    logger_setup.setup_logging(args)
    _ensure_args(args, description_details)

    yt_client_factory: YtClientFactory = (
        yt_client_factory_producer(args.dry_run) if yt_client_factory_producer else get_yt_client_factory(args.dry_run)
    )
    yt_sync_settings: Settings = settings or default_settings()
    _ensure_settings(yt_sync_settings, args)

    scenario_params = dict(yson.loads(args.scenario_params.encode())) if args.scenario_params else dict()
    has_changes = False
    for stage in description_details.get_prepared_stage_descriptions(
        args.stage,
        {entity_type: set(getattr(args, f"{entity_type.value}s")) for entity_type in StageEntityType},
    ):
        main_cluster_name = description_details.get_main_cluster(stage)
        main_yt_client = yt_client_factory(main_cluster_name)
        yt_sync = YtSync(
            yt_client_factory=yt_client_factory,
            settings=yt_sync_settings,
            lock=_get_lock(args, main_yt_client, stage),
            scenario_name=args.scenario,
        )
        for entity_type in StageEntityType:
            for spec in stage.get_entities(entity_type).values():
                getattr(yt_sync, f"add_desired_{entity_type.value}")(spec)
        has_changes |= yt_sync.sync(**scenario_params)

    exit_code: int = 2 if (has_changes and args.scenario == "dump_diff") else 0
    if exit_on_finish:
        sys.exit(exit_code)
    return exit_code
