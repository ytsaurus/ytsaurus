import yt.logger as logger
import yt.wrapper as yt
from yt.admin._experimental import warn_experimental, EXPERIMENTAL_HELP_SUFFIX
from yt.admin.helpers import confirm

import argparse
import math
import time
from collections import namedtuple
from typing import Any, Dict, Optional, Tuple


ZONE_PATH = "//sys/bundle_controller/controller/zones/zone_default"


def pretty_bytes(x: int) -> str:
    if x < 2**10:
        return str(x)
    elif x < 2**20:
        return f"{x / 2**10:.02f}".rstrip("0").rstrip(".") + " KB"
    elif x < 2**30:
        return f"{x / 2**20:.02f}".rstrip("0").rstrip(".") + " MB"
    else:
        return f"{x / 2**30:.02f}".rstrip("0").rstrip(".") + " GB"


CpuLimits = namedtuple(
    "CpuLimits",
    [
        "write_thread_pool_size",
        "lookup_thread_pool_size",
        "query_thread_pool_size",
    ])


MemoryLimits = namedtuple(
    "MemoryLimits",
    [
        "tablet_dynamic",
        "tablet_static",
        "compressed_block_cache",
        "uncompressed_block_cache",
        "versioned_chunk_meta",
        "lookup_row_cache",
        "reserved",
    ])


def pretty_namedtuple(tuple, indent, formatter=lambda x: x) -> str:
    result = ""
    for k, v in zip(tuple._fields, tuple):
        result += f"{' ' * indent}{k}: {formatter(v)}\n"
    return result


def guess_default_config(cpu: int, memory: int) -> Tuple[CpuLimits, MemoryLimits]:
    cpu_limits = None
    if cpu <= 4:
        cpu_limits = CpuLimits(1, 1, 1)
    elif cpu <= 10:
        cpu_limits = CpuLimits(5, 2, 2)
    else:
        cpu_limits = CpuLimits(10, 6, 6)

    def _round(x):
        if x >= 4 * 2**30:
            return int(math.round(x / 2**30) * 2**30)
        else:
            return int(math.round(x / (2**30 // 10)) * (2**30 // 10))

    reserved = max(2**30, round(memory * 0.15))
    tablet_dynamic = round(memory * 0.1)
    block_cache = round(memory * 0.08)
    versioned_chunk_meta = round(memory * 0.1)
    lookup_row_cache = min(100 * 2**20, round(memory * 0.1))
    tablet_static = memory - (
        reserved + tablet_dynamic + versioned_chunk_meta + block_cache * 2 + lookup_row_cache)
    assert tablet_static >= 0

    memory_limits = MemoryLimits(
        tablet_dynamic,
        tablet_static,
        block_cache,
        block_cache,
        versioned_chunk_meta,
        lookup_row_cache,
        reserved)

    return cpu_limits, memory_limits


class DryRunClient:
    def __init__(self, client):
        self.client = client

    def set(self, path, value, *args, **kwargs) -> None:
        logger.info(f"Would run yt set {path} {yt.yson.dumps(value).decode()}")

    def remove(self, path, *args, **kwargs) -> None:
        logger.info(f"Would run yt remove {path}")

    def create(self, type, path=None, *args, **kwargs) -> None:
        logger.info(f"Would run yt create {type} {path}")

    def __getattr__(self, name):
        return getattr(self.client, name)


class BundleController:
    def __init__(self, client: Optional[yt.YtClient], dry_run: bool, yes: bool):
        self.dry_run = dry_run
        self.yes = yes
        client = client if client is not None else yt
        self.client = DryRunClient(client) if dry_run else client

    def _confirm(self, message: str = "Confirm?") -> None:
        if not confirm(message, assume_yes=self.yes or self.dry_run):
            raise Exception("Aborting") from None

    def initialize(
        self,
        cpu: Optional[int],
        memory: Optional[int],
        init_all: bool,
        init_default_zone: bool,
        init_nodes: bool,
        init_bundles: bool,
        init_bundle_system_quotas: bool,
        no_init_system_directories: bool,
        bundle_node_count: Dict[str, int],
    ) -> None:
        if not no_init_system_directories:
            self.init_basic()

        bc_disabled = False

        def _disable_bc():
            nonlocal bc_disabled
            if bc_disabled:
                return
            logger.info("Will disable bundle controller (yt set //sys/@disable_bundle_controller %true)")
            self.client.set("//sys/@disable_bundle_controller", True)
            bc_disabled = True

        try:
            if init_default_zone or init_all:
                _disable_bc()
                self.init_zone(cpu, memory)

            if init_nodes or init_all:
                _disable_bc()
                self.init_nodes()

            if init_bundles or init_all:
                _disable_bc()
                self.init_bundles(bundle_node_count)

            if init_bundle_system_quotas or init_all:
                _disable_bc()
                self.init_system_quotas()

            if bc_disabled:
                self._confirm(
                    "Will enable bundle controller (yt set //sys/@disable_bundle_controller %false). "
                    "This may cause tablet cell reallocation and temporary bundle failures. Continue?")
                self.client.set("//sys/@disable_bundle_controller", False)
            else:
                logger.warning(
                    "The script did not perform any actions, did you forget "
                    "to set necessary flags (perhaps \"init --init-all\")?")
        except Exception as e:
            if bc_disabled:
                raise Exception(
                    "WARNING: Bundle controller was disabled and the script failed abnormally. Consider fixing "
                    "the issue and rerunning the script or turning it on manually with "
                    "yt set //sys/@disable_bundle_controller %false") from e
            raise

    def get_node_resource_limits(self, cpu: int, memory: int) -> Dict[str, Any]:
        return {
            "vcpu": cpu * 1000,
            "memory": memory,
            "net_bytes": 0,
        }

    def get_proxy_resource_limits(self, cpu: int, memory: int) -> Dict[str, Any]:
        return {
            "vcpu": cpu * 1000,
            "memory": memory,
            "net_bytes": 0,
        }

    def init_basic(self) -> None:
        dirs = [
            "//sys/bundle_controller/coordinator",
            "//sys/bundle_controller/controller/zones",
            "//sys/bundle_controller/controller/bundles_state",
        ]

        created = False
        for dir in dirs:
            if not self.client.exists(dir):
                logger.info(f"Creating directory {dir}")
                self.client.create("map_node", dir, recursive=True)
                created = True

        if created:
            logger.info("System directories created")

        account = "bundle_system_quotas"
        if not self.client.exists(f"//sys/accounts/{account}"):
            logger.info(f"Creating account \"{account}\"")
            self.client.create("account", attributes={"name": account})

    def init_zone(self, cpu: Optional[int], memory: Optional[int]) -> None:
        assert cpu is not None, "--cpu must be specified for zone initialization"
        assert memory is not None, "--memory must be specified for zone initialization"

        node_resource_limits = self.get_node_resource_limits(cpu, memory)
        proxy_resource_limits = self.get_proxy_resource_limits(cpu, memory)

        cpu_limits, memory_limits = guess_default_config(
            node_resource_limits["vcpu"] // 1000,
            node_resource_limits["memory"])

        logger.info(
            "Will initialize zone_default with the following config:\n"
            "  node_resource_guarantee:\n"
            f"    vcpu: {node_resource_limits['vcpu']}\n"
            f"    memory: {pretty_bytes(node_resource_limits['memory'])}\n"
            "  rpc_proxy_resource_guarantee:\n"
            f"    vcpu: {proxy_resource_limits['vcpu']}\n"
            f"    memory: {pretty_bytes(proxy_resource_limits['memory'])}\n"
            "  node_cpu_limits:\n"
            f"    {pretty_namedtuple(cpu_limits, 4).strip()}\n"
            "  node_memory_limits:\n"
            f"    {pretty_namedtuple(memory_limits, 4, pretty_bytes).strip()}")
        self._confirm()

        if self.client.exists(ZONE_PATH):
            self._confirm("Zone already exists, overwriting?")

        zone = {
            "tablet_node_sizes": {
                "regular": {
                    "default_config": {
                        "cpu_limits": cpu_limits._asdict(),
                        "memory_limits": memory_limits._asdict(),
                    },
                    "resource_guarantee": node_resource_limits,
                },
            },
            "rpc_proxy_sizes": {
                "regular": {
                    "resource_guarantee": proxy_resource_limits,
                },
            },
        }

        self.client.create("map_node", ZONE_PATH, ignore_existing=True)

        for k, v in zone.items():
            self.client.set(f"{ZONE_PATH}/@{k}", v)

        logger.info("Zone initialization completed")

    def _get_node_resource_guarantee(self, action: str) -> Dict[str, Any]:
        try:
            return self.client.get(ZONE_PATH + "/@tablet_node_sizes/regular/resource_guarantee")
        except yt.YtError:
            raise Exception(f"Zone is not initialized, cannot {action}") from None

    def init_nodes(self) -> None:
        resources = self._get_node_resource_guarantee("initialize nodes")

        annotations = {
            "allocated_for_bundle": "spare",
            "allocated": True,
            "resources": resources,
        }

        nodes = self.client.list("//sys/tablet_nodes")

        logger.info(f"Will annotate {len(nodes)} nodes:")
        for n in nodes:
            logger.info(f"  {n}")
        logger.info("Resources:")
        logger.info(f"  vcpu: {resources['vcpu']}")
        logger.info(f"  memory: {pretty_bytes(resources['memory'])}")

        self._confirm()

        for n in nodes:
            self.client.set(f"//sys/cluster_nodes/{n}/@bundle_controller_annotations", annotations)

        logger.info("Nodes initialization completed")

    def init_bundles(self, bundle_node_count: Dict[str, int]) -> None:
        resources = self._get_node_resource_guarantee("initialize bundles")

        bundles = self.client.list("//sys/tablet_cell_bundles", attributes=["tablet_cell_count"])

        cpu_limits, _ = guess_default_config(
            resources["vcpu"] // 1000,
            resources["memory"])
        cells_per_node = cpu_limits.write_thread_pool_size

        total_node_count = 0
        bundles_with_nodes = []
        for bundle in bundles:
            cell_count = bundle.attributes["tablet_cell_count"]
            node_count = bundle_node_count.get(
                str(bundle),
                (cell_count + cells_per_node - 1) // cells_per_node)

            logger.info(
                f"Will give {node_count} nodes to bundle {bundle} with {cell_count} cells")
            total_node_count += node_count
            bundles_with_nodes.append((str(bundle), node_count))

        cluster_node_count = self.client.get("//sys/tablet_nodes/@count")
        if total_node_count > cluster_node_count:
            raise Exception(
                f"Not enough nodes for all bundles, need {total_node_count}, have {cluster_node_count}")

        for bundle, node_count in bundles_with_nodes:
            self.init_bundle(bundle, node_count)

        logger.info("Bundles initialization completed")

    def init_bundle(self, bundle: str, node_count: int) -> None:
        try:
            node_resources = self.client.get(
                ZONE_PATH + "/@tablet_node_sizes/regular/resource_guarantee")
            proxy_resources = self.client.get(
                ZONE_PATH + "/@rpc_proxy_sizes/regular/resource_guarantee")
            default_config = self.client.get(
                ZONE_PATH + "/@tablet_node_sizes/regular/default_config")
        except yt.YtError:
            raise Exception("Zone is not initialized, cannot initialize bundles") from None

        try:
            node_tag_filter = self.client.get(f"//sys/tablet_cell_bundles/{bundle}/@node_tag_filter")
        except yt.YtError:
            node_tag_filter = ""
        if node_tag_filter and not node_tag_filter.startswith("zone_default/"):
            raise Exception(f"Bundle \"{bundle}\" has nonempty @node_tag_filter")

        attributes = {
            "zone": "zone_default",
            "enable_bundle_controller": True,
            "bundle_controller_target_config": {
                "tablet_node_count": node_count,
                "tablet_node_resource_guarantee": {**node_resources, "type": "regular"},
                "cpu_limits": default_config["cpu_limits"],
                "memory_limits": default_config["memory_limits"],
                "rpc_proxy_count": 0,
                "rpc_proxy_resource_guarantee": {**proxy_resources, "type": "regular"},
            }
        }

        cell_ids = sorted(self.client.get(
            f"//sys/tablet_cell_bundles/{bundle}/@tablet_cell_ids"))
        expected_cell_count = node_count * default_config["cpu_limits"]["write_thread_pool_size"]

        if expected_cell_count < len(cell_ids):
            logger.info(
                f"Bundle has {len(cell_ids)} tablet cells and should have "
                f"{expected_cell_count} after reconfiguration, "
                f"{len(cell_ids) - expected_cell_count} cells will be removed")
            self._confirm("Confirm tablet cells removal?")

        for cell_id in cell_ids[expected_cell_count:]:
            self.client.remove("#" + cell_id)

        if not self.dry_run:
            pending_cells = set(cell_ids[expected_cell_count:])
            while pending_cells:
                time.sleep(1)
                logger.info(f"Waiting for alive cells: {', '.join(pending_cells)}")
                alive_cells = set()
                for cell_id in pending_cells:
                    if self.client.exists("#" + cell_id):
                        alive_cells.add(cell_id)
                pending_cells = alive_cells

                if not pending_cells:
                    logger.info("All extra tablet cells removed")

        logger.info(f"Setting config for bundle {bundle}")
        for k, v in attributes.items():
            self.client.set(f"//sys/tablet_cell_bundles/{bundle}/@{k}", v)

        self.set_bundle_resource_limits(bundle, node_count)

        logger.info(f"Finished initializing bundle \"{bundle}\"")

    def init_system_quotas(self) -> None:
        bundles = self.client.list("//sys/tablet_cell_bundles", attributes=["options"])

        affected_bundles = []
        for bundle in bundles:
            options = bundle.attributes["options"]
            if (
                options["changelog_account"].endswith("bundle_system_quotas") and
                options["snapshot_account"].endswith("bundle_system_quotas")
            ):
                continue
            affected_bundles.append(bundle)
            logger.info(f"Will set changelog/snapshot accounts for bundle {bundle}")

        if not affected_bundles:
            return

        self._confirm(
            "Will set changelog/snapshot accounts for bundles. This will cause "
            "temporary tablet cell unavailability. Continue?")

        for bundle in affected_bundles:
            self.init_bundle_system_quotas(bundle)

        logger.info("Finished setting bundle system quotas")

    def prepare_system_quotas_account(self, bundle: str) -> str:
        account_name = bundle + "_bundle_system_quotas"
        self.client.create(
            "account",
            attributes={
                "name": account_name,
                "parent_name": "bundle_system_quotas",
            },
            ignore_existing=True)
        return account_name

    def init_bundle_system_quotas(self, bundle: str) -> None:
        account_name = self.prepare_system_quotas_account(bundle)

        options_path = f"//sys/tablet_cell_bundles/{bundle}/@options"
        options = self.client.get(options_path)
        options["changelog_account"] = account_name
        options["snapshot_account"] = account_name
        self.client.set(options_path, options)

    def create_bundle(self, bundle_name: str) -> None:
        account_name = self.prepare_system_quotas_account(bundle_name)
        options = {
            "changelog_account": account_name,
            "snapshot_account": account_name,
        }
        try:
            self.client.create(
                "tablet_cell_bundle",
                attributes={
                    "name": bundle_name,
                    "options": options,
                })
        except yt.YtError as e:
            if e.is_already_exists():
                self._confirm("Bundle already exists, initialize?")
            else:
                raise

        self.init_bundle(bundle_name, node_count=0)
        logger.info(f"Bundle \"{bundle_name}\" created")

    def drop_stuck_allocations(self, bundle_name: str) -> None:
        state_path = f"//sys/bundle_controller/controller/bundles_state/{bundle_name}/@node_allocations"

        enabled = not self.client.exists("//sys/@disable_bundle_controller") or \
            self.client.get("//sys/@disable_bundle_controller") in ("false", False)
        if enabled:
            self.client.set("//sys/@disable_bundle_controller", True)
            time.sleep(1)
        for allocation in self.client.get(state_path):
            logger.info(f"Will remove allocation {allocation}")
            self.client.remove(
                f"//sys/bundle_controller/internal_allocations/allocation_requests/{allocation}",
                force=True)
        self.client.set(state_path, {})
        if enabled:
            self.client.set("//sys/@disable_bundle_controller", False)
        logger.info("All stuck allocations removed")

    def set_bundle_resource_limits(self, bundle_name: str, node_count: int) -> None:
        resources = self._get_node_resource_guarantee("set bundle resource limits")

        logger.info(f"Setting resource limits for bundle {bundle_name}")
        self.client.set(
            f"//sys/tablet_cell_bundles/{bundle_name}/@resource_limits/cpu",
            node_count * resources["vcpu"] // 1000)
        self.client.set(
            f"//sys/tablet_cell_bundles/{bundle_name}/@resource_limits/memory",
            node_count * resources["memory"])


@warn_experimental
def run_bundle_controller_init(
    cpu, memory, init_all, init_default_zone, init_nodes, init_bundles,
    init_bundle_system_quotas, no_init_system_directories, bundle_node_count,
    dry_run, yes, client: Optional[yt.YtClient] = None, **_,
) -> None:
    BundleController(client, dry_run, yes).initialize(
        cpu=cpu,
        memory=memory,
        init_all=init_all,
        init_default_zone=init_default_zone,
        init_nodes=init_nodes,
        init_bundles=init_bundles,
        init_bundle_system_quotas=init_bundle_system_quotas,
        no_init_system_directories=no_init_system_directories,
        bundle_node_count=bundle_node_count,
    )


@warn_experimental
def run_bundle_controller_create_bundle(bundle_name, dry_run, yes, client: Optional[yt.YtClient] = None, **_) -> None:
    BundleController(client, dry_run, yes).create_bundle(bundle_name)


@warn_experimental
def run_bundle_controller_drop_allocations(bundle_name, dry_run, yes, client: Optional[yt.YtClient] = None, **_) -> None:
    BundleController(client, dry_run, yes).drop_stuck_allocations(bundle_name)


@warn_experimental
def run_bundle_controller_set_resource_limits(bundle_name, node_count, dry_run, yes, client: Optional[yt.YtClient] = None, **_) -> None:
    BundleController(client, dry_run, yes).set_bundle_resource_limits(bundle_name, node_count)


def _add_common_arguments(parser) -> None:
    parser.add_argument("--dry-run", action="store_true", help="do not execute any commands, only log them")
    parser.add_argument("--yes", "-y", action="store_true", help="skip interactive confirmation prompts")


def _parse_bundle_node_count(value: str) -> Dict[str, int]:
    return yt.yson.loads(value.encode())


def _add_init_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "init",
        help="initialize cluster for Bundle controller",
        description="Initialize cluster for Bundle controller. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_bundle_controller_init)
    parser.add_argument("--cpu", type=int, help="amount of CPU per instance")
    parser.add_argument("--memory", type=int, help="amount of RAM per instance")
    parser.add_argument("--init-all", action="store_true", help="initialize default zone, nodes, bundles and system accounts")
    parser.add_argument("--init-default-zone", action="store_true", help="initialize default zone")
    parser.add_argument("--init-nodes", action="store_true", help="initialize nodes")
    parser.add_argument("--init-bundles", action="store_true", help="initialize bundles")
    parser.add_argument("--init-bundle-system-quotas", action="store_true", help="initialize bundle system quotas accounts")
    parser.add_argument("--no-init-system-directories", action="store_true", help="skip creating system bundle controller directories")
    parser.add_argument(
        "--bundle-node-count", default={}, type=_parse_bundle_node_count,
        help="node count override for bundles. Format: {bundle_name_1=node_count; bundle_name_2=node_count; ...}")
    _add_common_arguments(parser)


def _add_create_bundle_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "create-bundle",
        help="create new bundle",
        description="Create new bundle. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_bundle_controller_create_bundle)
    parser.add_argument("bundle_name", type=str, help="bundle name")
    _add_common_arguments(parser)


def _add_drop_allocations_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "drop-allocations",
        help="delete incorrect allocations from state",
        description="Delete incorrect allocations from state. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_bundle_controller_drop_allocations)
    parser.add_argument("bundle_name", type=str, help="bundle name")
    _add_common_arguments(parser)


def _add_set_resource_limits_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "set-bundle-resource-limits",
        help="set \"cpu\" and \"memory\" to \"resource_limits\" attribute of a bundle "
             "that correspond to the provided node count",
        description="Set \"cpu\" and \"memory\" to \"resource_limits\" attribute of a bundle "
                    "that correspond to the provided node count. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_bundle_controller_set_resource_limits)
    parser.add_argument("bundle_name", type=str, help="bundle name")
    parser.add_argument("node_count", type=int, help="maximum node count")
    _add_common_arguments(parser)


def add_bundle_controller_subparsers(subparsers) -> None:
    _add_init_subparser(subparsers)
    _add_create_bundle_subparser(subparsers)
    _add_drop_allocations_subparser(subparsers)
    _add_set_resource_limits_subparser(subparsers)


def add_bundle_controller_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "bundle-controller",
        help="Manage Bundle controller",
        description="Manage Bundle controller. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    bundle_controller_subparsers = parser.add_subparsers()
    bundle_controller_subparsers.required = True
    add_bundle_controller_subparsers(bundle_controller_subparsers)
