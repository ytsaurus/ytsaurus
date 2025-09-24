import yt.wrapper as yt_
import argparse
from collections import namedtuple
import math
import time


def pretty_bytes(x):
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
        "query_thread_pool_size"
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


def pretty_namedtuple(tuple, indent, formatter=lambda x: x):
    result = ""
    for k, v in zip(tuple._fields, tuple):
        result += f"{' ' * indent}{k}: {formatter(v)}\n"
    return result


def guess_default_config(cpu, memory):
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

    def set(self, path, value):
        value = yt_.yson.dumps(value).decode()
        print(f"Running yt set {path} {value}")

    def remove(self, path):
        print(f"Running yt remove {path}")

    def create(self, type, path=None, *args, **kwargs):
        print(f"Running yt create {type} {path}")

    def __getattr__(self, name):
        return getattr(self.client, name)


class Main:
    def __init__(self, args):
        self.args = args
        self.dry_run = args.dry_run
        self.client = DryRunClient(yt_) if self.dry_run else yt_
        self.zone_path = "//sys/bundle_controller/controller/zones/zone_default"

    def initialize(self):
        if not self.args.no_init_system_directories:
            self.init_basic()

        bc_disabled = False

        def _disable_bc():
            nonlocal bc_disabled
            if bc_disabled:
                return
            print("Will disable bundle controller (yt //sys/@disable_bundle_controller %true)")
            self.client.set("//sys/@disable_bundle_controller", True)
            bc_disabled = True

        try:
            if self.args.init_default_zone or self.args.init_all:
                _disable_bc()
                self.init_zone()

            if self.args.init_nodes or self.args.init_all:
                _disable_bc()
                self.init_nodes()

            if self.args.init_bundles or self.args.init_all:
                _disable_bc()
                self.init_bundles()

            if self.args.init_bundle_system_quotas or self.args.init_all:
                _disable_bc()
                self.init_system_quotas()

            if bc_disabled:
                self.confirm(
                    "Will enable bundle controller (yt //sys/@disable_bundle_controller %true). "
                    "This may cause tablet cell reallocation and temporary bundle failures. Continue?")
                self.client.set("//sys/@disable_bundle_controller", False)
            else:
                print(
                    "The script did not perform any actions, did you forget "
                    "to set necessary flags (perhaps --init-all)?")
        except Exception as e:
            if bc_disabled:
                raise Exception(
                    "WARNING: Bundle controller was disabled and the script failed abnormally. Consider fixing "
                    "the issue and rerunning the script or turning it on manually with "
                    "yt set //sys/@disable_bundle_controller %false") from e
            raise

    def confirm(self, message="Confirm?"):
        if self.dry_run:
            return
        answer = input(f"{message} [y/N] ").lower()
        if answer == "y":
            return True
        raise Exception("Aborting") from None

    def get_node_resource_limits(self):
        return {
            "vcpu": args.cpu * 1000,
            "memory": args.memory,
            "net_bytes": 0,
        }

    def get_proxy_resource_limits(self):
        return {
            "vcpu": args.cpu * 1000,
            "memory": args.memory,
            "net_bytes": 0,
        }

    def init_basic(self):
        dirs = [
            "//sys/bundle_controller/coordinator",
            "//sys/bundle_controller/controller/zones",
            "//sys/bundle_controller/controller/bundles_state",
        ]

        created = False
        for dir in dirs:
            if not self.client.exists(dir):
                print("Creating directory", dir)
                self.client.create("map_node", dir, recursive=True)
                created = True

        if created:
            print("System directories created")

        account = "bundle_system_quotas"
        if not self.client.exists(f"//sys/accounts/{account}"):
            print(f"Creating account \"{account}\"")

            self.client.create("account", attributes={"name": account})

    def init_zone(self):
        assert self.args.cpu is not None, "--cpu must be specified for zone initialization"
        assert self.args.memory is not None, "--memory must be specified for zone initialization"

        node_resource_limits = self.get_node_resource_limits()
        proxy_resource_limits = self.get_proxy_resource_limits()

        cpu_limits, memory_limits = guess_default_config(
            node_resource_limits["vcpu"] // 1000,
            node_resource_limits["memory"])

        print(f"""\
Will initialize zone_default with the following_config:
  node_resource_guarantee:
    vcpu: {node_resource_limits["vcpu"]}
    memory: {pretty_bytes(node_resource_limits["memory"])}
  rpc_proxy_resource_guarantee:
    vcpu: {proxy_resource_limits["vcpu"]}
    memory: {pretty_bytes(proxy_resource_limits["memory"])}
  node_cpu_limits:
    {pretty_namedtuple(cpu_limits, 4).strip()}
  node_memory_limits:
    {pretty_namedtuple(memory_limits, 4, pretty_bytes).strip()}""")
        self.confirm()

        if self.client.exists(self.zone_path):
            self.confirm("Zone already exists, overwriting?")

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

        self.client.create("map_node", self.zone_path, ignore_existing=True)

        for k, v in zone.items():
            self.client.set(f"{self.zone_path}/@{k}", v)

        print("Zone initialization completed\n")

    def init_nodes(self):
        try:
            resources = self.client.get(
                self.zone_path + "/@tablet_node_sizes/regular/resource_guarantee")
        except yt_.YtError:
            raise Exception("Zone is not initialized, cannot initialize nodes") from None

        annotations = {
            "allocated_for_bundle": "spare",
            "allocated": True,
            "resources": resources,
        }

        nodes = self.client.list("//sys/tablet_nodes")

        print(f"Will annotate {len(nodes)} nodes:")
        for n in nodes:
            print(f"  {n}")
        print("Resources:")
        print(f"  vcpu: {resources['vcpu']}")
        print(f"  memory: {pretty_bytes(resources['memory'])}")

        self.confirm()

        for n in nodes:
            self.client.set(f"//sys/cluster_nodes/{n}/@bundle_controller_annotations", annotations)

        print("Nodes initialization completed\n")

    def init_bundles(self):
        try:
            resources = self.client.get(
                self.zone_path + "/@tablet_node_sizes/regular/resource_guarantee")
        except yt_.YtError:
            raise Exception("Zone is not initialized, cannot initialize nodes") from None

        bundles = self.client.list("//sys/tablet_cell_bundles", attributes=["tablet_cell_count"])

        cpu_limits, _ = guess_default_config(
            resources["vcpu"] // 1000,
            resources["memory"])
        cells_per_node = cpu_limits.write_thread_pool_size

        total_node_count = 0
        bundles_with_nodes = []
        for bundle in bundles:
            cell_count = bundle.attributes["tablet_cell_count"]
            node_count = self.args.bundle_node_count.get(
                str(bundle),
                (cell_count + cells_per_node - 1) // cells_per_node)

            print(f"Will give {node_count} nodes to bundle {bundle} with {cell_count} cells")
            total_node_count += node_count
            bundles_with_nodes.append((str(bundle), node_count))

        cluster_node_count = self.client.get("//sys/tablet_nodes/@count")
        if total_node_count > cluster_node_count:
            raise Exception("Not enough nodes for all bundles, need {total_node_count}, have {cluster_node_count}")

        for bundle, node_count in bundles_with_nodes:
            self.init_bundle(bundle, node_count)

        print("Bundles initialization completed\n")

    def init_bundle(self, bundle, node_count):
        try:
            node_resources = self.client.get(
                self.zone_path + "/@tablet_node_sizes/regular/resource_guarantee")
            proxy_resources = self.client.get(
                self.zone_path + "/@rpc_proxy_sizes/regular/resource_guarantee")
            default_config = self.client.get(
                self.zone_path + "/@tablet_node_sizes/regular/default_config")
        except yt_.YtError:
            raise Exception("Zone is not initialized, cannot initialize bundles") from None

        try:
            node_tag_filter = self.client.get(f"//sys/tablet_cell_bundles/{bundle}/@node_tag_filter")
        except yt_.YtError:
            node_tag_filter = ""
        if node_tag_filter and not node_tag_filter.startswith("zone_default/"):
            raise Exception(f"Bundle \"{bundle}\" has nonempty @node_tag_filter")

        attributes = {
            "zone": "zone_default",
            "enable_bundle_controller": True,
            "bundle_controller_target_config": {
                "tablet_node_count": node_count,
                "tablet_node_resource_guarantee": node_resources | {"type": "regular"},
                "cpu_limits": default_config["cpu_limits"],
                "memory_limits": default_config["memory_limits"],
                "rpc_proxy_count": 0,
                "rpc_proxy_resource_guarantee": proxy_resources | {"type": "regular"},
            }
        }

        cell_ids = sorted(self.client.get(
            f"//sys/tablet_cell_bundles/{bundle}/@tablet_cell_ids"))
        expected_cell_count = node_count * default_config["cpu_limits"]["write_thread_pool_size"]

        if expected_cell_count < len(cell_ids):
            print(
                f"Bundle has {len(cell_ids)} tablet cells and should have "
                f"{expected_cell_count} after reconfiguration, "
                f"{len(cell_ids) - expected_cell_count} cells will be removed")
            self.confirm("Confirm tablet cells removal?")

        for cell_id in cell_ids[expected_cell_count:]:
            self.client.remove("#" + cell_id)

        if not self.dry_run:
            pending_cells = set(cell_ids[expected_cell_count:])
            while pending_cells:
                time.sleep(1)
                print(f"Waiting for alive cells: {', '.join(pending_cells)}")
                alive_cells = set()
                for cell_id in pending_cells:
                    if self.client.exists("#" + cell_id):
                        alive_cells.add(cell_id)
                pending_cells = alive_cells

                if not pending_cells:
                    print("All extra tablet cells removed")

        print(f"Setting config for bundle {bundle}")
        for k, v in attributes.items():
            self.client.set(f"//sys/tablet_cell_bundles/{bundle}/@{k}", v)

        self.set_bundle_resource_limits(bundle, node_count)

        print(f"Finished initializing bundle \"{bundle}\"")

    def init_system_quotas(self):
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
            print(f"Will set changelog/snapshot accounts for bundle {bundle}")

        if not affected_bundles:
            return

        self.confirm(
            "Will set changelog/snapshot accounts for bundles. This will cause "
            "temporary tablet cell unavailability. Continue?")

        for bundle in affected_bundles:
            self.init_bundle_system_quotas(bundle)

        print("Finished setting bundle system quotas\n")

    def prepare_system_quotas_account(self, bundle):
        account_name = bundle + "_bundle_system_quotas"
        self.client.create(
            "account",
            attributes={
                "name": account_name,
                "parent_name": "bundle_system_quotas",
            },
            ignore_existing=True)
        return account_name

    def init_bundle_system_quotas(self, bundle):
        account_name = self.prepare_system_quotas_account(bundle)

        options_path = f"//sys/tablet_cell_bundles/{bundle}/@options"
        options = self.client.get(options_path)
        options["changelog_account"] = account_name
        options["snapshot_account"] = account_name
        self.client.set(options_path, options)

    def create_bundle(self, bundle_name):
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
        except yt_.YtError as e:
            if e.is_already_exists():
                self.confirm("Bundle already exists, initialize?")
            else:
                raise

        self.init_bundle(bundle_name, node_count=0)
        print(f"Bundle \"{bundle_name}\" created")

    def drop_stuck_allocations(self, bundle_name):
        state_path = f"//sys/bundle_controller/controller/bundles_state/{bundle_name}/@node_allocations"

        enabled = not self.client.exists("//sys/@disable_bundle_controller") or \
            self.client.get("//sys/@disable_bundle_controller") in ("false", False)
        if enabled:
            self.client.set("//sys/@disable_bundle_controller", True)
            time.sleep(1)
        for allocation in self.client.get(state_path):
            print(f"Will remove allocation {allocation}")
            self.client.remove(
                f"//sys/bundle_controller/internal_allocations/allocation_requests/{allocation}",
                force=True)
        self.client.set(state_path, {})
        if enabled:
            self.client.set("//sys/@disable_bundle_controller", False)
        print("All stuck allocations removed")

    def set_bundle_resource_limits(self, bundle_name, node_count):
        try:
            resources = self.client.get(
                self.zone_path + "/@tablet_node_sizes/regular/resource_guarantee")
        except yt_.YtError:
            raise Exception("Zone is not initialized, cannot set bundle resource limits") from None

        print(f"Setting resource limits for bundle {bundle_name}")
        self.client.set(
            f"//sys/tablet_cell_bundles/{bundle_name}/@resource_limits/cpu",
            node_count * resources["vcpu"] // 1000)
        self.client.set(
            f"//sys/tablet_cell_bundles/{bundle_name}/@resource_limits/memory",
            node_count * resources["memory"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", type=yt_.config.set_proxy, help="YT proxy")
    parser.add_argument("--dry-run", action="store_true", help="do not execute any commands")

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="initialize cluster for Bundle controller")
    init_parser.add_argument("--cpu", type=int, help="amount of CPU per instance")
    init_parser.add_argument("--memory", type=int, help="amount of RAM per instance")
    init_parser.add_argument("--init-all", action="store_true", help="initialize default zone, nodes bundles and system accounts")
    init_parser.add_argument("--init-default-zone", action="store_true", help="initialize default zone")
    init_parser.add_argument("--init-nodes", action="store_true", help="initialize nodes")
    init_parser.add_argument("--init-bundles", action="store_true", help="initialize bundles")
    init_parser.add_argument("--init-bundle-system-quotas", action="store_true", help="initialize bundle system quotas accounts")
    init_parser.add_argument("--no-init-system-directories", action="store_true", help="skip creating system bundle controller directories")
    init_parser.add_argument(
        "--bundle-node-count", default={}, type=lambda x: yt_.yson.loads(x.encode()),
        help="node count override for bundles. Format: {bundle_name_1=node_count; bundle_name_2=node_count; ...}")

    create_bundle_parser = subparsers.add_parser("create-bundle", help="create new bundle")
    create_bundle_parser.add_argument("bundle_name", type=str, help="bundle name")
    create_bundle_parser.add_argument("--force", action="store_true", help="override existing bundle")

    drop_allocations_parser = subparsers.add_parser("drop-allocations", help="delete incorrect allocations from state")
    drop_allocations_parser.add_argument("bundle_name", type=str, help="bundle name")

    set_bundle_resource_limits_parser = subparsers.add_parser(
        "set-bundle-resource-limits",
        help="set \"cpu\" and \"memory\" to \"resource_limits\" attribute of a bundle that correspond "
        "to the provided node count")
    set_bundle_resource_limits_parser.add_argument("bundle_name", type=str, help="bundle name")
    set_bundle_resource_limits_parser.add_argument("node_count", type=int, help="maximum node count")

    args = parser.parse_args()

    runner = Main(args)
    if args.command == "init":
        runner.initialize()
    elif args.command == "create-bundle":
        runner.create_bundle(args.bundle_name)
    elif args.command == "drop-allocations":
        runner.drop_stuck_allocations(args.bundle_name)
    elif args.command == "set-bundle-resource-limits":
        runner.set_bundle_resource_limits(args.bundle_name, args.node_count)
    else:
        assert False, f"Invalid command \"{args.command}\""
