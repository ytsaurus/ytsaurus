"""
Pod size actualization optimization library.

Entry point: run_optimization().
All tunable algorithm parameters are explicit arguments with sensible defaults.
"""

from pathlib import Path

import pandas as pd

from .scripts import shared as cfg
from .catalog import (  # noqa: F401
    build_host_value_df,
    compute_allocation_scarcity_coefficients,
    compute_resource_coefficients,
    generate_container_catalog_proxy,
    generate_container_catalog_old,
)
from .data import (
    build_bundle_groups,
    load_bundle_data,
    load_container_specs,
    load_host_data,
    prepare_numa_host_data,
    validate_host_columns,
)
from .precompute import precompute_n_min_tables
from .optimize import optimize_cpsat, run_beam_refinement
from .simple import optimize_assignment
from .results import (  # noqa: F401
    annotate_assignments_with_validity,
    build_warm_start_from_solution,
    load_warm_start,
    save_progress,
)
from .scripts.shared import disc_round


def _index_bundle_csvs(bundle_csv_paths: list) -> dict:
    """Разложить CSV с метриками бандлов по методу.

    Метод берётся из содержимого, поэтому порядок файлов не важен. Пустой файл —
    нормальный вход: выходов у кубика окон больше, чем бывает периодов.
    """
    paths_by_method = {}
    for path in bundle_csv_paths:
        df_peek = pd.read_csv(path, nrows=1)
        if df_peek.empty:
            print(f"  {path}: пустой файл, пропускаем")
            continue
        method = df_peek['method_name'].iloc[0]
        if method in paths_by_method:
            raise ValueError(f"two bundle CSVs for method {method!r}: " f"{paths_by_method[method]} and {path}")
        paths_by_method[method] = path
    return paths_by_method


def run_simple_optimization(
    resource_coefficients: dict,
    warm_start_data: dict,
    # --- Clusters and data ---
    clusters: list = ("seneca-sas", "seneca-vla", "seneca-klg"),
    methods_to_load: list = (
        "noAggr_SeriesMax_timeDropBelowP90_txThrottler_replication",
        "noAggr_SeriesMax_timeDropBelowP90_txThrottler_seneca_sas_replication",
        "noAggr_SeriesMax_timeDropBelowP95_txThrottler_usual",
    ),
    working_dir: str = ".",
    # --- Explicit file paths (for Nirvana; override working_dir if provided) ---
    bundle_csv_paths: list[str] | None = None,  # one per method, all clusters inside
    node_spec_csv_paths: list[str] | None = None,  # one per cluster
    rpc_spec_csv_paths: list[str] | None = None,  # one per cluster
    fail_on_method_mismatch: bool = False,
    allow_empty: bool = False,
) -> tuple:
    """
    Data loading + optimize_assignment in one call.

    warm_start_data: dict in the same format as run_optimization uses
      (keys: sizes_df, prev_k_node, prev_k_proxy, ...). node_sizes and proxy_sizes
      are extracted from sizes_df sorted by ContainerTypeID.
    resource_coefficients = {"a": ..., "b": ..., "c": ...} — scarcity prices.
    methods_to_load идут от старого к свежему: последний метод — эталон конфигурации.
    fail_on_method_mismatch — падать при расхождении конфигурации между методами.

    Returns (res_df, sizes_df, assign_df).
    """
    import sys

    # When stdout goes to a file (not a TTY), Python uses 8KB block buffering.
    # Switch to line-buffering so every print() is immediately visible in logs.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass  # reconfigure not available on older Python or non-TextIOWrapper stdout

    sizes_df_ws = warm_start_data["sizes_df"].sort_values("ContainerTypeID")
    node_rows = sizes_df_ws[sizes_df_ws["InstanceType"] == "node"]
    proxy_rows = sizes_df_ws[sizes_df_ws["InstanceType"] == "proxy"]
    node_sizes = list(zip(node_rows["CPU"], node_rows["Memory"], node_rows["Network"]))
    proxy_sizes = list(zip(proxy_rows["CPU"], proxy_rows["Memory"], proxy_rows["Network"]))
    config_names = (
        list(node_rows["ConfigName"]) + list(proxy_rows["ConfigName"]) if "ConfigName" in sizes_df_ws.columns else None
    )
    print(f"Extracted {len(node_sizes)} node sizes and {len(proxy_sizes)} proxy sizes from warm_start_data")

    clusters = list(clusters)
    methods_to_load = list(methods_to_load)

    _bundle_file_paths = None
    if bundle_csv_paths is not None:
        _bundle_file_paths = _index_bundle_csvs(bundle_csv_paths)

    print("--- 1. Loading Container Specifications ---")
    node_specs_df, rpc_specs_df = load_container_specs(
        clusters,
        node_spec_paths=node_spec_csv_paths,
        rpc_spec_paths=rpc_spec_csv_paths,
        working_dir=working_dir,
    )

    print("\n--- 2. Loading Bundle Data ---")
    bundle_data = load_bundle_data(
        clusters,
        methods_to_load,
        node_specs_df,
        rpc_specs_df,
        bundle_file_paths=_bundle_file_paths,
        working_dir=working_dir,
        fail_on_method_mismatch=fail_on_method_mismatch,
        allow_empty=allow_empty,
    )

    print("\n--- 3. Building Bundle Groups ---")
    bundle_groups = build_bundle_groups(bundle_data)

    # Цены заданы конфигом, а потребление плывёт: печатаем, во что они обошлись бы,
    # посчитанные по этим данным — так видно, что конфиг пора пересчитать.
    suggested, _ = compute_allocation_scarcity_coefficients(bundle_data)
    print(
        "\nResource prices: "
        f"configured {({k: round(v, 6) for k, v in resource_coefficients.items()})}, "
        f"current allocation suggests {({k: round(v, 6) for k, v in suggested.items()})}"
    )

    print(f"\n--- 4. Assigning sizes: {len(node_sizes)} node, {len(proxy_sizes)} proxy ---")
    res_df, sizes_df, assign_df = optimize_assignment(
        bundle_groups,
        node_sizes,
        proxy_sizes,
        resource_coefficients,
        config_names,
    )
    print(f"Done. container_cost={float(res_df['container_cost'].iloc[0]):.6f}")
    return res_df, sizes_df, assign_df


def run_optimization(
    output_dir: str,
    working_dir: str = ".",
    # --- Explicit file paths (for Nirvana; override working_dir if provided) ---
    # All sequences are order-independent: cluster/method identified from file content.
    host_csv_paths: list[str] | None = None,  # one per cluster ('cluster' column required)
    bundle_csv_paths: (
        list[str] | None
    ) = None,  # one per method, all clusters inside ('cluster'+'method_name' columns required)
    node_spec_csv_paths: list[str] | None = None,  # one per cluster ('cluster' column required)
    rpc_spec_csv_paths: list[str] | None = None,  # one per cluster ('cluster' column required)
    warm_start_data: dict | None = None,  # pre-loaded warm start (overrides warm_start_dir)
    catalog_mandatory_json_data: list[dict] | None = None,  # previous results to pin catalog sizes
    # --- Clusters and data ---
    clusters: list = ("seneca-sas", "seneca-vla", "seneca-klg"),
    methods_to_load: list = (
        "noAggr_SeriesMax_timeDropBelowP90_txThrottler_replication",
        "noAggr_SeriesMax_timeDropBelowP90_txThrottler_seneca_sas_replication",
        "noAggr_SeriesMax_timeDropBelowP95_txThrottler_usual",
    ),
    # --- Problem size ---
    k_node: int = 5,
    k_proxy: int = 2,
    # --- Fixed container ids ---
    fixed_container_ids: list = (),
    # --- Catalog ---
    catalog_node_n: int = 70,
    catalog_proxy_n: int = 70,
    catalog_node_max_cpu: float = 28.0,
    catalog_node_max_mem: float = 200.0,
    catalog_node_max_net: float = 600.0,
    catalog_proxy_max_cpu: float = 12.0,
    catalog_proxy_max_mem: float = 20.0,
    catalog_proxy_max_net: float = 200.0,
    catalog_node_neighborhood_spread: float = 0.2,
    catalog_proxy_neighborhood_spread: float = 0.2,
    # --- Parallelism ---
    parallel_runs: int = 32,
    workers_per_run: int = 4,
    # --- Stage flags and time limits ---
    run_stage0: bool = False,
    run_stage0b: bool = False,
    run_stage1: bool = True,
    run_stage2: bool = True,
    run_stage3: bool = False,
    stage0_time_limit_sec: int = 60,
    stage0b_time_limit_sec: int = 600,
    stage1_time_limit_sec: int = 120,
    stage2_time_limit_sec: int = 120,
    stage3_time_limit_sec: int = 120,
    maximize_container_cost: bool = False,
    # --- Base run tasks ---
    base_tasks_per_stage: int = 32,
    seed_base: int = 10000,
    # --- Iterative refinement ---
    iterative_refinement_enabled: bool = True,
    refine_iters: int = 30,
    refine_max_changed_sizes: int | None = None,
    refine_tasks_per_stage: int = 32,
    refine_seed_base: int = 20000,
    beam_count: int = 1,
    # --- Debug ---
    verbose: bool = False,
):
    import sys

    # When stdout goes to a file (not a TTY), Python uses 8KB block buffering.
    # Switch to line-buffering so every print() is immediately visible in logs.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass  # reconfigure not available on older Python or non-TextIOWrapper stdout

    clusters = list(clusters)
    methods_to_load = list(methods_to_load)
    fixed_container_ids = list(fixed_container_ids)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # --- Build file path dicts if explicit paths provided ---
    _bundle_file_paths = None
    if bundle_csv_paths is not None:
        _bundle_file_paths = _index_bundle_csvs(bundle_csv_paths)

    # --- Load data ---
    print("--- 1. Loading and Preparing Data ---")
    hosts_df = load_host_data(clusters, file_paths=host_csv_paths, working_dir=working_dir)
    validate_host_columns(hosts_df)
    hosts = prepare_numa_host_data(hosts_df)

    print("\n--- 1a. Loading Container Specifications ---")
    node_specs_df, rpc_specs_df = load_container_specs(
        clusters,
        node_spec_paths=node_spec_csv_paths,
        rpc_spec_paths=rpc_spec_csv_paths,
        working_dir=working_dir,
    )
    bundle_data = load_bundle_data(
        clusters,
        methods_to_load,
        node_specs_df,
        rpc_specs_df,
        bundle_file_paths=_bundle_file_paths,
        working_dir=working_dir,
    )

    print("\n--- 2. Building bundle groups ---")
    bundle_groups = build_bundle_groups(bundle_data)

    # --- Scarcity prices ---
    print("\n--- 3. Computing scarcity prices ---")
    resource_coefficients, rc_debug = compute_resource_coefficients(bundle_data, hosts)
    print("Demand (int):", rc_debug["demand_int"])
    print("Supply  (int):", rc_debug["supply_int"])
    print("Total NUMA nodes:", rc_debug["total_numa_nodes"])
    print("Avg per NUMA (int):", {k: round(v, 1) for k, v in rc_debug["avg_per_numa_int"].items()})
    print("Raw prices:", {k: round(v, 6) for k, v in rc_debug["raw_prices"].items()})
    print("Weighted prices:", {k: round(v, 8) for k, v in rc_debug["weighted_prices"].items()})
    print("Coefficients:", {k: round(v, 6) for k, v in resource_coefficients.items()})

    host_value_df = build_host_value_df(hosts, resource_coefficients)
    print("Sample NUMA node values:")
    print(host_value_df.head(10).to_string(index=False))

    # --- Collect mandatory catalog sizes ---
    _mand_node_sizes, _mand_proxy_sizes = set(), set()

    def _collect_mandatory_from_sizes_df(sizes_df):
        for _, row in sizes_df.iterrows():
            entry = (
                disc_round(float(row['CPU']), cfg.CPU_STEP),
                disc_round(float(row['Memory']), cfg.MEM_STEP),
                disc_round(float(row['Network']), cfg.NET_STEP),
            )
            if row['InstanceType'] == 'node':
                _mand_node_sizes.add(entry)
            else:
                _mand_proxy_sizes.add(entry)

    # From pre-loaded JSON results (Nirvana path)
    if catalog_mandatory_json_data:
        for result_dict in catalog_mandatory_json_data:
            if result_dict.get("sizes"):
                _collect_mandatory_from_sizes_df(pd.DataFrame(result_dict["sizes"]))

    # Pin warm-start sizes into the catalog so hints land on exact entries
    if warm_start_data is not None and "sizes_df" in warm_start_data:
        _collect_mandatory_from_sizes_df(warm_start_data["sizes_df"])

    if _mand_node_sizes:
        print(f"Mandatory node sizes for catalog: {len(_mand_node_sizes)}")
    if _mand_proxy_sizes:
        print(f"Mandatory proxy sizes for catalog: {len(_mand_proxy_sizes)}")

    # --- Generate catalogs ---
    print("\n--- 3c. Generating container catalogs ---")
    node_cat_cpu, node_cat_mem, node_cat_net, node_ws_mask = generate_container_catalog_proxy(
        instance_type='node',
        resource_coefficients=resource_coefficients,
        n_limit=catalog_node_n,
        max_cpu=catalog_node_max_cpu,
        max_mem=catalog_node_max_mem,
        max_net=catalog_node_max_net,
        mandatory_sizes=list(_mand_node_sizes) if _mand_node_sizes else None,
        neighborhood_spread=catalog_node_neighborhood_spread,
    )
    proxy_cat_cpu, proxy_cat_mem, proxy_cat_net, proxy_ws_mask = generate_container_catalog_proxy(
        instance_type='proxy',
        resource_coefficients=resource_coefficients,
        n_limit=catalog_proxy_n,
        max_cpu=catalog_proxy_max_cpu,
        max_mem=catalog_proxy_max_mem,
        max_net=catalog_proxy_max_net,
        mandatory_sizes=list(_mand_proxy_sizes) if _mand_proxy_sizes else None,
        neighborhood_spread=catalog_proxy_neighborhood_spread,
    )
    print(f"Node catalog:  {len(node_cat_cpu)} entries (+{sum(node_ws_mask)} warm-start)")
    print(f"Proxy catalog: {len(proxy_cat_cpu)} entries (+{sum(proxy_ws_mask)} warm-start)")

    # Save catalog
    def _snap(v_int, step, container_step):
        return round(v_int * step / container_step) * container_step

    catalog_df = pd.DataFrame(
        {
            "InstanceType": (["node"] * len(node_cat_cpu)) + (["proxy"] * len(proxy_cat_cpu)),
            "CPU": (
                [_snap(v, cfg.CPU_STEP, cfg.NODE_CONTAINER_CPU_STEP) for v in node_cat_cpu]
                + [_snap(v, cfg.CPU_STEP, cfg.PROXY_CONTAINER_CPU_STEP) for v in proxy_cat_cpu]
            ),
            "Memory": (
                [_snap(v, cfg.MEM_STEP, cfg.NODE_CONTAINER_MEM_STEP) for v in node_cat_mem]
                + [_snap(v, cfg.MEM_STEP, cfg.PROXY_CONTAINER_MEM_STEP) for v in proxy_cat_mem]
            ),
            "Network": (
                [_snap(v, cfg.NET_STEP, cfg.NODE_CONTAINER_NET_STEP) for v in node_cat_net]
                + [_snap(v, cfg.NET_STEP, cfg.PROXY_CONTAINER_NET_STEP) for v in proxy_cat_net]
            ),
            "WarmStart": node_ws_mask + proxy_ws_mask,
        }
    )
    catalog_df.to_csv(output_path / "catalog.csv", index=False)

    # --- Precompute n_min tables ---
    all_clusters = sorted({c for bg in bundle_groups for c in bg.bundles_by_cluster})
    n_min_tables, infeasible_catalog = precompute_n_min_tables(
        bundle_groups,
        all_clusters,
        node_cat_cpu,
        node_cat_mem,
        node_cat_net,
        proxy_cat_cpu,
        proxy_cat_mem,
        proxy_cat_net,
    )

    # --- Common catalog kwargs ---
    catalog_kw = dict(
        node_catalog_cpu=node_cat_cpu,
        node_catalog_mem=node_cat_mem,
        node_catalog_net=node_cat_net,
        proxy_catalog_cpu=proxy_cat_cpu,
        proxy_catalog_mem=proxy_cat_mem,
        proxy_catalog_net=proxy_cat_net,
    )

    # --- Run optimization ---
    print(f"\n--- 4. Optimization: K_node={k_node}, K_proxy={k_proxy} ---")

    prefix = f"kn{k_node}_kp{k_proxy}"

    def hook(stage, res_df_, sizes_df_, assign_df_, patterns_df_, phys_hosts_df_):
        save_progress(output_path, prefix, res_df_, sizes_df_, assign_df_, patterns_df_, phys_hosts_df_, tag=stage)
        if not res_df_.empty:
            hc = float(res_df_['host_cost'].iloc[0])
            cc = float(res_df_.get('container_cost', pd.Series([float('nan')])).iloc[0])
            st = res_df_['status'].iloc[0] if 'status' in res_df_.columns else '?'
            print(f"     [{stage}] status={st}  host_cost={hc:.4f}  container_cost={cc:.4f}")

    if warm_start_data is not None:
        ws_data = warm_start_data  # pre-loaded (e.g. from Nirvana JSON input)
    else:
        ws_data = None

    # Base run
    beam = optimize_cpsat(
        bundle_groups=bundle_groups,
        hosts=hosts,
        K_node=k_node,
        K_proxy=k_proxy,
        resource_coefficients=resource_coefficients,
        **catalog_kw,
        slot_cfgs=[{"warm_start_data": ws_data, "prev_sizes_df": None, "max_changed_sizes": None}],
        top_k=beam_count,
        parallel_runs=parallel_runs,
        workers_per_run=workers_per_run,
        tasks_per_stage=base_tasks_per_stage,
        seed_base=seed_base,
        run_stage0=run_stage0,
        stage0_time_limit_sec=stage0_time_limit_sec,
        run_stage0b=run_stage0b,
        stage0b_time_limit_sec=stage0b_time_limit_sec,
        run_stage1=run_stage1,
        stage1_time_limit_sec=stage1_time_limit_sec,
        run_stage2=run_stage2,
        stage2_time_limit_sec=stage2_time_limit_sec,
        run_stage3=run_stage3,
        stage3_time_limit_sec=stage3_time_limit_sec,
        stage2_maximize_container_cost=maximize_container_cost,
        progress_hook=hook,
        verbose=verbose,
        globally_fixed_container_ids=fixed_container_ids or None,
        n_min_tables=n_min_tables,
        infeasible_catalog=infeasible_catalog,
    )

    if not beam:
        print("Base run found no feasible solution")
        return None

    # Iterative refinement
    if iterative_refinement_enabled:
        beam = run_beam_refinement(
            initial_beam=beam,
            bundle_groups=bundle_groups,
            hosts=hosts,
            K_node=k_node,
            K_proxy=k_proxy,
            resource_coefficients=resource_coefficients,
            **catalog_kw,
            beam_count=beam_count,
            refine_iters=refine_iters,
            max_changed_sizes=refine_max_changed_sizes,
            parallel_runs=parallel_runs,
            workers_per_run=workers_per_run,
            tasks_per_stage=refine_tasks_per_stage,
            seed_base=refine_seed_base,
            run_stage0=False,
            run_stage1=run_stage1,
            stage1_time_limit_sec=stage1_time_limit_sec,
            run_stage2=run_stage2,
            stage2_time_limit_sec=stage2_time_limit_sec,
            run_stage3=run_stage3,
            stage3_time_limit_sec=stage3_time_limit_sec,
            progress_hook=hook,
            globally_fixed_container_ids=fixed_container_ids or None,
            on_improvement=lambda b: save_progress(
                output_path,
                prefix,
                b["res_df"],
                b["sizes_df"],
                b["assign_df"],
                b["patterns_df"],
                b["phys_hosts_df"],
                tag="best",
            ),
            verbose=verbose,
            n_min_tables=n_min_tables,
            infeasible_catalog=infeasible_catalog,
        )

    if beam:
        best = beam[0]
        save_progress(
            output_path,
            prefix,
            best["res_df"],
            best["sizes_df"],
            best["assign_df"],
            best["patterns_df"],
            best["phys_hosts_df"],
            tag="best",
        )
        print(f"\nBest host_cost: {best['host_cost']:.6f}")

    return beam
