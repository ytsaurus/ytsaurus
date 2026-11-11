#!/usr/bin/env python3
"""Ручной запуск collect_bundle_usage_by_period.sql через query tracker.

В Нирване параметры и пути выходов подставляет YQL-процессор. Здесь тот же
запрос запускается вне графа: каждый `DECLARE $x AS T;` переписывается в
`$x = <значение>;`, так что .sql остаётся единственным местом с логикой.
"""

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

QUERY_PATH = Path(__file__).with_name("collect_bundle_usage_by_period.sql")
DECLARE_RE = re.compile(r"^DECLARE\s+\$(\w+)\s+AS\s+[^;]+;", re.MULTILINE)
OUTPUTS = ("metrics_0", "metrics_1", "metrics_2", "node_specs", "rpc_specs")
DEFAULT_EXPIRATION_TIMEOUT = 7 * 24 * 60 * 60 * 1000


def literal(value) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return repr(value)


def render_data_center_files(query: str, file_names) -> str:
    # FileContent requires a constant name during YQL type annotation.
    contents = ", ".join(f"FileContent({literal(name)})" for name in file_names)
    if not contents:
        raise ValueError("at least one data center file is required")
    return query.replace("/* DATA_CENTER_FILES */", contents)


def render(args) -> str:
    values = {
        "end_date": args.end_date,
        "period_days": args.period_days,
        "periods": args.periods,
        "quantile": args.quantile,
        "usage_nodes": args.usage_nodes,
        "usage_proxies": args.usage_proxies,
        "nodes_spec": args.nodes_spec,
        "proxies_spec": args.proxies_spec,
        "bundle_spec": args.bundle_spec,
    }
    values.update({name: f"{args.output_prefix}_{name}" for name in OUTPUTS})

    seen = set()

    def substitute(m):
        name = m.group(1)
        if name not in values:
            sys.exit(f"нет значения для параметра ${name}")
        seen.add(name)
        return f"${name} = {literal(values[name])};"

    text, n = DECLARE_RE.subn(
        substitute, render_data_center_files(QUERY_PATH.read_text(), [Path(path).name for path in args.data_centers])
    )
    if not n:
        sys.exit(f"в {QUERY_PATH} не нашлось ни одного DECLARE")
    unused = set(values) - seen
    if unused:
        sys.exit(f"параметры без DECLARE в запросе: {', '.join(sorted(unused))}")

    # Пул в файле не зашит: в графе его выставляет опция yqlp_yt_pool.
    if args.pool:
        text = text.replace("USE kolmogorov;", f'PRAGMA yt.Pool = "{args.pool}";\nUSE kolmogorov;', 1)
    return text


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--end-date", default="", help="последний день окна, YYYY-MM-DD (UTC); по умолчанию вчера")
    p.add_argument("--period-days", type=int, default=7, help="длина периода в днях")
    p.add_argument("--periods", type=int, default=3, help="сколько периодов брать, 1..3")
    p.add_argument("--quantile", type=float, default=98.75, help="квантиль по времени внутри периода")
    p.add_argument(
        "--data-centers",
        required=True,
        action="append",
        help="локальный YSON-файл с ДЦ от collect_bc_info; повторите для каждой группы",
    )
    p.add_argument(
        "--output-prefix",
        default="//home/yt-analytics/dynamic-tables/pod-size-actualization/tmp/bundle-usage",
        help="префикс путей выходов: <prefix>_metrics_0, ..., <prefix>_node_specs",
    )
    p.add_argument("--proxy", default="kolmogorov")
    p.add_argument("--pool", default="yt-dev")
    p.add_argument(
        "--expiration-timeout",
        type=int,
        default=DEFAULT_EXPIRATION_TIMEOUT,
        help="TTL выходных таблиц без обращений, миллисекунды; 0 отключает TTL",
    )
    p.add_argument("--yt-cli", default="yt", help="yt-клиент с поддержкой query (0.15+); в системном может не быть")
    p.add_argument("--usage-nodes", default="//home/bigdata_dwh/meta_dwh/dds/yt_tablet_nodes_monium_usage_log/1d")
    p.add_argument("--usage-proxies", default="//home/bigdata_dwh/meta_dwh/dds/yt_rpc_proxy_monium_usage_log/1d")
    p.add_argument("--nodes-spec", default="//home/bigdata_dwh/meta_dwh/dds/yt_bundle_controller_tablet_nodes_log/1d")
    p.add_argument("--proxies-spec", default="//home/bigdata_dwh/meta_dwh/dds/yt_rpc_proxy_spec_log/1d")
    p.add_argument("--bundle-spec", default="//home/bigdata_dwh/meta_dwh/dds/dds_yt_bundle_spec_log/1d")
    p.add_argument("--dry-run", action="store_true", help="только напечатать запрос")
    args = p.parse_args()

    file_names = [Path(path).name for path in args.data_centers]
    if len(set(file_names)) != len(file_names):
        p.error("имена YSON-файлов data-centers должны быть уникальными")
    files = [
        {"name": Path(path).name, "content": Path(path).read_text(), "type": "raw_inline_data"}
        for path in args.data_centers
    ]
    # CLI structured arguments use YSON, while file contents remain opaque strings.
    files_yson = (
        "["
        + "".join(
            "{" + "".join(f"{json.dumps(key)}={json.dumps(value)};" for key, value in file.items()) + "};"
            for file in files
        )
        + "]"
    )
    query = render(args)
    if args.dry_run:
        print(query)
        return

    yt = [args.yt_cli, "--proxy", args.proxy]
    print(
        f"окно: {args.periods} x {args.period_days} дн. по {args.end_date or 'вчера'}, "
        f"квантиль {args.quantile} -> {args.output_prefix}_*",
        flush=True,
    )

    def run(*argv) -> str:
        done = subprocess.run(yt + list(argv), capture_output=True, text=True)
        if done.returncode:
            sys.exit(done.stderr.strip() or f"yt {' '.join(argv[:2])}: код {done.returncode}")
        return done.stdout.strip()

    # Запускаем асинхронно: ссылка нужна сразу, запрос идёт минуты.
    # --async без --format падает с "You should specify format".
    query_id = run("query", "yql", query, "--files", files_yson, "--async", "--format", "json")
    print(f"запрос: https://yt.yandex-team.ru/{args.proxy}/queries/{query_id}", flush=True)

    while True:
        info = json.loads(run("get-query", query_id, "--format", "json"))
        state = info["state"]
        if state in ("completed", "failed", "aborted"):
            break
        time.sleep(15)
    print(f"состояние: {state}")
    if state != "completed":
        sys.exit(json.dumps(info.get("error", info), ensure_ascii=False, indent=2))

    for name in OUTPUTS:
        path = f"{args.output_prefix}_{name}"
        if args.expiration_timeout > 0:
            run("set", f"{path}/@expiration_timeout", str(args.expiration_timeout))
        print(f"   {path}: строк {run('get', f'{path}/@row_count')}")


if __name__ == "__main__":
    main()
