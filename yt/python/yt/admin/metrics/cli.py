import yt.logger as logger
from yt.admin._experimental import warn_experimental, EXPERIMENTAL_HELP_SUFFIX
from yt.admin.metrics.config import DEFAULT_MAX_SERIES, MetricsDumpConfig, MetricsReplayConfig
from yt.admin.metrics.dump import MetricsDumper, _selectors_from_extra_targets
from yt.admin.metrics.prometheus import PrometheusClient
from yt.admin.metrics.promql import eliminate_subsets
from yt.admin.metrics.replay import MetricsReplayer
from yt.admin.metrics.spec import SpecLoader

import argparse
from datetime import datetime, timezone


@warn_experimental
def run_metrics_validate(spec, target, **_) -> None:
    parsed = SpecLoader(spec).load()
    selectors = eliminate_subsets(parsed.selectors) + _selectors_from_extra_targets(target)
    logger.info(f"Found {len(selectors)} unique selectors (from {len(parsed.selectors)} raw)")
    for s in selectors:
        print(s)


@warn_experimental
def run_metrics_dump(spec, from_ts, to_ts, prometheus_url, step, target, output, max_series, force, **_) -> None:
    safe_from_ts = from_ts.replace("Z", "+00:00")
    safe_to_ts = to_ts.replace("Z", "+00:00")

    cfg = MetricsDumpConfig(
        spec_path=spec,
        from_ts=datetime.fromisoformat(safe_from_ts),
        to_ts=datetime.fromisoformat(safe_to_ts),
        step=step,
        extra_targets=target,
        output=output,
        max_series=max_series,
        force=force,
    )
    MetricsDumper(PrometheusClient(prometheus_url)).run(cfg)


@warn_experimental
def run_metrics_replay(archive, prometheus_port, grafana_port, **_) -> None:
    cfg = MetricsReplayConfig(
        archive=archive,
        prometheus_port=prometheus_port,
        grafana_port=grafana_port,
    )
    MetricsReplayer().run(cfg)


def _add_metrics_validate_arguments(parser) -> None:
    parser.add_argument("--spec", type=str, required=True, help="Path to spec.yaml")
    parser.add_argument("--target", type=str, action="append", default=None, help='Additional inline target as JSON, e.g. \'{"type":"metric","query":"yt_x"}\'')


def _add_metrics_dump_arguments(parser) -> None:
    current_time = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    parser.add_argument("--spec", type=str, required=True, help="Path to spec.yaml")
    parser.add_argument("--from-ts", type=str, required=True, metavar="ISO8601", help=f"Start timestamp (i.e. {current_time})")
    parser.add_argument("--to-ts", type=str, required=True, metavar="ISO8601", help=f"End timestamp (i.e. {current_time})")
    parser.add_argument("--prometheus-url", type=str, required=True, help="Prometheus base URL")
    parser.add_argument("--step", type=str, default=None, help="Step (overrides spec defaults.step)")
    parser.add_argument("--target", type=str, action="append", default=None, help='Additional inline target as JSON, e.g. \'{"type":"metric","query":"yt_x"}\'')
    parser.add_argument("--output", type=str, default="metrics.zip", help="Output archive path")
    parser.add_argument("--max-series", type=int, default=DEFAULT_MAX_SERIES, help="Confirmation threshold for total series count")
    parser.add_argument("--force", action="store_true", help="Skip confirmation when over --max-series")


def _add_metrics_replay_arguments(parser) -> None:
    parser.add_argument("archive", type=str, help="Path to metrics archive (.zip)")
    parser.add_argument("--prometheus-port", type=int, default=None, help="Port to expose Prometheus on (default: Docker picks a free port)")
    parser.add_argument("--grafana-port", type=int, default=None, help="Port to expose Grafana on (default: Docker picks a free port)")


def _add_validate_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "validate",
        help="Validate metrics spec and print unique selectors",
        description="Validate metrics spec and print unique selectors. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_metrics_validate)
    _add_metrics_validate_arguments(parser)


def _add_dump_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "dump",
        help="Dump metrics from Prometheus into an archive",
        description=(
            "Dump metrics from Prometheus into an archive. "
            "Auth via PROMETHEUS_USER/PROMETHEUS_PASSWORD env. " + EXPERIMENTAL_HELP_SUFFIX
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_metrics_dump)
    _add_metrics_dump_arguments(parser)


def _add_replay_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "replay",
        help="Serve a dumped metrics archive over Prometheus + Grafana",
        description="Run local Prometheus and Grafana containers from a dumped metrics archive. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_metrics_replay)
    _add_metrics_replay_arguments(parser)


def add_metrics_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "metrics",
        help="Metrics tools",
        description="Metrics tools (validate / dump / replay). " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    metrics_subparsers = parser.add_subparsers()
    metrics_subparsers.required = True
    _add_validate_subparser(metrics_subparsers)
    _add_dump_subparser(metrics_subparsers)
    _add_replay_subparser(metrics_subparsers)
