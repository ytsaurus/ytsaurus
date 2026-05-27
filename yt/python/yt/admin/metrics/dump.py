import yt.logger as logger
import yt.packages.requests as requests
from yt.admin.metrics.config import MetricsDumpConfig
from yt.admin.metrics.openmetrics import OpenMetricsWriter
from yt.admin.metrics.prometheus import PrometheusClient
from yt.admin.metrics.promql import eliminate_subsets, extract_selectors
from yt.admin.metrics.spec import SpecLoader

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import json
import os
import sys
import time
import zipfile


def _confirm(prompt: str) -> bool:
    try:
        return input(prompt).strip().lower() in ("y", "yes")
    except EOFError:
        return False


def _selectors_from_extra_targets(targets: Optional[List[str]]) -> List[str]:
    out: List[str] = []
    for raw in targets or []:
        data: Dict[str, Any] = {}
        try:
            data = json.loads(raw)
        except json.decoder.JSONDecodeError as ex:
            logger.warning(f"Failed to load target {raw}: {ex}")
            continue
        if data.get("type") == "metric" and data.get("query"):
            out.extend(extract_selectors(data["query"]))
        else:
            logger.warning(f"Unknown target: {json.dumps(data)}")
    return out


class MetricsDumper:
    def __init__(self, prom: PrometheusClient):
        self.prom = prom

    def run(self, cfg: MetricsDumpConfig) -> None:
        spec = SpecLoader(cfg.spec_path).load()
        selectors = eliminate_subsets(spec.selectors + _selectors_from_extra_targets(cfg.extra_targets))
        if not selectors:
            logger.error("No selectors to dump")
            return

        if cfg.to_ts <= cfg.from_ts:
            raise ValueError("--to-ts must be greater than --from-ts")

        if os.path.exists(cfg.output) and not cfg.force:
            if not _confirm(f"{cfg.output} exists, overwrite? [y/N] "):
                logger.info("Aborted")
                return

        start, end = cfg.from_ts.timestamp(), cfg.to_ts.timestamp()
        step = cfg.step or spec.step

        estimate_at = min(end, time.time())
        if not self._confirm_volume(selectors, estimate_at, cfg.max_series, cfg.force):
            logger.info("Aborted")
            return

        meta = {
            "from_ts": cfg.from_ts.isoformat(),
            "to_ts": cfg.to_ts.isoformat(),
            "start_unix": start,
            "end_unix": end,
            "step": step,
            "selectors": selectors,
            "spec": spec.raw,
            "dumped_at": datetime.now(timezone.utc).isoformat(),
        }
        succeeded, samples = self._stream_archive(cfg.output, selectors, start, end, step, spec.dashboards, meta)
        logger.info(f"Dumped {succeeded}/{len(selectors)} queries, {samples} samples to {cfg.output}")

    def _confirm_volume(self, selectors: List[str], at: float, max_series: int, force: bool) -> bool:
        logger.info(f"Estimating volume for {len(selectors)} selectors")
        counts = {sel: self.prom.count_series(sel, at) for sel in selectors}
        total = sum(c for c in counts.values())
        for sel, cnt in counts.items():
            marker = "?" if cnt < 0 else str(cnt)
            logger.info(f"  series={marker:>10s}  {sel}")
        logger.info(f"Total series at to_ts: {total}")
        if total > max_series and not force:
            return _confirm(f"Total series {total} > --max-series {max_series}. Continue? [y/N]: ")
        return True

    def _stream_archive(
        self, output: str, selectors: List[str], start: float, end: float, step: str,
        dashboards: List[Tuple[str, Dict[str, Any]]], meta: Dict[str, Any],
    ) -> Tuple[int, int]:
        succeeded = 0
        samples = 0
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zip_file:
            with zip_file.open("data.om", "w", force_zip64=True) as om_stream:
                writer = OpenMetricsWriter(om_stream)
                for i, selector in enumerate(selectors):
                    logger.info(f"Querying [{i + 1}/{len(selectors)}] {selector}")
                    try:
                        data = self.prom.query_range(selector, start, end, step)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Failed to query {selector}: {e}")
                        response = getattr(e, "response", None)
                        if response is not None and response.status_code in (401, 403):
                            logger.error(
                                "Got %d from Prometheus. "
                                "Try set PROMETHEUS_USER and PROMETHEUS_PASSWORD environment variables.",
                                response.status_code,
                            )
                            sys.exit(1)
                        continue
                    if data.get("status") != "success":
                        logger.error(f"Prometheus error for {selector}: {data}")
                        continue
                    for series in data.get("data", {}).get("result", []):
                        writer.write_series(series)
                    succeeded += 1
                writer.finalize()
                if writer.duplicate_series:
                    logger.info(f"Skipped {writer.duplicate_series} duplicate series")
                samples = writer.samples
            for name, data in dashboards:
                zip_file.writestr(f"dashboards/{name}", json.dumps(data, indent=2))
            zip_file.writestr("meta.json", json.dumps(meta, indent=2))
        return succeeded, samples
