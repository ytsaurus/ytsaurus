from yt.admin.metrics.promql import escape_label_value

from typing import IO, Any, Dict, Set, Tuple


class OpenMetricsWriter:
    def __init__(self, stream):
        self._stream: IO[bytes] = stream
        self._names_seen: Set[str] = set()
        self._series_seen: Set[Tuple[Tuple[str, str], ...]] = set()
        self.samples = 0
        self.duplicate_series = 0

    def write_series(self, series: Dict[str, Any]) -> None:
        metric = series.get("metric", {})
        name = metric.get("__name__")

        series_key = tuple(sorted((str(k), str(v)) for k, v in metric.items()))
        if series_key in self._series_seen:
            self.duplicate_series += 1
            return
        self._series_seen.add(series_key)

        if name not in self._names_seen:
            self._write(f"# HELP {name} \n")
            self._write(f"# TYPE {name} gauge\n")
            self._names_seen.add(name)
        label_parts = [
            f'{k}="{escape_label_value(v)}"'
            for k, v in sorted(metric.items()) if k != "__name__"
        ]
        label_block = ("{" + ",".join(label_parts) + "}") if label_parts else ""
        for ts_seconds, raw_val in series.get("values", []):
            if not isinstance(raw_val, str):
                # histogram objects is not supported (https://github.com/prometheus/prometheus/blob/v3.11.3/web/api/v1/openapi_schemas.go#L417)
                continue
            self._write(f"{name}{label_block} {raw_val} {float(ts_seconds)}\n")
            self.samples += 1

    def finalize(self) -> None:
        self._write("# EOF\n")

    def _write(self, text: str) -> None:
        self._stream.write(text.encode("utf-8"))
