import yt.logger as logger
from yt.admin.metrics.config import DEFAULT_STEP, MetricsSpec
from yt.admin.metrics.promql import UNRESOLVED_VAR_RE, extract_selectors

import json
import os
from typing import Any, Dict, List, Tuple


class SpecLoader:
    def __init__(self, spec_path: str):
        try:
            import yaml
        except ImportError as e:
            from yt.admin.helpers import install_hint
            raise ImportError(
                "The 'yaml' package is required for SpecLoader. "
                f"Install it with: {install_hint('yaml')}"
            ) from e
        if not os.path.isfile(spec_path):
            raise FileNotFoundError(f"Spec file not found: {spec_path}")
        self.spec_path = os.path.abspath(spec_path)
        with open(spec_path) as f:
            self.raw = yaml.safe_load(f) or {}

    def load(self) -> MetricsSpec:
        step = self.raw.get("defaults", {}).get("step", DEFAULT_STEP)
        selectors: List[str] = []
        dashboards: List[Tuple[str, Dict[str, Any]]] = []

        for target in self.raw.get("targets", []) or []:
            target_type = target.get("type")
            if target_type == "dashboard":
                dashboard_name, dashboard_selectors, dashboard_data = self._load_dashboard(target)
                selectors.extend(dashboard_selectors)
                dashboards.append((dashboard_name, dashboard_data))
            elif target_type == "metric":
                query = target.get("query")
                if query:
                    selectors.extend(extract_selectors(query))
            else:
                logger.warning(f"Unknown target type: {target_type!r}")
        return MetricsSpec(selectors=selectors, dashboards=dashboards, step=step, raw=self.raw)

    def _load_dashboard(self, target: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, Any]]:
        path = target.get("path")
        params = {key: str(value) for key, value in (target.get("params") or {}).items()}
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(self.spec_path), path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Dashboard file not found: {path}")
        with open(path) as f:
            dashboard = json.load(f)

        variables: Dict[str, str] = {}
        for templating in dashboard.get("templating", {}).get("list", []):
            name = templating.get("name")
            current = templating.get("current", {}).get("text")
            if name and current is not None:
                variables[name] = str(current)
        variables.update(params)

        selectors: List[str] = []
        for raw_expr in walk_exprs(dashboard.get("panels", [])):
            expr = raw_expr
            for key, value in sorted(variables.items(), key=lambda x: len(x[0]), reverse=True):
                expr = expr.replace("${" + key + "}", value).replace("$" + key, value)
            unresolved = sorted({u for u in UNRESOLVED_VAR_RE.findall(expr) if u.lstrip("$").strip("{}") not in {"__rate_interval", "__interval"}})
            if unresolved:
                logger.warning(f"Dashboard {os.path.basename(path)}: unresolved variables {unresolved}")
            selectors.extend(extract_selectors(expr))
        return os.path.basename(path), selectors, dashboard


def walk_exprs(node: Any) -> List[str]:
    out: List[str] = []
    if isinstance(node, dict):
        if isinstance(node.get("expr"), str):
            out.append(node["expr"])
        for v in node.values():
            out.extend(walk_exprs(v))
    elif isinstance(node, list):
        for item in node:
            out.extend(walk_exprs(item))
    return out


def params_by_dashboard(spec: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    for target in spec.get("targets", []) or []:
        if target.get("type") != "dashboard":
            continue
        path = target.get("path")
        if not path:
            continue
        result[os.path.basename(path)] = {k: str(v) for k, v in (target.get("params") or {}).items()}
    return result
