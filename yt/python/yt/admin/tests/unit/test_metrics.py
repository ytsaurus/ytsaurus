from yt.admin.metrics.config import DashboardInfo, MetricsDumpConfig
from yt.admin.metrics.dump import MetricsDumper, _selectors_from_extra_targets
from yt.admin.metrics.grafana import build_dashboard_url
from yt.admin.metrics.openmetrics import OpenMetricsWriter
from yt.admin.metrics.promql import (
    _parse_matchers_from_tokens,
    _parse_selector,
    _selector_subsumes,
    _tokenize,
    _value_subsumes,
    eliminate_subsets,
    escape_label_value,
    extract_selectors,
)
from yt.admin.metrics.spec import SpecLoader, params_by_dashboard, walk_exprs

import pytest

import json
import os
import zipfile
from datetime import datetime, timezone


class TestPromql:
    @pytest.mark.parametrize(
        "expr, expected",
        [
            ("metric[5m]", ["metric"]),
            ("metric[5m offset 1h]", ["metric"]),
            ("rate(foo[1m])[5m:]", ["foo"]),
            ("sum by (pod) (metric)", ["metric"]),
            ("metric without (pod)", ["metric"]),
            ("time() + 1", []),
            ("absent(foo)", ["foo"]),
            ('{job="node"}', ['{job="node"}']),
            (
                r"""
            (
                topk(10,
                    (
                        rate({
                            cluster=~"someclustername",
                            service=~"yt-tablet-node.*",
                            __name__=~"yt_action_queue_time_cumulative",
                            tablet_cell_bundle=~"sys",
                            pod=~".*",
                            thread=~".+"
                        }[$__rate_interval])
                    )
                    /
                    ({
                        cluster=~"someclustername",
                        service=~"yt-tablet-node.*",
                        __name__=~"yt_resource_tracker_thread_count",
                        tablet_cell_bundle=~"sys",
                        pod=~".*",
                        thread=~".+"
                    })
                )
            )
            """,
                [
                    '{__name__=~"yt_action_queue_time_cumulative",cluster=~"someclustername",pod=~".*",service=~"yt-tablet-node.*",tablet_cell_bundle=~"sys",thread=~".+"}',
                    '{__name__=~"yt_resource_tracker_thread_count",cluster=~"someclustername",pod=~".*",service=~"yt-tablet-node.*",tablet_cell_bundle=~"sys",thread=~".+"}',
                ],
            ),
            (
                'sum(rate(foo_total{pod="p1",cluster=~"someclustername"}[5m])) + bar_total + vector(1) + time()',
                [
                    'foo_total{cluster=~"someclustername",pod="p1"}',
                    "bar_total",
                ],
            ),
            ('{cluster="someclustername", service=~"yt-master.*"}', ['{cluster="someclustername",service=~"yt-master.*"}']),
            (r'foo_total{b="x\"y",a="back\\slash",c="line\nbreak"}', [r'foo_total{a="back\\slash",b="x\"y",c="linenbreak"}']),
            ("foo{a='single quoted',b=`raw\\value`}", [r'foo{a="single quoted",b="raw\\value"}']),
            ("rate(foo_total[5m]) + increase(bar_total[1h])", ["foo_total", "bar_total"]),
        ],
    )
    def test_extract_selectors(self, expr, expected):
        assert extract_selectors(expr) == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            ('plain', 'plain'),
            ('a"b', r'a\"b'),
            (r'a\b', r'a\\b'),
            ("a\nb", r"a\nb"),
        ],
    )
    def test_escape_label_value(self, value, expected):
        assert escape_label_value(value) == expected

    @pytest.mark.parametrize(
        "expr, expected_tokens",
        [
            ("  foo_bar123  ", [("IDENT", "foo_bar123")]),
            (r'"double"', [("STRING", "double")]),
            (r"'single'", [("STRING", "single")]),
            (r"`backtick\n`", [("STRING", r"backtick\n")]),
            (r'"esc\"ape"', [("STRING", 'esc"ape')]),
            (r"'esc\'ape'", [("STRING", "esc'ape")]),
            ("{}(),[]", [("LBRACE", "{"), ("RBRACE", "}"), ("LPAREN", "("), ("RPAREN", ")"), ("COMMA", ","), ("LBRACKET", "["), ("RBRACKET", "]")]),
            ("= != =~ !~", [("OP", "="), ("OP", "!="), ("OP", "=~"), ("OP", "!~")]),
            (
                "a + b - c / d * e",
                [
                    ("IDENT", "a"),
                    ("OTHER", "+"),
                    ("IDENT", "b"),
                    ("OTHER", "-"),
                    ("IDENT", "c"),
                    ("OTHER", "/"),
                    ("IDENT", "d"),
                    ("OTHER", "*"),
                    ("IDENT", "e"),
                ],
            ),
            ('metric{label=~"val"}', [("IDENT", "metric"), ("LBRACE", "{"), ("IDENT", "label"), ("OP", "=~"), ("STRING", "val"), ("RBRACE", "}")]),
        ],
    )
    def test_tokenize_all_paths(self, expr, expected_tokens):
        assert _tokenize(expr) == expected_tokens

    @pytest.mark.parametrize(
        "expr, expected_matchers, expected_end_index",
        [
            ('{a="1", b=~"2"}', [("a", "=", "1"), ("b", "=~", "2")], 9),
            ('{a="1" b="2"}', [("a", "=", "1"), ("b", "=", "2")], 8),
            ('{a > "1"}', [], 5),
            ('{a = }', [], 4),
            ('{a="1"', [("a", "=", "1")], 4),
            ('{"a"="1"}', [], 5),
        ],
    )
    def test_parse_matchers_and_recovery(self, expr, expected_matchers, expected_end_index):
        tokens = _tokenize(expr)
        matchers, end_idx = _parse_matchers_from_tokens(tokens, 0)
        assert matchers == expected_matchers
        assert end_idx == expected_end_index

    @pytest.mark.parametrize(
        "selector, expected_metric, expected_matchers",
        [
            ('foo_total{a="1", b=~"2"}', "foo_total", {"a": ("=", "1"), "b": ("=~", "2")}),
            ('foo_total', "foo_total", {}),
            ('{a="1"}', None, {"a": ("=", "1")}),
            ('', None, {}),
        ],
    )
    def test_parse_selector(self, selector, expected_metric, expected_matchers):
        metric, matchers = _parse_selector(selector)
        assert metric == expected_metric
        assert matchers == expected_matchers

    @pytest.mark.parametrize(
        "a_op, a_val, b_op, b_val, expected",
        [
            ("=", "x", "=", "x", True),
            ("=", "x", "=", "y", False),
            ("!=", "x", "!=", "x", True),
            ("!=", "x", "!=", "y", False),
            ("=~", ".*", "=", "foo", True),
            ("=~", "f.*", "=", "foo", True),
            ("=~", "b.*", "=", "foo", False),
            ("=~", ".*", "=~", "anything", True),
            ("=~", ".+", "=~", "anything", True),
            ("=~", "exact", "=~", "exact", True),
            ("=~", "exact", "=~", "other", False),
            ("=~", "[invalid", "=", "foo", False),
            ("=", "x", "!=", "x", False),
            ("!=", "x", "=", "x", False),
        ],
    )
    def test_value_subsumes(self, a_op, a_val, b_op, b_val, expected):
        assert _value_subsumes(a_op, a_val, b_op, b_val) is expected

    @pytest.mark.parametrize(
        "a, b, expected",
        [
            ('foo{a="1"}', 'foo{a="1"}', True),
            ('foo{a="1"}', 'bar{a="1"}', False),
            ('foo', 'foo{a="1"}', True),
            ('foo{a=~".*"}', 'foo{a="1", b="2"}', True),
            ('foo{a="1"}', 'foo', False),
            ('foo{a="1", b="2"}', 'foo{a="1"}', False),
            ('foo{a="1"}', 'foo{a="2"}', False),
        ],
    )
    def test_selector_subsumes(self, a, b, expected):
        assert _selector_subsumes(a, b) is expected

    @pytest.mark.parametrize(
        "selectors, expected",
        [
            (
                [
                    'yt_accounts{account=~".*"}',
                    'yt_accounts{account="sys"}',
                    'yt_accounts{account="tmp"}',
                    'other_metric{account="sys"}',
                ],
                [
                    'yt_accounts{account=~".*"}',
                    'other_metric{account="sys"}',
                ],
            ),
            (
                [
                    'a{host=~".*"}',
                    'b{host="host-1"}',
                ],
                [
                    'a{host=~".*"}',
                    'b{host="host-1"}',
                ],
            ),
            (
                [
                    'foo{a="1"}',
                    'foo{a="1"}',
                    'foo{a="2"}',
                ],
                [
                    'foo{a="1"}',
                    'foo{a="2"}',
                ],
            ),
            (
                [
                    'yt_accounts{account=~".*"}',
                    'yt_accounts{account="sys"}',
                    'yt_accounts{account="tmp"}',
                    'yt_accounts{account="sys", pool="default"}',
                ],
                [
                    'yt_accounts{account=~".*"}',
                ],
            ),
            (
                [
                    'metric',
                    'metric{job="node"}',
                    'metric{job="node", pod="1"}',
                ],
                [
                    'metric',
                ],
            ),
            (
                [
                    'metric{a=~"["}',
                    'metric{a="1"}',
                ],
                [
                    'metric{a="1"}',
                    'metric{a=~"["}',
                ],
            ),
            (
                [
                    'metric{a="1"}',
                    '{__name__="metric", a="1"}',
                ],
                [
                    'metric{a="1"}',
                ],
            ),
            (
                [
                    '{__name__=~"metric.*", a="1"}',
                    'metric{a="1"}',
                ],
                [
                    '{__name__=~"metric.*", a="1"}',
                ],
            ),
            (
                [
                    'yt_accounts{account="sys"}',
                    '{__name__="yt_accounts", account=~".*"}',
                ],
                [
                    '{__name__="yt_accounts", account=~".*"}',
                ],
            ),
        ],
    )
    def test_eliminate_subsets(self, selectors, expected):
        assert eliminate_subsets(selectors) == expected


class TestSpecLoader:
    def test_walk_exprs(self):
        dashboard_fragment = {
            "panels": [
                {
                    "targets": [
                        {"expr": "metric_a"},
                        {"expr": "metric_b"},
                    ],
                    "nested": {
                        "expr": "metric_c",
                    },
                },
                {
                    "rows": [
                        {
                            "panels": [
                                {"expr": "metric_d"},
                            ],
                        },
                    ],
                },
            ],
        }

        assert walk_exprs(dashboard_fragment) == [
            "metric_a",
            "metric_b",
            "metric_c",
            "metric_d",
        ]

    def test_loads_metric_targets(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
defaults:
  step: 30s
targets:
- type: metric
  query: foo_total{cluster="someclustername",pod=~"p.*"}
- type: metric
  query: bar_total
""")

        spec = SpecLoader(str(spec_path)).load()

        assert spec.step == "30s"
        assert spec.selectors == [
            'foo_total{cluster="someclustername",pod=~"p.*"}',
            "bar_total",
        ]
        assert spec.dashboards == []
        assert spec.raw["defaults"]["step"] == "30s"

    def test_uses_default_step_when_missing(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: foo_total
""")

        spec = SpecLoader(str(spec_path)).load()

        assert spec.step == "15s"
        assert spec.selectors == ["foo_total"]

    def test_loads_dashboard_targets_replaces_variables_and_keeps_dashboard(self, tmp_path):
        dashboard_path = tmp_path / "scheduler-pool.json"
        dashboard = {
            "uid": "scheduler-pool",
            "title": "Scheduler Pool",
            "templating": {
                "list": [
                    {
                        "name": "cluster",
                        "current": {"text": "default-cluster"},
                    },
                    {
                        "name": "pool",
                        "current": {"text": "default-pool"},
                    },
                ],
            },
            "panels": [
                {
                    "targets": [
                        {
                            "expr": 'rate({__name__=~"yt_scheduler_pool_metric",cluster=~"$cluster",pool=~"${pool}"}[$__rate_interval])',
                        },
                    ],
                },
            ],
        }
        dashboard_path.write_text(json.dumps(dashboard))

        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
defaults:
  step: 10s
targets:
- type: dashboard
  path: scheduler-pool.json
  params:
    cluster: someclustername
- type: metric
  query: direct_metric{pod="p1"}
""")

        spec = SpecLoader(str(spec_path)).load()

        assert spec.step == "10s"
        assert spec.selectors == [
            '{__name__=~"yt_scheduler_pool_metric",cluster=~"someclustername",pool=~"default-pool"}',
            'direct_metric{pod="p1"}',
        ]
        assert spec.dashboards == [
            ("scheduler-pool.json", dashboard),
        ]

    def test_resolves_dashboard_path_relative_to_spec(self, tmp_path):
        subdir = tmp_path / "dashboards"
        subdir.mkdir()

        dashboard_path = subdir / "d.json"
        dashboard_path.write_text(
            json.dumps(
                {
                    "panels": [
                        {
                            "targets": [
                                {"expr": "foo_total"},
                            ],
                        },
                    ],
                }
            )
        )

        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: dashboard
  path: dashboards/d.json
""")

        spec = SpecLoader(str(spec_path)).load()

        assert spec.selectors == ["foo_total"]
        assert spec.dashboards[0][0] == "d.json"

    def test_params_by_dashboard(self):
        spec = {
            "targets": [
                {
                    "type": "dashboard",
                    "path": "dashboards/scheduler-pool.json",
                    "params": {
                        "cluster": "someclustername",
                        "pool": "foo",
                    },
                },
                {
                    "type": "metric",
                    "query": "foo_total",
                },
            ],
        }

        assert params_by_dashboard(spec) == {
            "scheduler-pool.json": {
                "cluster": "someclustername",
                "pool": "foo",
            },
        }


class TestGrafana:
    @pytest.mark.parametrize(
        "dashboard, variables, from_ms, to_ms, expected",
        [
            (
                DashboardInfo(file="scheduler-pool.json", uid="scheduler-pool", title="Scheduler Pool", slug="scheduler-pool"),
                {"pool": "foo", "cluster": "someclustername"},
                1000,
                2000,
                "http://localhost:3000/d/scheduler-pool/scheduler-pool?from=1000&to=2000&var-cluster=someclustername&var-pool=foo",
            ),
            (
                DashboardInfo(file="d.json", uid="uid", title="Title", slug="title"),
                {"b": "2", "a": "1"},
                None,
                None,
                "http://localhost:3000/d/uid/title?var-a=1&var-b=2",
            ),
        ],
    )
    def test_build_dashboard_url(self, dashboard, variables, from_ms, to_ms, expected):
        assert build_dashboard_url("http://localhost:3000", dashboard, variables, from_ms=from_ms, to_ms=to_ms) == expected


class TestOpenMetrics:
    class BytesCollector:
        def __init__(self):
            self.parts = []

        def write(self, data):
            self.parts.append(data)

        def text(self):
            return b"".join(self.parts).decode("utf-8")

    def test_writes_help_type_samples_and_eof(self):
        stream = self.BytesCollector()
        writer = OpenMetricsWriter(stream)

        writer.write_series(
            {
                "metric": {
                    "__name__": "foo_total",
                    "pod": "p1",
                    "cluster": "someclustername",
                },
                "values": [
                    [1000.0, "1"],
                    [1015.0, "2"],
                ],
            }
        )
        writer.write_series(
            {
                "metric": {
                    "__name__": "foo_total",
                    "pod": "p2",
                    "cluster": "someclustername",
                },
                "values": [
                    [1000.0, "3"],
                ],
            }
        )
        writer.finalize()

        assert writer.samples == 3

        text = stream.text()
        assert text.count("# HELP foo_total") == 1
        assert text.count("# TYPE foo_total gauge") == 1
        assert 'foo_total{cluster="someclustername",pod="p1"} 1 1000.0' in text
        assert 'foo_total{cluster="someclustername",pod="p1"} 2 1015.0' in text
        assert 'foo_total{cluster="someclustername",pod="p2"} 3 1000.0' in text
        assert text.endswith("# EOF\n")

    def test_escapes_label_values(self):
        stream = self.BytesCollector()
        writer = OpenMetricsWriter(stream)

        writer.write_series(
            {
                "metric": {
                    "__name__": "foo_total",
                    "label": 'quote"backslash\\newline\n',
                },
                "values": [
                    [1.0, "42"],
                ],
            }
        )
        writer.finalize()

        assert r'foo_total{label="quote\"backslash\\newline\n"} 42 1.0' in stream.text()


class TestDump:
    class FakePrometheusClient:
        def __init__(self):
            self.count_series_calls = []
            self.query_range_calls = []

        def count_series(self, selector, at):
            self.count_series_calls.append((selector, at))
            return 1

        def query_range(self, selector, start, end, step):
            self.query_range_calls.append((selector, start, end, step))
            return {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {
                                "__name__": "foo_total",
                                "pod": "p1",
                            },
                            "values": [
                                [start, "1"],
                                [end, "2"],
                            ],
                        },
                    ],
                },
            }

    def test_selectors_from_extra_targets(self):
        targets = [
            '{"type": "metric", "query": "foo_total{pod=\\"p1\\"}"}',
            '{"type": "metric", "query": "bar_total"}',
            '{"type": "unknown", "query": "ignored"}',
            'not-json',
        ]

        assert _selectors_from_extra_targets(targets) == [
            'foo_total{pod="p1"}',
            "bar_total",
        ]

    def test_writes_archive(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
defaults:
  step: 30s
targets:
- type: metric
  query: foo_total{pod="p1"}
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        assert output_path.exists()
        assert prom.count_series_calls[0][0] == 'foo_total{pod="p1"}'
        assert prom.query_range_calls == [
            (
                'foo_total{pod="p1"}',
                datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp(),
                datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc).timestamp(),
                "30s",
            ),
        ]

        with zipfile.ZipFile(str(output_path), "r") as zf:
            assert sorted(zf.namelist()) == [
                "data.om",
                "meta.json",
            ]

            meta = json.loads(zf.read("meta.json").decode("utf-8"))
            assert meta["step"] == "30s"
            assert meta["selectors"] == [
                'foo_total{pod="p1"}',
            ]
            assert meta["spec"]["targets"][0]["query"] == 'foo_total{pod="p1"}'

            data = zf.read("data.om").decode("utf-8")
            assert "# HELP foo_total" in data
            assert "# TYPE foo_total gauge" in data
            assert 'foo_total{pod="p1"} 1 1767225600.0' in data
            assert 'foo_total{pod="p1"} 2 1767225660.0' in data
            assert data.endswith("# EOF\n")

    def test_uses_cli_step_override(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
defaults:
  step: 30s
targets:
- type: metric
  query: foo_total
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step="5s",
            extra_targets=None,
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        assert prom.query_range_calls[0][3] == "5s"
        with zipfile.ZipFile(str(output_path), "r") as zf:
            meta = json.loads(zf.read("meta.json").decode("utf-8"))
            assert meta["step"] == "5s"

    def test_adds_extra_targets_to_dump(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: foo_total
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=[
                '{"type": "metric", "query": "bar_total{pod=\\"p1\\"}"}',
            ],
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        assert set([call[0] for call in prom.query_range_calls]) == set(
            [
                "foo_total",
                'bar_total{pod="p1"}',
            ]
        )

    def test_writes_dashboards_to_archive(self, tmp_path):
        dashboard_path = tmp_path / "dashboard.json"
        dashboard = {
            "uid": "uid",
            "title": "Title",
            "panels": [
                {
                    "targets": [
                        {"expr": "foo_total"},
                    ],
                },
            ],
        }
        dashboard_path.write_text(json.dumps(dashboard))

        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: dashboard
  path: dashboard.json
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        with zipfile.ZipFile(str(output_path), "r") as zf:
            assert "dashboards/dashboard.json" in zf.namelist()
            assert json.loads(zf.read("dashboards/dashboard.json").decode("utf-8")) == dashboard

    def test_does_not_create_archive_when_no_selectors(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets: []
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        assert not output_path.exists()
        assert prom.count_series_calls == []
        assert prom.query_range_calls == []

    def test_raises_when_time_range_is_invalid(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: foo_total
""")

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(tmp_path / "metrics.zip"),
            max_series=100,
            force=False,
        )

        with pytest.raises(ValueError, match="--to-ts must be greater than --from-ts"):
            MetricsDumper(self.FakePrometheusClient()).run(cfg)

    def test_skips_failed_prometheus_queries(self, tmp_path):
        class PartiallyFailingPrometheusClient(self.FakePrometheusClient):
            def query_range(self, selector, start, end, step):
                self.query_range_calls.append((selector, start, end, step))
                if selector == "bad_metric":
                    return {
                        "status": "error",
                        "error": "boom",
                    }
                return super().query_range(selector, start, end, step)

        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: bad_metric
- type: metric
  query: good_metric
""")

        output_path = tmp_path / "metrics.zip"
        prom = PartiallyFailingPrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        assert output_path.exists()
        with zipfile.ZipFile(str(output_path), "r") as zf:
            data = zf.read("data.om").decode("utf-8")
            assert "# HELP foo_total" in data
            assert data.endswith("# EOF\n")

    def test_does_not_prompt_when_total_series_is_under_limit(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: foo_total
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=1,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        assert output_path.exists()

    def test_respects_force_when_total_series_is_over_limit(self, tmp_path):
        class LargePrometheusClient(self.FakePrometheusClient):
            def count_series(self, selector, at):
                self.count_series_calls.append((selector, at))
                return 1000

        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: foo_total
""")

        output_path = tmp_path / "metrics.zip"
        prom = LargePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=10,
            force=True,
        )

        MetricsDumper(prom).run(cfg)

        assert output_path.exists()

    def test_archive_contains_no_absolute_local_paths(self, tmp_path):
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text("""
targets:
- type: metric
  query: foo_total
""")

        output_path = tmp_path / "metrics.zip"
        prom = self.FakePrometheusClient()

        cfg = MetricsDumpConfig(
            spec_path=str(spec_path),
            from_ts=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            to_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            step=None,
            extra_targets=None,
            output=str(output_path),
            max_series=100,
            force=False,
        )

        MetricsDumper(prom).run(cfg)

        with zipfile.ZipFile(str(output_path), "r") as zf:
            for name in zf.namelist():
                assert not os.path.isabs(name)
                assert ".." not in name.split("/")
