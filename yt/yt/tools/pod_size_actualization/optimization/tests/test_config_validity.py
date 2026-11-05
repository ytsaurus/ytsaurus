"""Tests for the config-validity annotation of assignments.

Кубик окон считает, сколько последних периодов бандл простоял в неизменной
конфигурации; счётчик доезжает до рекомендации как ValidPeriods и Confidence.

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import pandas as pd
import pytest

from yt.yt.tools.pod_size_actualization.optimization import (
    annotate_assignments_with_validity as _annotate_assignments_with_validity,
)
from yt.yt.tools.pod_size_actualization.optimization.data import load_bundle_validity
from yt.yt.tools.pod_size_actualization.optimization.results import (
    rename_columns_to_snake_case,
)

ADMINISTRATIVE_VALUES = {
    "abc_service_slug": "yt",
    "abc_service_path": "yandex/infra/yt",
    "value_stream_slug": "data-platform",
    "value_stream_name_ru": "Платформа данных",
    "business_unit_slug": "infrastructure",
    "business_unit_name_ru": "Инфраструктура",
    "business_group_slug": "yandex-infrastructure",
    "business_group_name_ru": "Инфраструктурные сервисы",
}


def test_final_table_columns_are_converted_to_snake_case():
    source = pd.DataFrame(
        columns=[
            "ClusterGroup",
            "ClusterGroupCatalogLoadedAt",
            "AssignedContainerTypeID",
            "BaseCPU",
            "abc_service_slug",
        ]
    )

    result = rename_columns_to_snake_case(source)

    assert result.columns.tolist() == [
        "cluster_group",
        "cluster_group_catalog_loaded_at",
        "assigned_container_type_id",
        "base_cpu",
        "abc_service_slug",
    ]


def assignments(*rows):
    """Назначения в том виде, в каком их отдаёт optimize_assignment."""
    return pd.DataFrame(
        [
            {
                "Cluster": cluster,
                "BundleName": name,
                "Bundle": f"{name}@{instance_type}@{cluster}",
                "InstanceType": instance_type,
                "AssignedContainerTypeID": 0,
                "NewCount": 1,
                "BaseCount": 1,
            }
            for cluster, name, instance_type in rows
        ]
    )


def annotate_assignments_with_validity(
    assign_df,
    validity_df,
    periods_total,
    configured_clusters=None,
):
    if configured_clusters is None:
        configured_clusters = set(validity_df['cluster'])
    return _annotate_assignments_with_validity(
        assign_df,
        validity_df,
        periods_total,
        configured_clusters,
    )


def validity(*rows, periods_total=3):
    """Валидность в том виде, в каком её отдаёт load_bundle_validity."""
    return pd.DataFrame(
        [
            {
                "cluster": cluster,
                "bundle": bundle,
                "node_valid_periods": node_periods,
                "proxy_valid_periods": proxy_periods,
                "node_last_config_change": node_change,
                "proxy_last_config_change": proxy_change,
                "bundle_spec_loaded_at": "2026-07-28",
                **{
                    f"{instance}_{kind}_coverage_period_{period}": 1.0
                    for instance in ("node", "proxy")
                    for kind in ("spec", "usage")
                    for period in range(periods_total)
                },
                "node_confidence": (
                    "none" if node_periods == 0 else "full" if node_periods >= periods_total else "low"
                ),
                "proxy_confidence": (
                    "none" if proxy_periods == 0 else "full" if proxy_periods >= periods_total else "low"
                ),
                "node_period_invalidation_reason": (
                    ""
                    if node_periods >= periods_total
                    else (
                        f"period_{node_periods}:recent_configuration_change"
                        if node_change
                        else f"period_{node_periods}:insufficient_usage_coverage"
                    )
                ),
                "proxy_period_invalidation_reason": (
                    ""
                    if proxy_periods >= periods_total
                    else (
                        f"period_{proxy_periods}:recent_configuration_change"
                        if proxy_change
                        else f"period_{proxy_periods}:insufficient_usage_coverage"
                    )
                ),
                "node_confidence_reason": (
                    ""
                    if node_periods >= periods_total
                    else "no_valid_period" if node_periods == 0 else "dropped_periods"
                ),
                "proxy_confidence_reason": (
                    ""
                    if proxy_periods >= periods_total
                    else "no_valid_period" if proxy_periods == 0 else "dropped_periods"
                ),
                "node_count": 3,
                "rpc_count": 0,
                "node_type": "medium",
                "rpc_type": "medium",
                **ADMINISTRATIVE_VALUES,
            }
            for cluster, bundle, node_periods, proxy_periods, node_change, proxy_change in rows
        ]
    )


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------


def test_all_periods_valid_gives_full_confidence():
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        validity(("seneca-sas", "ads", 3, 3, None, None)),
        periods_total=3,
    )
    assert out["Confidence"].tolist() == ["full"]
    assert out["ValidPeriods"].tolist() == [3]


def test_recent_config_change_lowers_confidence():
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        validity(("seneca-sas", "ads", 1, 3, "2026-07-20", None)),
        periods_total=3,
    )
    assert out["Confidence"].tolist() == ["low"]
    assert out["ValidPeriods"].tolist() == [1]
    assert out["LastConfigChange"].tolist() == ["2026-07-20"]


def test_confidence_is_full_when_only_one_period_was_collected():
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        validity(("seneca-sas", "ads", 1, 1, None, None), periods_total=1),
        periods_total=1,
    )
    assert out["Confidence"].tolist() == ["full"]


def test_no_config_change_leaves_the_date_empty():
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        validity(("seneca-sas", "ads", 3, 3, None, None)),
        periods_total=3,
    )
    assert out["LastConfigChange"].tolist() == [""]


# ---------------------------------------------------------------------------
# Ключ джойна
# ---------------------------------------------------------------------------


def test_node_and_proxy_of_one_bundle_are_annotated_independently():
    data = validity(("seneca-sas", "ads", 1, 3, "2026-07-20", None))
    data.loc[0, "rpc_count"] = 2
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node"), ("seneca-sas", "ads", "proxy")),
        data,
        periods_total=3,
    )
    by_type = dict(zip(out["InstanceType"], out["ValidPeriods"]))
    assert by_type == {"node": 1, "proxy": 3}
    assert dict(zip(out["InstanceType"], out["Confidence"])) == {"node": "low", "proxy": "full"}


def test_same_bundle_on_different_clusters_gets_its_own_validity():
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node"), ("seneca-vla", "ads", "node")),
        validity(("seneca-sas", "ads", 3, 3, None, None), ("seneca-vla", "ads", 2, 3, "2026-07-14", None)),
        periods_total=3,
    )
    assert dict(zip(out["Cluster"], out["ValidPeriods"])) == {"seneca-sas": 3, "seneca-vla": 2}


def test_missing_validity_is_an_error():
    with pytest.raises(ValueError, match="no validity data"):
        annotate_assignments_with_validity(
            assignments(("seneca-sas", "ads", "node")),
            validity(("seneca-vla", "ads", 3, 3, None, None)),
            periods_total=3,
            configured_clusters={"seneca-sas", "seneca-vla"},
        )


def test_service_columns_do_not_leak_into_the_result():
    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        validity(("seneca-sas", "ads", 3, 3, None, None)),
        periods_total=3,
    )
    assert "node_valid_periods" not in out.columns
    assert "_merge" not in out.columns
    assert {"Cluster", "Bundle", "AssignedContainerTypeID", "NewCount"} <= set(out.columns)


def test_empty_assignments_stay_empty():
    out = annotate_assignments_with_validity(
        assignments(),
        validity(("seneca-sas", "ads", 3, 3, None, None)),
        periods_total=3,
    )
    assert out.empty


def test_no_valid_period_is_retained_with_coverage_reasons():
    data = validity(("seneca-sas", "ads", 0, 3, None, None))
    data.loc[0, "rpc_count"] = 0
    data.loc[0, "node_period_invalidation_reason"] = (
        "period_0:insufficient_spec_coverage,period_0:insufficient_usage_coverage"
    )

    out = annotate_assignments_with_validity(assignments(), data, periods_total=3)

    assert len(out) == 1
    row = out.iloc[0]
    assert row["InstanceType"] == "node"
    assert row["RecommendationStatus"] == "not_recommended"
    assert row["PeriodInvalidationReason"] == (
        "period_0:insufficient_spec_coverage,period_0:insufficient_usage_coverage"
    )
    assert row["ConfidenceReason"] == "no_valid_period"
    assert row["BundleSpecLoadedAt"] == "2026-07-28"


def test_no_valid_period_reports_recent_configuration_change():
    data = validity(("seneca-sas", "ads", 0, 3, "2026-07-27", None))
    data.loc[0, "rpc_count"] = 0

    out = annotate_assignments_with_validity(assignments(), data, periods_total=3)

    row = out.iloc[0]
    assert row["PeriodInvalidationReason"] == "period_0:recent_configuration_change"
    assert row["LastConfigChange"] == "2026-07-27"


def test_recommendation_without_dropped_periods_has_empty_reasons():
    data = validity(("seneca-sas", "ads", 3, 3, None, None))

    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        data,
        periods_total=3,
    )

    row = out.iloc[0]
    assert row["BundleSpecLoadedAt"] == "2026-07-28"
    assert row["RecommendationStatus"] == "recommended"
    assert row["PeriodInvalidationReason"] == ""
    assert row["ConfidenceReason"] == ""
    assert {name: row[name] for name in ADMINISTRATIVE_VALUES} == ADMINISTRATIVE_VALUES


def test_administrative_fields_are_retained_without_recommendation():
    data = validity(("seneca-sas", "ads", 0, 3, None, None))
    data.loc[0, "rpc_count"] = 0

    out = annotate_assignments_with_validity(assignments(), data, periods_total=3)

    row = out.iloc[0]
    assert row["RecommendationStatus"] == "not_recommended"
    assert {name: row[name] for name in ADMINISTRATIVE_VALUES} == ADMINISTRATIVE_VALUES


def yql_period_files(tmp_path, period_usage_coverage):
    """Периодные выходы YQL: диагностика в них общая, coverage периода — своё."""
    paths = []
    for period, usage_coverage in enumerate(period_usage_coverage):
        row = validity(("seneca-sas", "ads", 2, 2, None, None)).iloc[0].to_dict()
        row.update(
            {
                "method_name": f"period_{period}",
                "periods_total": len(period_usage_coverage),
                "node_period_invalidation_reason": "period_2:insufficient_usage_coverage",
                "node_confidence_reason": "dropped_periods",
                "node_spec_coverage": 1.0,
                "proxy_spec_coverage": 1.0,
                "node_usage_coverage": usage_coverage,
                "proxy_usage_coverage": usage_coverage,
            }
        )
        path = tmp_path / f"period_{period}.csv"
        pd.DataFrame([row]).to_csv(path, index=False)
        paths.append(str(path))
    return paths


def test_load_bundle_validity_accepts_consistent_yql_diagnostics(tmp_path):
    loaded, periods_total = load_bundle_validity(yql_period_files(tmp_path, [0.7] * 3))

    assert periods_total == 3
    assert loaded.loc[0, "node_usage_coverage_period_0"] == pytest.approx(0.7)
    assert loaded.loc[0, "node_period_invalidation_reason"] == "period_2:insufficient_usage_coverage"

    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        loaded,
        periods_total,
    )
    assert out.loc[0, "Confidence"] == "low"
    assert out.loc[0, "PeriodInvalidationReason"] == "period_2:insufficient_usage_coverage"
    assert out.loc[0, "ConfidenceReason"] == "dropped_periods"


def test_coverage_of_every_period_reaches_the_final_table(tmp_path):
    loaded, periods_total = load_bundle_validity(yql_period_files(tmp_path, [1.0, 0.9, 0.3]))

    assert loaded.loc[0, "node_usage_coverage_period_2"] == pytest.approx(0.3)

    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        loaded,
        periods_total,
    )
    row = out.iloc[0]
    assert row["UsageCoveragePeriod_0"] == pytest.approx(1.0)
    assert row["UsageCoveragePeriod_1"] == pytest.approx(0.9)
    assert row["UsageCoveragePeriod_2"] == pytest.approx(0.3)
    assert row["SpecCoveragePeriod_2"] == pytest.approx(1.0)
    # Усреднённых столбцов в итоговой таблице нет: их заменяют периодные.
    assert "UsageCoverage" not in out.columns

    renamed = rename_columns_to_snake_case(out)
    assert "usage_coverage_period_2" in renamed.columns


def test_full_periods_with_coverage_below_point_seven_have_low_confidence():
    data = validity(("seneca-sas", "ads", 3, 3, None, None))
    data.loc[0, "node_confidence"] = "low"
    data.loc[0, "node_confidence_reason"] = "insufficient_spec_coverage"

    out = annotate_assignments_with_validity(
        assignments(("seneca-sas", "ads", "node")),
        data,
        periods_total=3,
    )

    assert out.loc[0, "Confidence"] == "low"
    assert out.loc[0, "ConfidenceReason"] == "insufficient_spec_coverage"


def test_unconfigured_clusters_are_filtered_from_not_recommended():
    data = validity(
        ("seneca-sas", "ads", 0, 0, None, None),
        ("zeno", "external", 0, 0, None, None),
    )

    out = annotate_assignments_with_validity(
        assignments(),
        data,
        periods_total=3,
        configured_clusters={"seneca-sas"},
    )

    assert set(out["Cluster"]) == {"seneca-sas"}


def test_missing_count_is_an_error():
    data = validity(("seneca-sas", "ads", 0, 3, None, None))
    data.loc[0, "node_count"] = float('nan')

    with pytest.raises(ValueError, match="cannot convert float NaN"):
        annotate_assignments_with_validity(assignments(), data, periods_total=3)
