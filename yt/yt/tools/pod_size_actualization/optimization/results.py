"""Save and load optimization results (JSON format)."""

import json
from pathlib import Path

from inflection import underscore
import numpy as np
import pandas as pd

from .data import BUNDLE_ADMINISTRATIVE_COLUMNS, VALIDITY_COLUMNS
from .scripts import shared as cfg


def rename_columns_to_snake_case(dataframe):
    """Return a copy with CamelCase column names converted to snake_case."""
    return dataframe.rename(columns=underscore)


def save_progress(
    output_dir: Path,
    prefix: str,
    res_df: pd.DataFrame,
    sizes_df: pd.DataFrame,
    assign_df: pd.DataFrame,
    patterns_df: pd.DataFrame,
    phys_hosts_df: pd.DataFrame,
    tag: str,
):
    result = {
        "summary": res_df.to_dict(orient="records"),
        "sizes": sizes_df.to_dict(orient="records"),
        "assignments": assign_df.to_dict(orient="records"),
        "patterns": patterns_df.to_dict(orient="records"),
        "physical_hosts": phys_hosts_df.to_dict(orient="records"),
    }
    out_file = output_dir / f"result_{prefix}_{tag}.json"
    with open(out_file, "w") as f:
        json.dump(result, f)


def load_warm_start(warm_start_dir, ws_k_node: int, ws_k_proxy: int):
    prefix = f"kn{ws_k_node}_kp{ws_k_proxy}"
    json_file = Path(warm_start_dir) / f"result_{prefix}_best.json"

    if not json_file.exists():
        print(f"Warm start: file not found: {json_file}")
        return None

    try:
        with open(json_file) as f:
            data = json.load(f)
        result = {"prev_k_node": ws_k_node, "prev_k_proxy": ws_k_proxy}
        if data.get("sizes"):
            result["sizes_df"] = pd.DataFrame(data["sizes"])
        else:
            print(f"Warm start: no sizes in {json_file}")
            return None
        if data.get("assignments"):
            result["assign_df"] = pd.DataFrame(data["assignments"])
        if data.get("patterns"):
            result["patterns_df"] = pd.DataFrame(data["patterns"])
        print(f"Loaded warm start from {json_file}: K_node={ws_k_node}, K_proxy={ws_k_proxy}")
        return result
    except Exception as e:
        print(f"Error loading warm start: {e}")
        return None


def annotate_assignments_with_validity(
    assign_df,
    validity_df,
    periods_total: int,
    configured_clusters,
):
    """Annotate recommendations and retain bundles with no valid period.

    Confidence: 'full' — данные за все периоды, 'low' — более старый период
    отброшен из-за coverage или недавней смены конфигурации, 'none' — невалиден
    period_0 и рекомендации нет. Валидность считается отдельно для node/proxy.
    """
    configured_clusters = set(configured_clusters)
    key_columns = {'Cluster', 'BundleName', 'InstanceType'}
    missing_columns = key_columns - set(assign_df.columns)
    if not assign_df.empty and missing_columns:
        raise ValueError(f"assign_df is missing columns: {sorted(missing_columns)}")

    validity_df = validity_df.copy()
    validity_df = validity_df[validity_df['cluster'].isin(configured_clusters)].copy()

    # Число периодов задаётся опцией графа, поэтому берём их из самих данных.
    periods = sorted(
        int(column.rsplit('_', 1)[1])
        for column in validity_df.columns
        if column.startswith('node_spec_coverage_period_')
    )
    period_outputs = [
        (
            f'{name}CoveragePeriod_{period}',
            f'node_{kind}_coverage_period_{period}',
            f'proxy_{kind}_coverage_period_{period}',
        )
        for name, kind in (('Spec', 'spec'), ('Usage', 'usage'))
        for period in periods
    ]

    def _reason(row, prefix, column):
        value = row[f'{prefix}_{column}']
        return '' if pd.isna(value) else str(value)

    if assign_df.empty:
        merged = assign_df.copy()
        recommended = assign_df.copy()
    else:
        merged = assign_df.merge(
            validity_df,
            how='left',
            indicator=True,
            left_on=['Cluster', 'BundleName'],
            right_on=['cluster', 'bundle'],
        )
        unmatched = merged[merged['_merge'] != 'both']
        if not unmatched.empty:
            sample = unmatched[['Cluster', 'BundleName']].head(3).to_dict(orient='records')
            raise ValueError(f"no validity data for {len(unmatched)} assignments, e.g. {sample}")

        is_node = merged['InstanceType'] == 'node'
        merged['ValidPeriods'] = np.where(is_node, merged['node_valid_periods'], merged['proxy_valid_periods']).astype(
            int
        )
        merged['LastConfigChange'] = pd.Series(
            np.where(is_node, merged['node_last_config_change'], merged['proxy_last_config_change']), index=merged.index
        ).fillna('')
        merged['BundleSpecLoadedAt'] = merged['bundle_spec_loaded_at']
        for output, node_column, proxy_column in period_outputs:
            merged[output] = np.where(is_node, merged[node_column], merged[proxy_column])
        merged['Confidence'] = np.where(is_node, merged['node_confidence'], merged['proxy_confidence'])
        merged['RecommendationStatus'] = 'recommended'
        for output, column in (
            ('PeriodInvalidationReason', 'period_invalidation_reason'),
            ('ConfidenceReason', 'confidence_reason'),
        ):
            merged[output] = [
                _reason(row, 'node' if row['InstanceType'] == 'node' else 'proxy', column)
                for _, row in merged.iterrows()
            ]
        service_columns = [
            column for _, node_column, proxy_column in period_outputs for column in (node_column, proxy_column)
        ]
        recommended = merged.drop(
            columns=['cluster', 'bundle', '_merge', *VALIDITY_COLUMNS, *service_columns],
            errors='ignore',
        )

    existing = set()
    if not assign_df.empty:
        existing = set(
            zip(
                assign_df['Cluster'],
                assign_df['BundleName'],
                assign_df['InstanceType'],
            )
        )

    not_recommended = []
    for _, row in validity_df.iterrows():
        if str(row['bundle']) in cfg.BUNDLES_TO_SKIP:
            continue
        for prefix, instance_type, count_column, type_column in (
            ('node', 'node', 'node_count', 'node_type'),
            ('proxy', 'proxy', 'rpc_count', 'rpc_type'),
        ):
            # int(NaN) raises: a missing count is malformed input, not zero.
            count = int(row[count_column])
            key = (row['cluster'], row['bundle'], instance_type)
            if count <= 0 or int(row[f'{prefix}_valid_periods']) > 0 or key in existing:
                continue

            last_change = row[f'{prefix}_last_config_change']
            not_recommended.append(
                {
                    'Cluster': row['cluster'],
                    'BundleName': row['bundle'],
                    'Bundle': f"{row['bundle']}@{instance_type}@{row['cluster']}",
                    'InstanceType': instance_type,
                    'BaseCount': count,
                    'BaseContainerType': row[type_column],
                    'ValidPeriods': 0,
                    'LastConfigChange': '' if pd.isna(last_change) else last_change,
                    'BundleSpecLoadedAt': row['bundle_spec_loaded_at'],
                    **{
                        output: row[node_column if prefix == 'node' else proxy_column]
                        for output, node_column, proxy_column in period_outputs
                    },
                    'Confidence': row[f'{prefix}_confidence'],
                    'RecommendationStatus': 'not_recommended',
                    'PeriodInvalidationReason': _reason(row, prefix, 'period_invalidation_reason'),
                    'ConfidenceReason': _reason(row, prefix, 'confidence_reason'),
                    **{name: row[name] for name in BUNDLE_ADMINISTRATIVE_COLUMNS},
                }
            )

    result = pd.concat(
        [recommended, pd.DataFrame(not_recommended)],
        ignore_index=True,
        sort=False,
    )

    full = int((result.get('Confidence') == 'full').sum()) if not result.empty else 0
    low = int((result.get('Confidence') == 'low').sum()) if not result.empty else 0
    skipped = len(result) - full - low
    print(f"Confidence: full {full}, low {low}, no recommendation {skipped} " f"(периодов всего {periods_total})")
    return result


def build_warm_start_from_solution(sizes_df, assign_df, patterns_df, k_node, k_proxy):
    return {
        "sizes_df": sizes_df.copy(),
        "assign_df": assign_df.copy() if assign_df is not None else pd.DataFrame(),
        "patterns_df": patterns_df.copy() if patterns_df is not None else pd.DataFrame(),
        "prev_k_node": k_node,
        "prev_k_proxy": k_proxy,
    }
