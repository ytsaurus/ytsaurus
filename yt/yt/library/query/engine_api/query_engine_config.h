#pragma once

#include "cg_cache.h"

#include <yt/yt/library/codegen_api/execution_backend.h>

#include <yt/yt/core/misc/configurable_singleton_decl.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TQueryEngineConfig)
DECLARE_REFCOUNTED_STRUCT(TQueryEngineDynamicConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TQueryEngineConfig, TQueryEngineDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

struct TQueryEngineConfig
    : public NYTree::TYsonStruct
{
    TCodegenCacheConfigPtr CodegenCache;

    REGISTER_YSON_STRUCT(TQueryEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryEngineConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryEngineDynamicConfig
    : public NYTree::TYsonStruct
{
    TCodegenCacheDynamicConfigPtr CodegenCache;
    std::optional<EStatisticsAggregation> StatisticsAggregation;
    std::optional<bool> UseOrderByInJoinSubqueries;
    std::optional<bool> EnableParallelizeUnorderedGroupBy;
    std::optional<int> ExpressionBuilderVersion;
    std::optional<NCodegen::EOptimizationLevel> OptimizationLevel;
    std::optional<bool> AllowUdfObjectCodeCache;
    std::optional<bool> RewriteCardinalityIntoHyperLogLogWithPrecision; // COMPAT(dtorilov): Remove after 25.4.
    std::optional<bool> AllowJoinWithAsyncLastCommittedTimestampIfRequireSyncReplicaIsFalse;
    std::optional<int> TruncatedQueryLengthForTracing;
    std::optional<bool> AllowHeavyRangeInferenceInJoins;
    std::optional<bool> AllowReverseScanForOrderBy; // COMPAT(dtorilov): Remove after 26.1.
    std::optional<bool> PrefetchJoinTables;

    std::optional<i64> JoinCacheSize;

    REGISTER_YSON_STRUCT(TQueryEngineDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryEngineDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
