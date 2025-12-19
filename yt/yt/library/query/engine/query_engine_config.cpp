#include "query_engine_config.h"

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NQueryClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_VERBOSE_CHANGING_QUERY_ENGINE_CONFIG

constinit const auto Logger = QueryClientLogger;

#endif

////////////////////////////////////////////////////////////////////////////////

void TQueryEngineConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("codegen_cache", &TThis::CodegenCache)
        .DefaultNew();
}

void TQueryEngineDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("codegen_cache", &TThis::CodegenCache)
        .DefaultNew();

    registrar.Parameter("statistics_aggregation", &TThis::StatisticsAggregation)
        .Optional();

    registrar.Parameter("use_order_by_in_join_subqueries", &TThis::UseOrderByInJoinSubqueries)
        .Optional();

    registrar.Parameter("expression_builder_version", &TThis::ExpressionBuilderVersion)
        .Optional();

    registrar.Parameter("codegen_optimization_level", &TThis::OptimizationLevel)
        .Optional();

    registrar.Parameter("rewrite_cardinality_into_hyper_log_log_with_precision", &TThis::RewriteCardinalityIntoHyperLogLogWithPrecision)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TQueryEngineConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TQueryEngineDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TQueryEngineConfigPtr& config)
{
#ifdef YT_VERBOSE_CHANGING_QUERY_ENGINE_CONFIG
    YT_LOG_INFO("Configure QueryEngine (Config: %v)", ConvertToYsonString(config, EYsonFormat::Text));
#else
    Y_UNUSED(config);
#endif
}

void ReconfigureSingleton(
    const TQueryEngineConfigPtr& /*config*/,
    const TQueryEngineDynamicConfigPtr& dynamicConfig)
{
    if (dynamicConfig->CodegenCache) {
        TCodegenCacheSingleton::Reconfigure(dynamicConfig->CodegenCache);
    }

#ifdef YT_VERBOSE_CHANGING_QUERY_ENGINE_CONFIG
    YT_LOG_INFO("Reconfigure QueryEngine (Config: %v)", ConvertToYsonString(dynamicConfig, EYsonFormat::Text));
#endif
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "query_engine_config",
    TQueryEngineConfig,
    TQueryEngineDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
