#pragma once

#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/tools/yqlworker/solomon_agent/solomon_agent_client.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>


namespace NYql {

NActors::IActor* CreateMetricsPusher(
    const NMonitoring::TDynamicCounterPtr& counters,
    const TSolomonAgentConfig& config,
    bool pidLabel = false);

} // namespace NYql
