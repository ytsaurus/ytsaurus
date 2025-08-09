#pragma once

#include <contrib/ydb/core/kqp/counters/kqp_counters.h>

#include <contrib/ydb/core/protos/kqp.pb.h>
#include <contrib/ydb/core/base/appdata.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
}

namespace NKikimr::NKqp {

struct TKqpRunScriptActorSettings {
    TString Database;
    TString ExecutionId;
    i64 LeaseGeneration = 0;
    TDuration LeaseDuration;
    TDuration ResultsTtl;
    TDuration ProgressStatsPeriod;
    TIntrusivePtr<TKqpCounters> Counters;
    bool SaveQueryPhysicalGraph = false;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
};

NActors::IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, TKqpRunScriptActorSettings&& settings, NKikimrConfig::TQueryServiceConfig queryServiceConfig);

} // namespace NKikimr::NKqp
