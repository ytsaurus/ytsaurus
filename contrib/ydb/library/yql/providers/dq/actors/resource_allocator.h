#pragma once

#include <contrib/ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <contrib/ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql {
    NActors::IActor* CreateResourceAllocator(
        NActors::TActorId gwmActor,
        NActors::TActorId senderId,
        NActors::TActorId resultId,
        ui32 workerCount,
        const TString& traceId,
        const TDqConfiguration::TPtr& settings,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TVector<NYql::NDqProto::TDqTask>& tasks = {},
        const TString& computeActorType = "old",
        NDqProto::EDqStatsMode statsMode = NDqProto::DQ_STATS_MODE_UNSPECIFIED);
} // namespace NYql
