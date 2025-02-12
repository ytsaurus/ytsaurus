#pragma once

#include <contrib/ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <contrib/ydb/public/api/protos/ydb_value.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NYql::NDqs::NExecutionHelpers {
    THolder<NActors::IActor> MakeResultAggregator(
        const TVector<TString>& columns,
        const NActors::TActorId& executerId,
        const TString& traceId,
        const THashMap<TString, TString>& secureParams,
        const TDqConfiguration::TPtr& settings,
        const TString& resultType,
        bool discard,
        const NActors::TActorId& graphExecutionEventsId);
} // namespace NYql::NDqs::NExecutionHelpers
