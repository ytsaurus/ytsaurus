#pragma once

#include <contrib/ydb/core/kqp/counters/kqp_counters.h>

#include <contrib/ydb/core/protos/kqp.pb.h>
#include <contrib/ydb/core/base/appdata.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
}

namespace NKikimr::NKqp {

struct TEvKqpRunScriptActor {
};

NActors::IActor* CreateRunScriptActor(const TString& executionId, const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration, TDuration leaseDuration, TDuration resultsTtl, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters);

} // namespace NKikimr::NKqp
