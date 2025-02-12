#pragma once

#include "spilling_counters.h"

#include <contrib/ydb/library/yql/dq/common/dq_common.h>
#include <contrib/ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId,
    TWakeUpCallback wakeUpCallback,
    TErrorCallback errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters,
    NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq
