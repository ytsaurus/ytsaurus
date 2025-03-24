#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ {

inline NActors::TActorId MakePQDReadCacheServiceActorId() {
    return NActors::TActorId(0, "PQCacheProxy");
}

IActor* CreatePQDReadCacheService(const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace
