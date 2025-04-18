#pragma once
#include <contrib/ydb/core/base/events.h>

#include <contrib/ydb/library/accessor/accessor.h>

#include <contrib/ydb/library/actors/core/events.h>

namespace NKikimr::NColumnShard::NTiers {

enum EEvents {
    EvTierCleared = EventSpaceBegin(TKikimrEvents::ES_TIERING),
    EvSSFetchingResult,
    EvSSFetchingProblem,
    EvTimeout,
    EvTiersManagerReadyForUsage,
    EvWatchSchemeObject,
    EvNotifySchemeObjectUpdated,
    EvNotifySchemeObjectDeleted,
    EvSchemeObjectResulutionFailed,
    EvListTieredStoragesResult,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_TIERING), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TIERING)");
}
