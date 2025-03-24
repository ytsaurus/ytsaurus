#pragma once
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/core/base/events.h>

namespace NKikimr::NSchemeShard::NBackground {

enum EEv {
    EvListRequest = EventSpaceBegin(TKikimrEvents::ES_SS_BG_TASKS),
    EvListResponse,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SS_BG_TASKS), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_SS_BG_TASKS)");

}