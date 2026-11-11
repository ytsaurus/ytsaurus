#pragma once

#include "journal_nbd_spec.h"

#include <yt/yt/server/controller_agent/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

TJournalNbdSpec ResolveJournalNbdSpec(
    const TJournalNbdOptionsPtr& journalNbdOptions,
    const NScheduler::TNbdDiskRequestPtr& diskRequest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
