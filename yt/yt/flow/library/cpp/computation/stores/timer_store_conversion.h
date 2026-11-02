#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Conversions between the timer store backends, run inside |Init| before the first iteration.
// Both move rows a batch per transaction and continue from any point after an interruption.

constexpr i64 TimerConversionBatchSize = 1000;

//! Imports the partition's configured timer streams into |pagedStore|, retaining other rows.
//! The store's families must be loaded; must be called from a fiber.
void ImportTimersIntoPages(const TTimerStoreContextPtr& context, const IPagedTimerStorePtr& pagedStore);

//! Moves every timer held by |pagedStore|, which must be initialized, into the |timers| table;
//! must be called from a fiber.
void ExportPagesIntoTimers(const TTimerStoreContextPtr& context, const IPagedTimerStorePtr& pagedStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
