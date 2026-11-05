#pragma once

#include "private.h"
#include "profile_manager.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(NDetail::TProfileManagerBase<TMultiConsumerSnapshotPtr>);
using IMultiConsumerProfileManager = NDetail::TProfileManagerBase<TMultiConsumerSnapshotPtr>;
using IMultiConsumerProfileManagerPtr = TIntrusivePtr<NDetail::TProfileManagerBase<TMultiConsumerSnapshotPtr>>;

////////////////////////////////////////////////////////////////////////////////

IMultiConsumerProfileManagerPtr CreateMultiConsumerProfileManager(
    const NProfiling::TProfiler& profiler,
    const NLogging::TLogger& logger,
    const NQueueClient::TConsumerTableRow& row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
