#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <util/generic/fwd.h>

#include <string_view>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Job-scoped throttler handles, quota classes and partition progress.
struct IJobThrottlerFactory
    : public TRefCounted
{
    //! Returns a cacheable handle that survives implementation replacement.
    //! Throws if the name is not in the current configuration.
    virtual NConcurrency::IThroughputThrottlerPtr GetClientOrThrow(
        std::string_view throttlerName) = 0;

    virtual NConcurrency::IThroughputThrottlerPtr TryGetClient(
        std::string_view throttlerName) = 0;

    //! Updates the partition progress timestamp used for priority scheduling.
    virtual void SetPriority(TPriority priority) = 0;

    //! Sets the quota class for each throttler; missing entries use the default class.
    virtual void SetQuotaClasses(THashMap<TThrottlerId, TQuotaClassId> quotaClassIds) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobThrottlerFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
