#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/fwd.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Local alias for throttler names used across this module. Kept as a plain
//! string so the module stays independent of flow-side strong typedefs.
using TThrottlerId = std::string;

//! Scheduling class for a quota request.
using TQuotaClassId = std::string;

inline const TQuotaClassId DefaultQuotaClassId = NFlow::DefaultQuotaClassName;

//! Priority key for RequestQuota RPCs. Smaller value == higher priority on
//! the server's bucket queue. Opaque to the factory and client — callers
//! decide the scale (e.g. plug in an event timestamp).
using TPriority = ui64;

DECLARE_REFCOUNTED_CLASS(TDistributedThrottlerBucket);
DECLARE_REFCOUNTED_STRUCT(IDistributedThrottlerService);
DECLARE_REFCOUNTED_STRUCT(IDistributedThrottlerFactory);
DECLARE_REFCOUNTED_CLASS(TRemoteThrottler);

DECLARE_REFCOUNTED_STRUCT(TDistributedThrottlerServiceConfig);
DECLARE_REFCOUNTED_STRUCT(TDistributedThrottlerBucketConfig);
DECLARE_REFCOUNTED_STRUCT(TDistributedThrottlerClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
