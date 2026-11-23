#pragma once

#include <yt/yt/flow/library/cpp/distributed_throttler/public.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedThrottlerBucket);
DECLARE_REFCOUNTED_CLASS(TRemoteThrottler);
DECLARE_REFCOUNTED_STRUCT(TDistributedThrottlerBucketConfig);
DECLARE_REFCOUNTED_STRUCT(TDistributedThrottlerClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
