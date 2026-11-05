#pragma once

#include "buffer_warmup.h"
#include "stream_inflight_limits.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Per-job view of the worker's buffer manager exposed to the computation:
//! warm-start plumbing and the output stream limit slots.
struct IPartitionBufferState
    : public TRefCounted
{
    //! Warm start: seed the buffer sizing from the persisted warmup once it is
    //! read; idempotent.
    virtual void SeedWarmup(const TPartitionBufferWarmup& warmup) = 0;

    //! Current converged sizing to persist back with the epoch transaction;
    //! empty when the v2 strategy is off.
    virtual TPartitionBufferWarmup GetWarmup() = 0;

    //! Whether warm-start is active (the v2 strategy is on). When off the
    //! computation skips the whole warmup path, leaving v1 behaviour untouched.
    virtual bool IsWarmupEnabled() = 0;

    virtual TDuration GetWarmupRefreshPeriod() = 0;

    virtual const TStreamLimitUsageStateMap& GetOutputStreamLimitUsageStates() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitionBufferState);

//! A buffer state for hosts that run computations outside a worker (perf
//! harnesses, tests): serves the given output limit slots, warm-start disabled.
IPartitionBufferStatePtr CreateDetachedPartitionBufferState(TStreamLimitUsageStateMap outputStreamLimitUsageStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
