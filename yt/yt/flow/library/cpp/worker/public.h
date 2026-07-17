#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/worker/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, WorkerLogger, "FlowWorker");
// Lives here (not private.h) so cross-role code such as the runner can reach it.
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, WorkerProfiler, "", "yt.flow.worker");

DECLARE_REFCOUNTED_STRUCT(TJobContext)
DECLARE_REFCOUNTED_STRUCT(TJobOrchidState)
DECLARE_REFCOUNTED_STRUCT(IJob)
DECLARE_REFCOUNTED_STRUCT(TJobTrackerContext)
DECLARE_REFCOUNTED_STRUCT(IJobTracker)
DECLARE_REFCOUNTED_STRUCT(IMessageDistributor)
DECLARE_REFCOUNTED_STRUCT(IBufferStateManager)
DECLARE_REFCOUNTED_STRUCT(IInputBuffer)
DECLARE_REFCOUNTED_STRUCT(IInputManager)

DECLARE_REFCOUNTED_STRUCT(IControllerConnector)
DECLARE_REFCOUNTED_STRUCT(IJobManager)

DECLARE_REFCOUNTED_STRUCT(TWorkerConfig)

DECLARE_REFCOUNTED_CLASS(TExternalPerformanceMetricsReporter)

////////////////////////////////////////////////////////////////////////////////

//! Returned by the destination worker as the result
//! of PushMessages request.
DEFINE_ENUM(EMessageDeliveryState,
    // TODO(gryzlov-ad): This status should be used for unknown messages without payload.
    (Unknown)

    // The message is accepted by the destination worker and
    // will be processed.
    (Accepted)

    // The message is declined because the destination worker is
    // overloaded at the moment. Source worker should decrease his
    // output window.
    (CongestionDeclined)

    // The message is declined by the destination worker since
    // it is unable to process it at the moment for reasons other
    // than congestion . Retrying with an appropriate backoff may help.
    (Declined)

    // The message with the given id was already accepted and queued
    // by the destination worker. One may retry pushing this message
    // again to check for processing completion.
    (ProcessingPending)

    // The message with the given id was already processed and its
    // results have been persisted. It makes no sense to retry pushing
    // this message again.
    (ProcessingFinished)
);

DECLARE_REFCOUNTED_STRUCT(TJobSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicJobSpec);

////////////////////////////////////////////////////////////////////////////////

// Per-message technical-overhead bytes accounted by the input/output buffers on top of
// the raw payload. The input buffer charges this in its limit math (IsUsageWithinLimits,
// inflatedFreeBytes); the distributor must charge the input-side cost against inflated_next_batch_byte_limits
// for every message it sends so it stops sending before the buffer's inflated bookkeeping
// overflows.
constexpr i64 InputMessageExtraTechnicalMemoryCost = 1000;
constexpr i64 OutputMessageExtraTechnicalMemoryCost = 2500;

//! The single source of truth for an input message's inflated size — raw payload plus the
//! input-side technical cost. Every input-side charge site (offer thinning, input-buffer
//! acquire, distributor charge) must go through this so a missed site can't desync the
//! source and destination accounting and reintroduce the back-pressure deadlock.
constexpr i64 InflatedByteSize(i64 byteSize)
{
    return byteSize + InputMessageExtraTechnicalMemoryCost;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
