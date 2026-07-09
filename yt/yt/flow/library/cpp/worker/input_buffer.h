#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/stream_inflight_limits.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct IInputBuffer
    : public TRefCounted
{
    using TOnProcessedCallback = TCallback<void(TJobId, std::vector<TMessageId>)>;

    using TConnectionStreamOfferBucket = std::pair<TSystemTimestamp, i64>;    // (MinOrderTimestamp, InflatedByteSize).
    using TConnectionStreamOffer = std::vector<TConnectionStreamOfferBucket>; // Sorted in reversed order.
    using TConnectionOffer = THashMap<TStreamId, TConnectionStreamOffer>;

    virtual void Reconfigure(TDynamicComputationSpecPtr dynamicSpec) = 0;
    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;

    // Returns instant result (accepted/declined) and if accepted, calls #onProcessed when message is processed.
    virtual TFuture<std::vector<EMessageDeliveryState>> AddMessages(
        TGuid connectionId,
        std::vector<TInputMessageConstPtr> messages,
        TOnProcessedCallback onProcessed) = 0;

    virtual void AddConnectionOffer(TGuid connectionId, TConnectionOffer offer) = 0;
    virtual TFuture<THashMap<TStreamId, i64>> GetConnectionLimits(TGuid connectionId) = 0;
    // Pass-by-value so the caller can `std::move` ownership in; the implementation moves the
    // deque through Passed() into the SerializedInvoker BIND closure, avoiding a copy.
    virtual void MarkPersisted(std::deque<TMessageId> messageIds) = 0;

    virtual TFuture<std::vector<TInputMessageConstPtr>> GetInputBatch(const THashSet<TStreamId>& allowedStreams) = 0;

    //! Returns the smallest stabilized event (alignment) timestamp across all
    //! pending messages, or InfinitySystemTimestamp if the buffer is empty.
    //! Reflects the logical time at which the job is currently lagging — used
    //! as a priority key for distributed throttler clients.
    /*!
     *  \note Thread affinity: any
     */
    virtual TSystemTimestamp GetMinStabilizedEventTimestamp() = 0;

    virtual const TComputationId& GetComputationId() = 0;
};

DEFINE_REFCOUNTED_TYPE(IInputBuffer);

////////////////////////////////////////////////////////////////////////////////

IInputBufferPtr CreateInputBuffer(
    TJobId jobId,
    NFlow::TStreamLimitUsageStateMap streamLimitUsageStates,
    TComputationSpecPtr computationSpec,
    TComputationId computationId,
    TDynamicComputationSpecPtr dynamicSpec,
    IInvokerPtr finalizerPoolInvoker,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
