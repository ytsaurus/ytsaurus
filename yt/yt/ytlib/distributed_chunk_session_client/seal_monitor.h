#pragma once

#include "seal_summary_fetcher.h"
#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <memory>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

using TDistributedChunkSessionSealSummaryFetchCallback =
    TCallback<TFuture<std::vector<TDistributedChunkSessionSealSummary>>(
        std::vector<NChunkClient::TChunkId>)>;

using TDistributedChunkSessionSealedCallback =
    TCallback<void(std::vector<TDistributedChunkSessionSealSummary>)>;

struct IDistributedChunkSessionSealSubscription
{
    virtual ~IDistributedChunkSessionSealSubscription() = default;

    //! Each chunk id must be new: it may not be already pending in this
    //! subscription nor tracked by any other active subscription.
    virtual void TrackChunks(std::vector<NChunkClient::TChunkId> chunkIds) = 0;
};

using TDistributedChunkSessionSealSubscriptionPtr =
    std::unique_ptr<IDistributedChunkSessionSealSubscription>;

//! Globally batches and polls master-side seal summaries for distributed-session chunks.
struct IDistributedChunkSessionSealMonitor
    : virtual public TRefCounted
{
    //! The callback may still be running when the subscription is destroyed;
    //! it must not capture raw pointers to the subscription owner.
    virtual TDistributedChunkSessionSealSubscriptionPtr Subscribe(
        TDistributedChunkSessionSealedCallback callback) = 0;

    //! Applies to polling decisions made after the reconfiguration.
    virtual void Reconfigure(TDistributedChunkSessionSealMonitorConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionSealMonitor)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionSealMonitorPtr CreateDistributedChunkSessionSealMonitor(
    TDistributedChunkSessionSealMonitorConfigPtr config,
    TDistributedChunkSessionSealSummaryFetchCallback fetchSealSummaries,
    IInvokerPtr invoker,
    NLogging::TLogger logger = DistributedChunkSessionLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
