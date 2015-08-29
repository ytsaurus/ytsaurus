#pragma once

#include "public.h"

#include <ytlib/chunk_client/chunk_service_proxy.h>
#include <ytlib/node_tracker_client/public.h>

#include <core/logging/log.h>
#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

typedef TCallback<void(const TChunkId& chunkId, const TChunkReplicaList& replicas)> TChunkLocatedHandler;

//! A chunk scraper for unavailable chunks.
class TChunkScraper
    : public TRefCounted
{
public:
    TChunkScraper(
        const TChunkScraperConfigPtr config,
        const IInvokerPtr invoker,
        const NConcurrency::IThroughputThrottlerPtr throttler,
        NRpc::IChannelPtr masterChannel,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        yhash_set<TChunkId> chunkIds,
        TChunkLocatedHandler onChunkLocated,
        const NLogging::TLogger& logger);

    //! Starts periodic polling.
    /*!
     *  Should be called when operation preparation is complete.
     *  Safe to call multiple times.
     */
    void Start();

    //! Stops periodic polling.
    void Stop();

    //! Reset a set of chunks scraper and start/stop scraper if necessary.
    void Reset(const yhash_set<TChunkId>& chunkIds);

private:
    void DoStart();
    void DoStop();
    void LocateChunks();

    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;

    TChunkServiceProxy Proxy_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    yhash_set<TChunkId> ChunkIds_;
    TChunkLocatedHandler OnChunkLocated_;
    NLogging::TLogger Logger;

    TSpinLock SpinLock_;
    bool Started_ = false;
    TChunkId NextChunkId_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

};

DEFINE_REFCOUNTED_TYPE(TChunkScraper)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
