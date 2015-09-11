#pragma once

#include "public.h"

#include <ytlib/api/public.h>
#include <ytlib/chunk_client/chunk_service_proxy.h>
#include <ytlib/node_tracker_client/public.h>
#include <ytlib/object_client/public.h>

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
        TThrottlerManagerPtr throttlerManager,
        NApi::IClientPtr masterClient,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const yhash_set<TChunkId>& chunkIds,
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
    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    TThrottlerManagerPtr ThrottlerManager_;
    NApi::IClientPtr MasterClient_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    TChunkLocatedHandler OnChunkLocated_;
    NLogging::TLogger Logger;

    TSpinLock SpinLock_;
    std::vector<TScraperTaskPtr> ScraperTasks_;

    void DoStart();
    void DoStop();
    void CreateTasks(const yhash_set<TChunkId>& chunkIds);
};

DEFINE_REFCOUNTED_TYPE(TChunkScraper)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
