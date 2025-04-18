#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TScrapedChunkInfo
{
    TChunkId ChunkId;
    TChunkReplicaWithMediumList Replicas;
    bool Missing;
};

using TChunkBatchLocatedHandler = TCallback<void(const std::vector<TScrapedChunkInfo>&)>;

//! A chunk scraper for unavailable chunks.
class TChunkScraper
    : public TRefCounted
{
public:
    TChunkScraper(
        TChunkScraperConfigPtr config,
        IInvokerPtr invoker,
        TThrottlerManagerPtr throttlerManager,
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const THashSet<TChunkId>& chunkIds,
        TChunkBatchLocatedHandler onChunkBatchLocated,
        NLogging::TLogger logger);

    ~TChunkScraper();

    //! Starts periodic polling.
    /*!
     *  Should be called when operation preparation is complete.
     *  Safe to call multiple times.
     */
    void Start();

    //! Stops periodic polling.
    void Stop();

private:
    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TThrottlerManagerPtr ThrottlerManager_;
    const NApi::NNative::IClientPtr Client_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TChunkBatchLocatedHandler OnChunkBatchLocated_;
    const NLogging::TLogger Logger;

    std::vector<TScraperTaskPtr> ScraperTasks_;

    //! Create scraper tasks for each cell.
    void CreateTasks(const THashSet<TChunkId>& chunkIds);
};

DEFINE_REFCOUNTED_TYPE(TChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
