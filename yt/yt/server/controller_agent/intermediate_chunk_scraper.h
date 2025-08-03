#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using TGetChunksCallback = TCallback<THashSet<NChunkClient::TChunkId>()>;

class TIntermediateChunkScraper
    : public TRefCounted
{
public:
    TIntermediateChunkScraper(
        TIntermediateChunkScraperConfigPtr config,
        IInvokerPtr invoker,
        IInvokerPoolPtr invokerPool,
        IInvokerPtr scraperInvoker,
        NChunkClient::TThrottlerManagerPtr throttlerManager,
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        TGetChunksCallback getChunksCallback,
        NChunkClient::TChunkBatchLocatedHandler onChunkLocated,
        const NLogging::TLogger& logger);

    void Start();

    void Restart();

private:
    const TIntermediateChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const IInvokerPoolPtr InvokerPool_;
    const IInvokerPtr ScraperInvoker_;
    const NChunkClient::TThrottlerManagerPtr ThrottlerManager_;
    const NApi::NNative::IClientPtr Client_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    const TGetChunksCallback GetChunksCallback_;
    const NChunkClient::TChunkBatchLocatedHandler OnChunkBatchLocated_;

    NChunkClient::TChunkScraperPtr ChunkScraper_;

    bool Started_ = false;
    bool ResetScheduled_ = false;

    TInstant ResetInstant_;

    NLogging::TLogger Logger;

    void ResetChunkScraper();
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
