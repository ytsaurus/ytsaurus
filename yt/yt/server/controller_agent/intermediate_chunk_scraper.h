#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using TGetChunksCallback = std::function<THashSet<NChunkClient::TChunkId>()>;

class TIntermediateChunkScraper
    : public TRefCounted
{
public:
    TIntermediateChunkScraper(
        TIntermediateChunkScraperConfigPtr config,
        IInvokerPtr invoker,
        IInvokerPoolPtr invokerPool,
        IInvokerPtr scraperHeavyInvoker,
        NChunkClient::TThrottlerManagerPtr throttlerManager,
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        TGetChunksCallback getChunksCallback,
        NChunkClient::TChunkBatchLocatedHandler onChunkBatchLocated,
        NChunkClient::TChunkScraperAvailabilityPolicy availabilityPolicy,
        NLogging::TLogger logger);

    void Start();

    void UpdateChunkSet();

    void OnChunkBecameUnavailable(NChunkClient::TChunkId chunk);

private:
    const TIntermediateChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const IInvokerPoolPtr InvokerPool_;

    const TGetChunksCallback GetChunksCallback_;

    THashSet<NChunkClient::TChunkId> LastUpdateChunks_;
    NChunkClient::TChunkScraperPtr ChunkScraper_;

    bool Started_ = false;
    bool UpdateScheduled_ = false;

    TInstant UpdateInstant_;

    NLogging::TLogger Logger;

    void UpdateChunkScraper();
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
