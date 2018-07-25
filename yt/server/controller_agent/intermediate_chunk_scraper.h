#pragma once

#include "public.h"

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/chunk_scraper.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<THashSet<NChunkClient::TChunkId>()> TGetChunksCallback;

class TIntermediateChunkScraper
    : public TRefCounted
{
public:
    TIntermediateChunkScraper(
        const TIntermediateChunkScraperConfigPtr& config,
        const IInvokerPtr& invoker,
        const NChunkClient::TThrottlerManagerPtr& throttlerManager,
        const NApi::NNative::IClientPtr& client,
        const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
        TGetChunksCallback getChunksCallback,
        NChunkClient::TChunkLocatedHandler onChunkLocated,
        const NLogging::TLogger& logger);

    void Start();

    void Restart();

private:
    const TIntermediateChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const NChunkClient::TThrottlerManagerPtr ThrottlerManager_;
    const NApi::NNative::IClientPtr Client_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    const TGetChunksCallback GetChunksCallback_;
    const NChunkClient::TChunkLocatedHandler OnChunkLocated_;

    NChunkClient::TChunkScraperPtr ChunkScraper_;

    bool Started_ = false;
    bool ResetScheduled_ = false;

    TInstant ResetInstant_;

    NLogging::TLogger Logger;

    void ResetChunkScraper();
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
