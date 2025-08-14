#include "intermediate_chunk_scraper.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NControllerAgent {

using namespace NChunkClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIntermediateChunkScraper::TIntermediateChunkScraper(
    TIntermediateChunkScraperConfigPtr config,
    IInvokerPtr invoker,
    IInvokerPoolPtr invokerPool,
    IInvokerPtr scraperHeavyInvoker,
    TThrottlerManagerPtr throttlerManager,
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TGetChunksCallback getChunksCallback,
    TChunkBatchLocatedHandler onChunkBatchLocated,
    TChunkScraperAvailabilityPolicy availabilityPolicy,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , InvokerPool_(std::move(invokerPool))
    , GetChunksCallback_(std::move(getChunksCallback))
    , ChunkScraper_(New<TChunkScraper>(
        Config_,
        Invoker_,
        std::move(scraperHeavyInvoker),
        std::move(throttlerManager),
        std::move(client),
        std::move(nodeDirectory),
        std::move(onChunkBatchLocated),
        availabilityPolicy,
        logger))
    , Logger(std::move(logger))
{ }

void TIntermediateChunkScraper::Start()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    if (!Started_) {
        Started_ = true;
        UpdateChunkScraper();
    }
    ChunkScraper_->Start();
}

void TIntermediateChunkScraper::UpdateChunkSet()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    if (!Started_ || UpdateScheduled_) {
        return;
    }

    auto deadline = UpdateInstant_ + Config_->RestartTimeout;
    if (deadline < TInstant::Now()) {
        UpdateChunkScraper();
    } else {
        TDelayedExecutor::Submit(
            BIND(&TIntermediateChunkScraper::UpdateChunkScraper, MakeWeak(this))
                .Via(Invoker_),
            deadline + TDuration::Seconds(1)); // +1 second to be sure.
        UpdateScheduled_ = true;
    }
}

void TIntermediateChunkScraper::OnChunkBecameUnavailable(TChunkId chunk)
{
    ChunkScraper_->OnChunkBecameUnavailable(chunk);
}

void TIntermediateChunkScraper::UpdateChunkScraper()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    UpdateInstant_ = TInstant::Now();
    UpdateScheduled_ = false;

    auto intermediateChunks = GetChunksCallback_();

    YT_LOG_DEBUG(
        "Update intermediate chunk scraper (ChunkCount: %v)",
        intermediateChunks.size());

    for (auto oldChunk : LastUpdateChunks_) {
        if (!intermediateChunks.contains(oldChunk)) {
            ChunkScraper_->Remove(oldChunk);
        }
    }

    for (auto chunk : intermediateChunks) {
        ChunkScraper_->Add(chunk);
    }

    LastUpdateChunks_ = std::move(intermediateChunks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
