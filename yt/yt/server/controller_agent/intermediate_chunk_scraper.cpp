#include "intermediate_chunk_scraper.h"

#include "config.h"
#include "private.h"

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
    IInvokerPtr scraperInvoker,
    TThrottlerManagerPtr throttlerManager,
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TGetChunksCallback getChunksCallback,
    TChunkBatchLocatedHandler onChunkBatchLocated,
    const NLogging::TLogger& logger)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , InvokerPool_(std::move(invokerPool))
    , ScraperInvoker_(std::move(scraperInvoker))
    , ThrottlerManager_(std::move(throttlerManager))
    , Client_(std::move(client))
    , NodeDirectory_(std::move(nodeDirectory))
    , GetChunksCallback_(std::move(getChunksCallback))
    , OnChunkBatchLocated_(std::move(onChunkBatchLocated))
    , Logger(logger)
{ }

void TIntermediateChunkScraper::Start()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    if (!Started_) {
        Started_ = true;
        ResetChunkScraper();
    }
}

void TIntermediateChunkScraper::Restart()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    if (!Started_ || ResetScheduled_) {
        return;
    }

    auto deadline = ResetInstant_ + Config_->RestartTimeout;
    if (deadline < TInstant::Now()) {
        ResetChunkScraper();
    } else {
        TDelayedExecutor::Submit(
            BIND(&TIntermediateChunkScraper::ResetChunkScraper, MakeWeak(this))
                .Via(Invoker_),
            deadline + TDuration::Seconds(1)); // +1 second to be sure.
        ResetScheduled_ = true;
    }
}

void TIntermediateChunkScraper::ResetChunkScraper()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    ResetInstant_ = TInstant::Now();
    ResetScheduled_ = false;

    if (ChunkScraper_) {
        ChunkScraper_->Stop();
    }

    auto intermediateChunks = GetChunksCallback_();

    YT_LOG_DEBUG(
        "Reset intermediate chunk scraper (ChunkCount: %v)",
        intermediateChunks.size());
    ChunkScraper_ = New<TChunkScraper>(
        Config_,
        ScraperInvoker_,
        ThrottlerManager_,
        Client_,
        NodeDirectory_,
        std::move(intermediateChunks),
        OnChunkBatchLocated_,
        Logger);
    ChunkScraper_->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
