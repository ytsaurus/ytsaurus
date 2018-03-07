#include "intermediate_chunk_scraper.h"

#include "config.h"
#include "private.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NControllerAgent {

using namespace NChunkClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIntermediateChunkScraper::TIntermediateChunkScraper(
    const TIntermediateChunkScraperConfigPtr& config,
    const IInvokerPtr& invoker,
    const TThrottlerManagerPtr& throttlerManager,
    const INativeClientPtr& client,
    const TNodeDirectoryPtr& nodeDirectory,
    TGetChunksCallback getChunksCallback,
    TChunkLocatedHandler onChunkLocated,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Invoker_(invoker)
    , ThrottlerManager_(throttlerManager)
    , Client_(client)
    , NodeDirectory_(nodeDirectory)
    , GetChunksCallback_(getChunksCallback)
    , OnChunkLocated_(onChunkLocated)
    , Logger(logger)
{ }

void TIntermediateChunkScraper::Start()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    if (!Started_) {
        Started_ = true;
        ResetChunkScraper();
    }
}

void TIntermediateChunkScraper::Restart()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

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
    VERIFY_INVOKER_AFFINITY(Invoker_);

    ResetInstant_ = TInstant::Now();
    ResetScheduled_ = false;

    if (ChunkScraper_) {
        ChunkScraper_->Stop();
    }

    auto intermediateChunks = GetChunksCallback_();

    LOG_DEBUG("Reset intermediate chunk scraper (ChunkCount: %v)",
        intermediateChunks.size());
    ChunkScraper_ = New<TChunkScraper>(
        Config_,
        Invoker_,
        ThrottlerManager_,
        Client_,
        NodeDirectory_,
        intermediateChunks,
        OnChunkLocated_,
        Logger);
    ChunkScraper_->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
