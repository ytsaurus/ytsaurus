#include "stdafx.h"
#include "chunk_scraper.h"

#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_service_proxy.h>
#include <ytlib/node_tracker_client/node_directory.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

TChunkScraper::TChunkScraper(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    const NConcurrency::IThroughputThrottlerPtr throttler,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    yhash_set<TChunkId> chunkIds,
    TChunkLocatedHandler onChunkLocated,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Invoker_(invoker)
    , Throttler_(throttler)
    , Proxy_(masterChannel)
    , NodeDirectory_(nodeDirectory)
    , ChunkIds_(std::move(chunkIds))
    , OnChunkLocated_(onChunkLocated)
    , Logger(logger)
    , Started_(false)
    , NextChunkId_(NullChunkId)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TChunkScraper::LocateChunks, MakeWeak(this)),
        Config_->ChunkScratchPeriod))
{
    Logger.AddTag("Scraper: %p", this);
}

//! Starts periodic polling.
void TChunkScraper::Start()
{
    TGuard<TSpinLock> guard(SpinLock_);
    DoStart();
}

void TChunkScraper::DoStart()
{
    if (Started_ || ChunkIds_.empty()) {
        return;
    }
    Started_ = true;

    LOG_DEBUG("Starting scraper (ChunkCount: %v, ChunkScratchPeriod: %v, MaxChunksPerScratch: %v)",
        ChunkIds_.size(),
        Config_->ChunkScratchPeriod,
        Config_->MaxChunksPerScratch);

    PeriodicExecutor_->Start();
}

//! Stops periodic polling.
void TChunkScraper::Stop()
{
    TGuard<TSpinLock> guard(SpinLock_);
    DoStop();
}

void TChunkScraper::DoStop()
{
    if (!Started_) {
        return;
    }
    LOG_DEBUG("Stopping scraper (ChunkCount: %v)", ChunkIds_.size());

    Started_ = false;
    PeriodicExecutor_->Stop();
}

//! Reset a set of chunks scraper and start/stop scraper if necessary.
void TChunkScraper::Reset(const yhash_set<TChunkId>& chunkIds)
{
    TGuard<TSpinLock> guard(SpinLock_);
    ChunkIds_ = chunkIds;
    NextChunkId_ = NullChunkId;
    LOG_DEBUG("Reset scraper (ChunkCount: %v)", ChunkIds_.size());
    if (ChunkIds_.empty()) {
        DoStop();
    } else {
        DoStart();
    }
}

void TChunkScraper::LocateChunks()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (ChunkIds_.empty()) {
        return;
    }

    auto nextChunkIterator = ChunkIds_.find(NextChunkId_);
    if (nextChunkIterator == ChunkIds_.end()) {
        nextChunkIterator = ChunkIds_.begin();
    }
    auto startIterator = nextChunkIterator;
    auto req = Proxy_.LocateChunks();

    for (int chunkCount = 0; chunkCount < Config_->MaxChunksPerScratch; ++chunkCount) {
        ToProto(req->add_chunk_ids(), *nextChunkIterator);
        ++nextChunkIterator;
        if (nextChunkIterator == ChunkIds_.end()) {
            nextChunkIterator = ChunkIds_.begin();
        }
        if (nextChunkIterator == startIterator) {
            // Total number of chunks is less than MaxChunksPerScratch.
            break;
        }
    }
    NextChunkId_ = (nextChunkIterator != ChunkIds_.end()) ? *nextChunkIterator : NullChunkId;
    guard.Release();

    WaitFor(Throttler_->Throttle(req->chunk_ids_size()));

    LOG_DEBUG("Locating chunks (Count: %v)", req->chunk_ids_size());

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Failed to locate input chunks");
        return;
    }

    const auto& rsp = rspOrError.Value();

    LOG_DEBUG("Located %v chunks", rsp->chunks_size());

    NodeDirectory_->MergeFrom(rsp->node_directory());

    for (const auto& chunkInfo : rsp->chunks()) {
        auto chunkId = NYT::FromProto<TChunkId>(chunkInfo.chunk_id());
        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());
        OnChunkLocated_.Run(std::move(chunkId), std::move(replicas));
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
