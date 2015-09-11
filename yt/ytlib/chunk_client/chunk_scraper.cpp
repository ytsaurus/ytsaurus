#include "stdafx.h"
#include "chunk_scraper.h"

#include "config.h"
#include "private.h"

#include <ytlib/api/client.h>
#include <ytlib/chunk_client/chunk_service_proxy.h>
#include <ytlib/chunk_client/throttler_manager.h>
#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/public.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

class TScraperTask
    : public TRefCounted
{
public:
    TScraperTask(
        const TChunkScraperConfigPtr config,
        const IInvokerPtr invoker,
        const NConcurrency::IThroughputThrottlerPtr throttler,
        NRpc::IChannelPtr masterChannel,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        TCellTag cellTag,
        std::vector<TChunkId> chunkIds,
        TChunkLocatedHandler onChunkLocated,
        const NLogging::TLogger& logger)
        : Config_(config)
        , Invoker_(invoker)
        , Throttler_(throttler)
        , Proxy_(masterChannel)
        , NodeDirectory_(nodeDirectory)
        , CellTag_(cellTag)
        , ChunkIds_(std::move(chunkIds))
        , OnChunkLocated_(onChunkLocated)
        , Logger(logger)
        , Started_(false)
        , NextChunkIndex_(0)
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TScraperTask::LocateChunks, MakeWeak(this)),
            Config_->ChunkScratchPeriod))
    {
        Logger.AddTag("ScraperTask: %p", this);
        Logger.AddTag("CellTag: %v", CellTag_);
    }

    //! Starts periodic polling.
    void Start()
    {
        if (Started_ || ChunkIds_.empty()) {
            return;
        }
        Started_ = true;
        NextChunkIndex_ = 0;

        LOG_DEBUG("Starting scraper task (ChunkCount: %v, ChunkScratchPeriod: %v, MaxChunksPerScratch: %v)",
            ChunkIds_.size(),
            Config_->ChunkScratchPeriod,
            Config_->MaxChunksPerScratch);

        PeriodicExecutor_->Start();
    }

    //! Stops periodic polling.
    void Stop()
    {
        if (!Started_) {
            return;
        }
        LOG_DEBUG("Stopping scraper task (ChunkCount: %v)", ChunkIds_.size());

        Started_ = false;
        PeriodicExecutor_->Stop();
    }

    void LocateChunks()
    {
        if (ChunkIds_.empty()) {
            return;
        }

        if (NextChunkIndex_ >=  ChunkIds_.size()) {
            NextChunkIndex_ = 0;
        }
        auto startIndex = NextChunkIndex_;
        auto req = Proxy_.LocateChunks();

        for (int chunkCount = 0; chunkCount < Config_->MaxChunksPerScratch; ++chunkCount) {
            ToProto(req->add_chunk_ids(), ChunkIds_[NextChunkIndex_]);
            ++NextChunkIndex_;
            if (NextChunkIndex_ >= ChunkIds_.size()) {
                NextChunkIndex_ = 0;
            }
            if (NextChunkIndex_ == startIndex) {
                // Total number of chunks is less than MaxChunksPerScratch.
                break;
            }
        }

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
private:

    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;

    TChunkServiceProxy Proxy_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    TCellTag CellTag_;
    std::vector<TChunkId> ChunkIds_;
    TChunkLocatedHandler OnChunkLocated_;
    NLogging::TLogger Logger;

    bool Started_ = false;
    size_t NextChunkIndex_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
};

DEFINE_REFCOUNTED_TYPE(TScraperTask)

///////////////////////////////////////////////////////////////////////////////

TChunkScraper::TChunkScraper(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    TThrottlerManagerPtr throttlerManager,
    NApi::IClientPtr masterClient,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const yhash_set<TChunkId>& chunkIds,
    TChunkLocatedHandler onChunkLocated,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Invoker_(invoker)
    , ThrottlerManager_(throttlerManager)
    , MasterClient_(masterClient)
    , NodeDirectory_(nodeDirectory)
    , OnChunkLocated_(onChunkLocated)
    , Logger(logger)
{
    CreateTasks(chunkIds);
}

//! Starts periodic polling.
void TChunkScraper::Start()
{
    TGuard<TSpinLock> guard(SpinLock_);
    DoStart();
}

void TChunkScraper::DoStart()
{
    for (auto& task : ScraperTasks_) {
        task->Start();
    }
}

//! Stops periodic polling.
void TChunkScraper::Stop()
{
    TGuard<TSpinLock> guard(SpinLock_);
    DoStop();
}

void TChunkScraper::DoStop()
{
    for (auto& task : ScraperTasks_) {
        task->Stop();
    }
}

//! Reset a set of chunks scraper and start/stop scraper if necessary.
void TChunkScraper::Reset(const yhash_set<TChunkId>& chunkIds)
{
    TGuard<TSpinLock> guard(SpinLock_);
    DoStop();
    ScraperTasks_.clear();
    CreateTasks(chunkIds);
    DoStart();
}

//! Separate chunks by cellTags.
static yhash_map<TCellTag, std::vector<TChunkId>> SeparateChunks(const yhash_set<TChunkId>& chunkIds)
{
    yhash_map<TCellTag, int> cellTags;
    for (const auto& chunkId : chunkIds) {
        auto cellTag = CellTagFromId(chunkId);
        ++cellTags[cellTag];
    }

    yhash_map<TCellTag, std::vector<TChunkId>> chunksByCells(cellTags.size());
    for (const auto& cellTag : cellTags) {
        chunksByCells[cellTag.first].reserve(cellTag.second);
    }

    for (const auto& chunkId : chunkIds) {
        auto cellTag = CellTagFromId(chunkId);
        chunksByCells[cellTag].push_back(chunkId);
    }

    return chunksByCells;
}

//! Create scraper tasks for each cell.
void TChunkScraper::CreateTasks(const yhash_set<TChunkId>& chunkIds)
{
    auto chunksByCells = SeparateChunks(chunkIds);

    for (const auto& cellChunks : chunksByCells) {
        auto cellTag = cellChunks.first;
        auto throttler = ThrottlerManager_->GetThrottler(cellTag);
        YCHECK(throttler);
        auto masterChannel = MasterClient_->GetMasterChannel(NApi::EMasterChannelKind::Leader, cellTag);
        YCHECK(masterChannel);
        auto task = New<TScraperTask>(
            Config_,
            Invoker_,
            throttler,
            masterChannel,
            NodeDirectory_,
            cellTag,
            std::move(cellChunks.second),
            OnChunkLocated_,
            Logger
        );
        ScraperTasks_.push_back(std::move(task));
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
