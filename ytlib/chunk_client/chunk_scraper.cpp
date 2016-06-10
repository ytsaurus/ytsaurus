#include "chunk_scraper.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/api/client.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;

using NYT::FromProto;

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
    , Throttler_(throttler)
    , NodeDirectory_(nodeDirectory)
    , CellTag_(cellTag)
    , ChunkIds_(std::move(chunkIds))
    , OnChunkLocated_(onChunkLocated)
    , Logger(logger)
    , Proxy_(masterChannel)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TScraperTask::LocateChunks, MakeWeak(this)),
        TDuration::Zero()))
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

        LOG_DEBUG("Starting scraper task (ChunkCount: %v)",
            ChunkIds_.size());

        PeriodicExecutor_->Start();
    }

    //! Stops periodic polling.
    void Stop()
    {
        if (!Started_) {
            return;
        }
        LOG_DEBUG("Stopping scraper task (ChunkCount: %v)",
            ChunkIds_.size());

        Started_ = false;
        PeriodicExecutor_->Stop();
    }

private:
    const TChunkScraperConfigPtr Config_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TCellTag CellTag_;
    const std::vector<TChunkId> ChunkIds_;
    const TChunkLocatedHandler OnChunkLocated_;

    NLogging::TLogger Logger;
    TChunkServiceProxy Proxy_;

    bool Started_ = false;
    int NextChunkIndex_ = 0;

    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;


    void LocateChunks()
    {
        if (ChunkIds_.empty()) {
            return;
        }

        if (NextChunkIndex_ >= ChunkIds_.size()) {
            NextChunkIndex_ = 0;
        }

        auto startIndex = NextChunkIndex_;
        auto req = Proxy_.LocateChunks();

        for (int chunkCount = 0; chunkCount < Config_->MaxChunksPerScratch; ++chunkCount) {
            ToProto(req->add_subrequests(), ChunkIds_[NextChunkIndex_]);
            ++NextChunkIndex_;
            if (NextChunkIndex_ >= ChunkIds_.size()) {
                NextChunkIndex_ = 0;
            }
            if (NextChunkIndex_ == startIndex) {
                // Total number of chunks is less than MaxChunksPerScratch.
                break;
            }
        }

        WaitFor(Throttler_->Throttle(req->subrequests_size()));

        LOG_DEBUG("Locating chunks (Count: %v)", req->subrequests_size());

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Failed to locate chunks");
            return;
        }

        const auto& rsp = rspOrError.Value();
        YCHECK(req->subrequests_size() == rsp->subresponses_size());

        LOG_DEBUG("Chunks located");

        NodeDirectory_->MergeFrom(rsp->node_directory());

        for (int index = 0; index < req->subrequests_size(); ++index) {
            const auto& subrequest = req->subrequests(index);
            const auto& subresponse = rsp->subresponses(index);
            if (!subresponse.missing()) {
                auto chunkId = FromProto<TChunkId>(subrequest);
                auto replicas = FromProto<TChunkReplicaList>(subresponse.replicas());
                OnChunkLocated_.Run(chunkId, replicas);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TScraperTask)

///////////////////////////////////////////////////////////////////////////////

TChunkScraper::TChunkScraper(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    TThrottlerManagerPtr throttlerManager,
    NApi::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const yhash_set<TChunkId>& chunkIds,
    TChunkLocatedHandler onChunkLocated,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Invoker_(invoker)
    , ThrottlerManager_(throttlerManager)
    , MasterClient_(client)
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
    for (const auto& task : ScraperTasks_) {
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

//! Create scraper tasks for each cell.
void TChunkScraper::CreateTasks(const yhash_set<TChunkId>& chunkIds)
{
    // Group chunks by cell tags.
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

    for (const auto& cellChunks : chunksByCells) {
        auto cellTag = cellChunks.first;
        auto throttler = ThrottlerManager_->GetThrottler(cellTag);
        auto masterChannel = MasterClient_->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Leader, cellTag);
        auto task = New<TScraperTask>(
            Config_,
            Invoker_,
            throttler,
            masterChannel,
            NodeDirectory_,
            cellTag,
            std::move(cellChunks.second),
            OnChunkLocated_,
            Logger);
        ScraperTasks_.push_back(std::move(task));
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
