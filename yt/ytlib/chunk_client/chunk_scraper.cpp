#include "chunk_scraper.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/small_vector.h>

#include <util/random/shuffle.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

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
    , OnChunkLocated_(onChunkLocated)
    , Invoker_(invoker)
    , ChunkIds_(std::move(chunkIds))
    , Logger(NLogging::TLogger(logger)
        .AddTag("ScraperTaskId: %v", TGuid::Create())
        .AddTag("CellTag: %v", CellTag_))
    , Proxy_(masterChannel)
    {
        Shuffle(ChunkIds_.begin(), ChunkIds_.end());
    }

    //! Starts periodic polling.
    void Start()
    {
        LOG_DEBUG("Starting scraper task (ChunkCount: %v)",
            ChunkIds_.size());

        LocateFuture_.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            if (!Started_) {
                Started_ = true;
                NextChunkIndex_ = 0;

                LocateChunks();
            }
        }).Via(Invoker_));
    }

    //! Stops periodic polling.
    TFuture<void> Stop()
    {
        LOG_DEBUG("Stopping scraper task (ChunkCount: %v)",
            ChunkIds_.size());

        Started_ = false;
        return LocateFuture_;
    }

private:
    const TChunkScraperConfigPtr Config_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TCellTag CellTag_;
    const TChunkLocatedHandler OnChunkLocated_;
    const IInvokerPtr Invoker_;

    TFuture<void> LocateFuture_ = VoidFuture;

    //! Non-const since we would like to shuffle it, to avoid scraping the same chunks
    //! on every restart.
    std::vector<TChunkId> ChunkIds_;

    const NLogging::TLogger Logger;

    TChunkServiceProxy Proxy_;

    std::atomic<bool> Started_ = { false };
    int NextChunkIndex_ = 0;


    void LocateChunks()
    {
        if (!Started_.load() || ChunkIds_.empty()) {
            return;
        }

        auto chunkCount = std::min<int>(ChunkIds_.size(), Config_->MaxChunksPerRequest);

        LocateFuture_ = Throttler_->Throttle(chunkCount)
            .Apply(BIND(&TScraperTask::DoLocateChunks, MakeWeak(this))
                .AsyncVia(Invoker_));
    }

    void DoLocateChunks(const TError& error)
    {
        if (!Started_) {
            return;
        }

        auto finallyGuard = Finally([&] () {
            LocateChunks();
        });

        if (!error.IsOK()) {
            LOG_WARNING(error, "Chunk scraper throttler failed unexpectedly");
            return;
        }

        if (NextChunkIndex_ >= ChunkIds_.size()) {
            NextChunkIndex_ = 0;
        }

        auto startIndex = NextChunkIndex_;
        auto req = Proxy_.LocateChunks();
        req->SetHeavy(true);

        constexpr int maxSampleChunkCount = 5;
        SmallVector<TChunkId, maxSampleChunkCount> sampleChunkIds;
        for (int chunkCount = 0; chunkCount < Config_->MaxChunksPerRequest; ++chunkCount) {
            ToProto(req->add_subrequests(), ChunkIds_[NextChunkIndex_]);
            if (sampleChunkIds.size() < maxSampleChunkCount) {
                sampleChunkIds.push_back(ChunkIds_[NextChunkIndex_]);
            }
            ++NextChunkIndex_;
            if (NextChunkIndex_ >= ChunkIds_.size()) {
                NextChunkIndex_ = 0;
            }
            if (NextChunkIndex_ == startIndex) {
                // Total number of chunks is less than MaxChunksPerRequest.
                break;
            }
        }

        LOG_DEBUG("Locating chunks (Count: %v, SampleChunkIds: %v)",
            req->subrequests_size(),
            sampleChunkIds);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Failed to locate chunks");
            return;
        }

        const auto& rsp = rspOrError.Value();
        YCHECK(req->subrequests_size() == rsp->subresponses_size());

        LOG_DEBUG("Chunks located (Count: %v, SampleChunkIds: %v)",
            req->subrequests_size(),
            sampleChunkIds);

        NodeDirectory_->MergeFrom(rsp->node_directory());

        for (int index = 0; index < req->subrequests_size(); ++index) {
            const auto& subrequest = req->subrequests(index);
            const auto& subresponse = rsp->subresponses(index);
            auto chunkId = FromProto<TChunkId>(subrequest);
            if (subresponse.missing()) {
                OnChunkLocated_.Run(chunkId, TChunkReplicaList(), true);
            } else {
                auto replicas = FromProto<TChunkReplicaList>(subresponse.replicas());
                OnChunkLocated_.Run(chunkId, replicas, false);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TScraperTask)

////////////////////////////////////////////////////////////////////////////////

TChunkScraper::TChunkScraper(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    TThrottlerManagerPtr throttlerManager,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const THashSet<TChunkId>& chunkIds,
    TChunkLocatedHandler onChunkLocated,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Invoker_(invoker)
    , ThrottlerManager_(throttlerManager)
    , Client_(client)
    , NodeDirectory_(nodeDirectory)
    , OnChunkLocated_(onChunkLocated)
    , Logger(logger)
{
    CreateTasks(chunkIds);
}

void TChunkScraper::Start()
{
    for (const auto& task : ScraperTasks_) {
        task->Start();
    }
}

TFuture<void> TChunkScraper::Stop()
{
    std::vector<TFuture<void>> futures;
    for (const auto& task : ScraperTasks_) {
        futures.push_back(task->Stop());
    }
    return Combine(futures);
}

void TChunkScraper::CreateTasks(const THashSet<TChunkId>& chunkIds)
{
    // Group chunks by cell tags.
    THashMap<TCellTag, int> cellTags;
    for (const auto& chunkId : chunkIds) {
        auto cellTag = CellTagFromId(chunkId);
        ++cellTags[cellTag];
    }

    THashMap<TCellTag, std::vector<TChunkId>> chunksByCells(cellTags.size());
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
        auto masterChannel = Client_->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower, cellTag);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
