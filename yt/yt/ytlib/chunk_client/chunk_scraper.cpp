#include "chunk_scraper.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/async_looper.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <util/random/shuffle.h>

namespace NYT::NChunkClient {

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
    , Logger(logger.WithTag("ScraperTaskId: %v, CellTag: %v",
        TGuid::Create(),
        CellTag_))
    , Proxy_(masterChannel)
    {
        Shuffle(ChunkIds_.begin(), ChunkIds_.end());
        Looper_ = New<TAsyncLooper>(
            Invoker_,
            BIND_NO_PROPAGATE([weakThis = MakeWeak(this)] (bool cleanStart) {
                if (auto strongThis = weakThis.Lock()) {
                    return strongThis->LocateChunksAsync(cleanStart);
                }
                // Break loop.
                return TFuture<void>();
            }),
            BIND_NO_PROPAGATE([weakThis = MakeWeak(this)] (bool cleanStart) {
                if (auto strongThis = weakThis.Lock()) {
                    strongThis->LocateChunksSync(cleanStart);
                }
            }),
            Logger.WithTag("AsyncLooper: %v", "ScraperTask"));
    }

    //! Starts periodic polling.
    void Start()
    {
        YT_LOG_DEBUG("Starting scraper task (ChunkCount: %v)",
            ChunkIds_.size());

        Looper_->Start();
    }

    //! Stops periodic polling.
    void Stop()
    {
        YT_LOG_DEBUG("Stopping scraper task (ChunkCount: %v)",
            ChunkIds_.size());

        Looper_->Stop();
    }

private:
    const TChunkScraperConfigPtr Config_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TCellTag CellTag_;
    const TChunkLocatedHandler OnChunkLocated_;
    const IInvokerPtr Invoker_;

    TAsyncLooperPtr Looper_;

    //! Non-const since we would like to shuffle it, to avoid scraping the same chunks
    //! on every restart.
    std::vector<TChunkId> ChunkIds_;

    const NLogging::TLogger Logger;

    TChunkServiceProxy Proxy_;

    int NextChunkIndex_ = 0;


    TFuture<void> LocateChunksAsync(bool /*cleanStart*/)
    {
        auto chunkCount = std::min<int>(ChunkIds_.size(), Config_->MaxChunksPerRequest);

        if (chunkCount == 0) {
            return TFuture<void>(); // Break loop.
        }

        return Throttler_->Throttle(chunkCount);
    }

    void LocateChunksSync(bool cleanStart)
    {
        if (cleanStart) {
            NextChunkIndex_ = 0;
        }

        DoLocateChunks();
    }

    void DoLocateChunks()
    {
        if (NextChunkIndex_ >= std::ssize(ChunkIds_)) {
            NextChunkIndex_ = 0;
        }

        auto startIndex = NextChunkIndex_;
        auto req = Proxy_.LocateChunks();
        req->SetRequestHeavy(true);
        req->SetResponseHeavy(true);

        constexpr int maxSampleChunkCount = 5;
        TCompactVector<TChunkId, maxSampleChunkCount> sampleChunkIds;
        for (int chunkCount = 0; chunkCount < Config_->MaxChunksPerRequest; ++chunkCount) {
            ToProto(req->add_subrequests(), ChunkIds_[NextChunkIndex_]);
            if (sampleChunkIds.size() < maxSampleChunkCount) {
                sampleChunkIds.push_back(ChunkIds_[NextChunkIndex_]);
            }
            ++NextChunkIndex_;
            if (NextChunkIndex_ >= std::ssize(ChunkIds_)) {
                NextChunkIndex_ = 0;
            }
            if (NextChunkIndex_ == startIndex) {
                // Total number of chunks is less than MaxChunksPerRequest.
                break;
            }
        }

        YT_LOG_DEBUG("Locating chunks (Count: %v, SampleChunkIds: %v)",
            req->subrequests_size(),
            sampleChunkIds);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to locate chunks");
            return;
        }

        const auto& rsp = rspOrError.Value();
        YT_VERIFY(req->subrequests_size() == rsp->subresponses_size());

        YT_LOG_DEBUG("Chunks located (Count: %v, SampleChunkIds: %v)",
            req->subrequests_size(),
            sampleChunkIds);

        NodeDirectory_->MergeFrom(rsp->node_directory());

        for (int index = 0; index < req->subrequests_size(); ++index) {
            const auto& subrequest = req->subrequests(index);
            const auto& subresponse = rsp->subresponses(index);
            auto chunkId = FromProto<TChunkId>(subrequest);
            if (subresponse.missing()) {
                OnChunkLocated_(chunkId, TChunkReplicaWithMediumList(), /*missing*/ true);
                continue;
            }
            if (subresponse.replicas_size() == 0) {
                TChunkReplicaWithMediumList replicas;
                for (auto replica : FromProto<TChunkReplicaList>(subresponse.legacy_replicas())) {
                    replicas.emplace_back(replica);
                }
                OnChunkLocated_(chunkId, replicas, /*missing*/ false);
            } else {
                auto replicas = FromProto<TChunkReplicaWithMediumList>(subresponse.replicas());
                OnChunkLocated_(chunkId, replicas, /*missing*/ false);
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
    for (const auto& task : ScraperTasks_) {
        task->Stop();
    }
    return VoidFuture;
}

void TChunkScraper::CreateTasks(const THashSet<TChunkId>& chunkIds)
{
    // Group chunks by cell tags.
    THashMap<TCellTag, int> cellTags;
    for (auto chunkId : chunkIds) {
        auto cellTag = CellTagFromId(chunkId);
        ++cellTags[cellTag];
    }

    THashMap<TCellTag, std::vector<TChunkId>> chunksByCells(cellTags.size());
    for (const auto& cellTag : cellTags) {
        chunksByCells[cellTag.first].reserve(cellTag.second);
    }

    for (auto chunkId : chunkIds) {
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

} // namespace NYT::NChunkClient
