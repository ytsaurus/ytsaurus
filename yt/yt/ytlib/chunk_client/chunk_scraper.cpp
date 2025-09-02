#include "chunk_scraper.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/async_looper.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;

using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;

using NYT::FromProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct IChunkScraperQueue
{
    virtual std::vector<TChunkId> ExtractBatch(int maxBatchSize) = 0;

    virtual void Add(TChunkId chunkId) = 0;
    virtual void Remove(TChunkId chunkId) = 0;
    virtual void OnBecameUnavailable(TChunkId chunkId)
    {
        OnLocatedUnavailable(chunkId);
    }

    virtual void OnLocatedAvailable(TChunkId chunkId) = 0;
    virtual void OnLocatedUnavailable(TChunkId chunkId) = 0;
    virtual void OnLocatedMissing(TChunkId chunkId) = 0;

    virtual i64 GetSize() const = 0;

    virtual ~IChunkScraperQueue() = default;
};

class TRoundRobinQueue final
    : public IChunkScraperQueue
{
public:
    TRoundRobinQueue() = default;

    TChunkId ExtractNext()
    {
        YT_VERIFY(GetSize() > 0);

        if (NextChunkToExtract_ == Queue_.end()) {
            NextChunkToExtract_ = Queue_.begin();
        }

        return NextChunkToExtract_++->ChunkId;
    }

    std::vector<TChunkId> ExtractBatch(int maxBatchSize) override
    {
        int batchSize = std::min<i64>(GetSize(), maxBatchSize);
        std::vector<TChunkId> result;
        result.reserve(batchSize);
        for (int i = 0; i < batchSize; ++i) {
            result.push_back(ExtractNext());
        }
        return result;
    }

    void OnLocatedAvailable(TChunkId /*chunkId*/) override
    {
        // Noop, be happy.
    }

    void OnLocatedUnavailable(TChunkId /*chunkId*/) override
    {
        // Noop, be stupid.
    }

    void OnLocatedMissing(TChunkId /*chunkId*/) override
    {
        // Noop, be simple.
    }

    void Add(TChunkId chunkId) override
    {
        YT_VERIFY(chunkId != NullChunkId);

        auto [it, isNew] = Chunks_.emplace(
            std::piecewise_construct,
            std::tuple{chunkId},
            std::tuple{chunkId});
        if (isNew) {
            Queue_.PushBack(&it->second);
        }
    }

    void Remove(TChunkId chunkId) override
    {
        YT_VERIFY(chunkId != NullChunkId);

        auto it = Chunks_.find(chunkId);
        if (it == Chunks_.end()) {
            return;
        }
        auto* node = &it->second;
        if (node == NextChunkToExtract_->Node()) {
            ++NextChunkToExtract_;
        }
        Queue_.Remove(node);
        Chunks_.erase(it);
    }

    i64 GetSize() const override
    {
        return Chunks_.size();
    }

private:
    struct TQueueNode
        : public TIntrusiveListItem<TQueueNode>
    {
        TChunkId ChunkId;

        TQueueNode(TChunkId chunkId)
            : ChunkId(chunkId)
        { }
    };

    // TODO(namorniradnug): Implement intrusive hashmap and use it here.
    THashMap<TChunkId, TQueueNode> Chunks_;
    TIntrusiveList<TQueueNode> Queue_;
    TIntrusiveList<TQueueNode>::iterator NextChunkToExtract_ = Queue_.begin();
};

class TScraperTask
    : public TRefCounted
{
public:
    TScraperTask(
        TChunkScraperConfigPtr config,
        IInvokerPtr serializedInvoker,
        IInvokerPtr heavyInvoker,
        IThroughputThrottlerPtr throttler,
        IChannelPtr masterChannel,
        TNodeDirectoryPtr nodeDirectory,
        TCellTag cellTag,
        TChunkBatchLocatedHandler onChunkBatchLocated,
        TChunkScraperAvailabilityPolicy availabilityPolicy,
        TLogger logger)
    : Config_(std::move(config))
    , Throttler_(std::move(throttler))
    , NodeDirectory_(std::move(nodeDirectory))
    , CellTag_(cellTag)
    , OnChunkBatchLocated_(std::move(onChunkBatchLocated))
    , SerializedInvoker_(std::move(serializedInvoker))
    , HeavyInvoker_(std::move(heavyInvoker))
    , AvailabilityPolicy_(availabilityPolicy)
    , Logger(std::move(logger).WithTag("ScraperTaskId: %v, CellTag: %v",
        TGuid::Create(),
        CellTag_))
    , Looper_(New<TAsyncLooper>(
        SerializedInvoker_,
        // There should be called BIND_NO_PROPAGATE, but currently we store allocation tags in trace context :(
        /*asyncStart*/ BIND([weakThis = MakeWeak(this)] () {
            if (auto strongThis = weakThis.Lock()) {
                return strongThis->LocateChunksAsync();
            }
            // Break loop.
            return TFuture<void>();
        }),
        /*syncFinish*/ BIND(&TScraperTask::LocateChunksSync, MakeWeak(this)),
        Logger.WithTag("AsyncLooper: %v", "ScraperTask")))
    , Proxy_(std::move(masterChannel))
    , ChunkIdsQueue_(std::make_unique<TRoundRobinQueue>())
    { }

    ~TScraperTask() override
    {
        Stop();
    }

    //! Starts periodic polling.
    void Start()
    {
        YT_LOG_DEBUG(
            "Starting scraper task (ChunkCount: %v)",
            ChunkIdsQueue_->GetSize());

        Looper_->Start();
    }

    //! Stops periodic polling.
    void Stop()
    {
        YT_LOG_DEBUG(
            "Stopping scraper task (ChunkCount: %v)",
            ChunkIdsQueue_->GetSize());

        Looper_->Stop();
    }

    void Add(TChunkId chunkId)
    {
        ChunkIdsQueue_->Add(chunkId);
    }

    void Remove(TChunkId chunkId)
    {
        ChunkIdsQueue_->Remove(chunkId);
        if (ChunkIdsQueue_->GetSize() == 0) {
            Stop();
        }
    }

    void OnChunkBecameUnavailable(TChunkId chunkId)
    {
        ChunkIdsQueue_->OnBecameUnavailable(chunkId);
    }

private:
    const TChunkScraperConfigPtr Config_;
    const IThroughputThrottlerPtr Throttler_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TCellTag CellTag_;
    const TChunkBatchLocatedHandler OnChunkBatchLocated_;
    const IInvokerPtr SerializedInvoker_;
    const IInvokerPtr HeavyInvoker_;
    const TChunkScraperAvailabilityPolicy AvailabilityPolicy_;
    const TLogger Logger;
    const TAsyncLooperPtr Looper_;
    const TChunkServiceProxy Proxy_;
    const std::unique_ptr<IChunkScraperQueue> ChunkIdsQueue_;

    std::optional<int> ThrottledBatchSize_;

    TFuture<void> LocateChunksAsync()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        ThrottledBatchSize_ = std::min<i64>(ChunkIdsQueue_->GetSize(), Config_->MaxChunksPerRequest);
        return Throttler_->Throttle(*ThrottledBatchSize_);
    }

    void LocateChunksSync()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(ThrottledBatchSize_.has_value());

        const auto batch = ChunkIdsQueue_->ExtractBatch(*ThrottledBatchSize_);
        ThrottledBatchSize_.reset();

        auto sampleChunkIds = MakeShrunkFormattableView(batch, TDefaultFormatter(), /*limit*/ 15);
        YT_LOG_DEBUG(
            "Locating chunks (Count: %v, SampleChunkIds: %v)",
            batch.size(),
            sampleChunkIds);

        auto rspOrError = WaitFor(MakeRequest(batch)->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to locate chunks");
            return;
        }

        auto& rsp = rspOrError.Value();
        YT_VERIFY(std::ssize(batch) == rsp->subresponses_size());

        std::vector<TScrapedChunkInfo> chunkInfos;
        chunkInfos.reserve(batch.size());

        int missingCount = 0;
        for (auto [index, chunkId] : SEnumerate(batch)) {
            const auto& subresponse = rsp->subresponses(index);
            auto chunkInfo = MakeScrapedChunkInfo(chunkId, subresponse);
            UpdateQueue(chunkInfo);

            if (chunkInfo.Availability == EChunkAvailability::Missing) {
                ++missingCount;
            }

            chunkInfos.push_back(std::move(chunkInfo));
        }

        WaitFor(BIND([this, rsp = std::move(rsp)] { NodeDirectory_->MergeFrom(rsp->node_directory()); })
            .AsyncVia(HeavyInvoker_)
            .Run()
            .ToImmediatelyCancelable())
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Chunks located (Count: %v, MissingCount: %v, SampleChunkIds: %v)",
            batch.size(),
            missingCount,
            sampleChunkIds);

        OnChunkBatchLocated_(std::move(chunkInfos));
    }

    TChunkServiceProxy::TReqLocateChunksPtr MakeRequest(const std::vector<TChunkId>& chunkIds)
    {
        auto req = Proxy_.LocateChunks();
        req->SetRequestHeavy(true);
        req->SetResponseHeavy(true);
        ToProto(req->mutable_subrequests(), chunkIds);
        return req;

    }

    TScrapedChunkInfo MakeScrapedChunkInfo(TChunkId chunkId, const NYT::NChunkClient::NProto::TRspLocateChunks::TSubresponse& rsp)
    {
        if (rsp.missing()) {
            return {
                .ChunkId = chunkId,
                .Availability = EChunkAvailability::Missing,
            };
        }

        auto replicas = FromProto<TChunkReplicaList>(rsp.replicas());
        bool isUnavailable = Visit(
            AvailabilityPolicy_,
            [&] (EChunkAvailabilityPolicy policy) -> bool {
                YT_VERIFY(rsp.has_erasure_codec());
                return IsUnavailable(
                    replicas,
                    FromProto<NErasure::ECodec>(rsp.erasure_codec()),
                    policy);
            },
            [&] (TMetadataAvailablePolicy) { return replicas.empty(); });

        return {
            .ChunkId = chunkId,
            .Replicas = std::move(replicas),
            .Availability = isUnavailable
                ? EChunkAvailability::Unavailable
                : EChunkAvailability::Available,
        };
    }

    void UpdateQueue(const TScrapedChunkInfo& info) {
        auto chunkId = info.ChunkId;
        switch (info.Availability) {
            case EChunkAvailability::Available:
                ChunkIdsQueue_->OnLocatedAvailable(chunkId);
                break;

            case EChunkAvailability::Unavailable:
                ChunkIdsQueue_->OnLocatedUnavailable(chunkId);
                break;

            case EChunkAvailability::Missing:
                ChunkIdsQueue_->OnLocatedMissing(chunkId);
                break;
        }
    }
};

DECLARE_REFCOUNTED_TYPE(TScraperTask);
DEFINE_REFCOUNTED_TYPE(TScraperTask);

////////////////////////////////////////////////////////////////////////////////

}  // namespace

struct TChunkScraper::TScraperTaskWrapper
{
    TScraperTaskPtr Impl;
};

////////////////////////////////////////////////////////////////////////////////

TChunkScraper::TChunkScraper(
    TChunkScraperConfigPtr config,
    IInvokerPtr serializedInvoker,
    IInvokerPtr heavyInvoker_,
    TThrottlerManagerPtr throttlerManager,
    NApi::NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TChunkBatchLocatedHandler onChunkBatchLocated,
    TChunkScraperAvailabilityPolicy availabilityPolicy,
    TLogger logger)
    : Config_(std::move(config))
    , SerializedInvoker_(std::move(serializedInvoker))
    , HeavyInvoker_(std::move(heavyInvoker_))
    , ThrottlerManager_(std::move(throttlerManager))
    , Client_(std::move(client))
    , NodeDirectory_(std::move(nodeDirectory))
    , OnChunkBatchLocated_(std::move(onChunkBatchLocated))
    , AvailabilityPolicy_(availabilityPolicy)
    , Logger(std::move(logger))
{ }

TChunkScraper::~TChunkScraper()
{
    Stop();
}

void TChunkScraper::Start()
{
    if (!std::exchange(IsStarted_, true)) {
        for (const auto& [_, task] : ScraperTasks_) {
            task.Impl->Start();
        }
    }
}

void TChunkScraper::Stop()
{
    if (std::exchange(IsStarted_, false)) {
        for (const auto& [_, task] : ScraperTasks_) {
            task.Impl->Stop();
        }
    }
}

void TChunkScraper::Add(TChunkId chunkId)
{
    auto cellTag = CellTagFromId(chunkId);

    auto& task = ScraperTasks_[cellTag].Impl;
    if (!task) {
        auto throttler = ThrottlerManager_->GetThrottler(cellTag);
        auto masterChannel = Client_->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower, cellTag);
        task = New<TScraperTask>(
            Config_,
            SerializedInvoker_,
            HeavyInvoker_,
            std::move(throttler),
            std::move(masterChannel),
            NodeDirectory_,
            cellTag,
            OnChunkBatchLocated_,
            AvailabilityPolicy_,
            Logger);
    }

    task->Add(chunkId);

    if (IsStarted_) {
        task->Start();
    }
}

void TChunkScraper::Remove(TChunkId chunkId)
{
    auto cellTag = CellTagFromId(chunkId);

    if (auto it = ScraperTasks_.find(cellTag); it != ScraperTasks_.end()) {
        it->second.Impl->Remove(chunkId);
    }
}

void TChunkScraper::OnChunkBecameUnavailable(TChunkId chunkId)
{
    auto cellTag = CellTagFromId(chunkId);
    if (auto it = ScraperTasks_.find(cellTag); it != ScraperTasks_.end()) {
        it->second.Impl->OnChunkBecameUnavailable(chunkId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
