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

#include <library/cpp/iterator/zip.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient::NProto;

using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;

using NYT::FromProto;

void FormatValue(TStringBuilderBase* builder, const TScrapedChunkInfo& info, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{ChunkId: %v, Replicas: %v, Availability: %v}",
        info.ChunkId,
        info.Replicas,
        info.Availability);
}

void FormatValue(TStringBuilderBase* builder, TChunkScraperAvailabilityPolicy policy, TStringBuf /*spec*/)
{
    Visit(
        policy,
        [&] (EChunkAvailabilityPolicy policy) {
            builder->AppendFormat("%v", policy);
        },
        [&] (TMetadataAvailablePolicy) {
            builder->AppendString("MetadataAvailable");
        });
}

namespace {

////////////////////////////////////////////////////////////////////////////////

struct IChunkScraperQueue
{
    virtual std::vector<TChunkId> ExtractBatch(int maxBatchSize) = 0;

    virtual void Add(TChunkId chunkId) = 0;

    /// Equivalent to `Add` with subsequent `MarkUnavailable`.
    virtual void AddUnavailable(TChunkId chunkId) = 0;

    virtual bool Remove(TChunkId chunkId) = 0;

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

    std::vector<TChunkId> ExtractBatch(int maxBatchSize) override
    {
        int batchSize = std::min<i64>(GetSize(), maxBatchSize);
        std::vector<TChunkId> result;
        result.reserve(batchSize);
        std::generate_n(
            std::back_inserter(result),
            batchSize,
            [this] { return ExtractNext(); });
        return result;
    }

    void Add(TChunkId chunkId) override
    {
        YT_VERIFY(chunkId != NullChunkId);

        auto [it, isNew] = Chunks_.emplace_unique(chunkId);
        if (isNew) {
            Queue_.PushBack(it->Node());
        }
    }

    void AddUnavailable(TChunkId chunkId) override
    {
        Add(chunkId);
    }

    bool Remove(TChunkId chunkId) override
    {
        YT_VERIFY(chunkId != NullChunkId);

        auto it = Chunks_.find(chunkId);
        if (it == Chunks_.end()) {
            return false;
        }
        auto node = it->Node();
        if (node == NextChunkToExtract_->Node()) {
            ++NextChunkToExtract_;
        }
        Queue_.Remove(node);
        Chunks_.erase(it);
        return true;
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

    i64 GetSize() const override
    {
        return Chunks_.size();
    }

    bool Contains(TChunkId chunkId) const
    {
        return Chunks_.find(chunkId) != Chunks_.end();
    }

private:
    struct TQueueNode
        : public TIntrusiveListItem<TQueueNode>
    {
        const TChunkId ChunkId;

        TQueueNode(TChunkId chunkId)
            : ChunkId(chunkId)
        { }
    };

    // TODO(namorniradnug): Implement intrusive hashmap and use it here.
    THashTable<
        TQueueNode,
        TChunkId,
        THash<TChunkId>,
        decltype([] (const TQueueNode& node) { return node.ChunkId; }),
        TEqualTo<TChunkId>,
        std::allocator<TQueueNode>> Chunks_;
    TIntrusiveList<TQueueNode> Queue_;
    TIntrusiveList<TQueueNode>::iterator NextChunkToExtract_ = Queue_.begin();

    TChunkId ExtractNext()
    {
        YT_VERIFY(GetSize() > 0);

        if (NextChunkToExtract_ == Queue_.end()) {
            NextChunkToExtract_ = Queue_.begin();
        }

        return NextChunkToExtract_++->ChunkId;
    }
};

class TUnavailableFirstQueue final
    : public IChunkScraperQueue
{
public:
    std::vector<TChunkId> ExtractBatch(int maxBatchSize) override
    {
        auto unavailableBatch = UnavailableQueue_.ExtractBatch(maxBatchSize);
        auto availableBatch = AvailableQueue_.ExtractBatch(maxBatchSize - unavailableBatch.size());
        return ConcatVectors(std::move(unavailableBatch), std::move(availableBatch));
    }

    void Add(TChunkId chunkId) override
    {
        if (!UnavailableQueue_.Contains(chunkId)) {
            AvailableQueue_.Add(chunkId);
        }
    }

    void AddUnavailable(TChunkId chunkId) override
    {
        AvailableQueue_.Remove(chunkId);
        UnavailableQueue_.Add(chunkId);
    }

    bool Remove(TChunkId chunkId) override
    {
        if (AvailableQueue_.Remove(chunkId)) {
            YT_VERIFY(!UnavailableQueue_.Contains(chunkId));
            return true;
        }
        return UnavailableQueue_.Remove(chunkId);
    }

    void OnLocatedAvailable(TChunkId chunkId) override
    {
        if (UnavailableQueue_.Remove(chunkId)) {
            AvailableQueue_.Add(chunkId);
        }
    }

    void OnLocatedUnavailable(TChunkId chunkId) override
    {
        if (AvailableQueue_.Remove(chunkId)) {
            UnavailableQueue_.Add(chunkId);
        }
    }

    void OnLocatedMissing(TChunkId chunkId) override
    {
        Remove(chunkId);
    }

    i64 GetSize() const override
    {
        return AvailableQueue_.GetSize() + UnavailableQueue_.GetSize();
    }

private:
    TRoundRobinQueue AvailableQueue_;
    TRoundRobinQueue UnavailableQueue_;
};

std::unique_ptr<IChunkScraperQueue> CreateQueue(bool prioritizeUnavailableChunks)
{
    if (prioritizeUnavailableChunks) {
        return std::make_unique<TUnavailableFirstQueue>();
    }
    return std::make_unique<TRoundRobinQueue>();
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace

class TChunkScraper::TScraperTask
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
    , ChunkIdsQueue_(CreateQueue(Config_->PrioritizeUnavailableChunks))
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

    void AddUnavailable(TChunkId chunkId)
    {
        ChunkIdsQueue_->AddUnavailable(chunkId);
    }

    void Remove(TChunkId chunkId)
    {
        ChunkIdsQueue_->Remove(chunkId);
    }

    void OnChunkBecameUnavailable(TChunkId chunkId)
    {
        ChunkIdsQueue_->OnLocatedUnavailable(chunkId);
    }

    i64 GetChunkCount() const
    {
        return ChunkIdsQueue_->GetSize();
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

        i64 queueSize = ChunkIdsQueue_->GetSize();
        if (queueSize == 0) {
            return {};
        }
        ThrottledBatchSize_ = std::min<i64>(queueSize, Config_->MaxChunksPerRequest);
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

        TEnumIndexedArray<EChunkAvailability, int> chunkAvailabilityCounters;
        std::vector<TScrapedChunkInfo> chunkInfos;
        chunkInfos.reserve(batch.size());
        for (auto [chunkId, subresponse] : Zip(batch, rsp->subresponses())) {
            const auto& chunkInfo = chunkInfos.emplace_back(MakeScrapedChunkInfo(chunkId, subresponse));
            UpdateQueue(chunkInfo);
            ++chunkAvailabilityCounters[chunkInfo.Availability];
        }

        WaitFor(
            BIND([rsp = std::move(rsp), nodeDirectoryWeak = TWeakPtr(NodeDirectory_)] {
                if (auto nodeDirectory = nodeDirectoryWeak.Lock()) {
                    nodeDirectory->MergeFrom(rsp->node_directory());
                }
            })
            .AsyncVia(HeavyInvoker_)
            .Run()
            .ToImmediatelyCancelable())
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Chunks located (Count: %v, AvailabilityStatistics: %v, SampleChunkIds: %v)",
            batch.size(),
            chunkAvailabilityCounters,
            sampleChunkIds);

        OnChunkBatchLocated_(std::move(chunkInfos));
    }

    TChunkServiceProxy::TReqLocateChunksPtr MakeRequest(const std::vector<TChunkId>& chunkIds) const
    {
        auto req = Proxy_.LocateChunks();
        req->SetRequestHeavy(true);
        req->SetResponseHeavy(true);
        ToProto(req->mutable_subrequests(), chunkIds);
        return req;
    }

    TScrapedChunkInfo MakeScrapedChunkInfo(TChunkId chunkId, const TRspLocateChunks::TSubresponse& rsp) const
    {
        if (rsp.missing()) {
            return {
                .ChunkId = chunkId,
                .Availability = EChunkAvailability::Missing,
            };
        }

        YT_VERIFY(rsp.has_erasure_codec());
        auto replicas = FromProto<TChunkReplicaList>(rsp.replicas());
        bool isUnavailable = IsUnavailable(replicas, FromProto<NErasure::ECodec>(rsp.erasure_codec()));

        return {
            .ChunkId = chunkId,
            .Replicas = std::move(replicas),
            .Availability = isUnavailable
                ? EChunkAvailability::Unavailable
                : EChunkAvailability::Available,
        };
    }

    bool IsUnavailable(const TChunkReplicaList& replicas, NErasure::ECodec codec) const
    {
        return Visit(
            AvailabilityPolicy_,
            [&] (EChunkAvailabilityPolicy policy) { return NChunkClient::IsUnavailable(replicas, codec, policy); },
            [&] (TMetadataAvailablePolicy) { return replicas.empty(); });
    }

    void UpdateQueue(const TScrapedChunkInfo& info)
    {
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

////////////////////////////////////////////////////////////////////////////////

TChunkScraper::TChunkScraper(
    TChunkScraperConfigPtr config,
    IInvokerPtr serializedInvoker,
    IInvokerPtr heavyInvoker,
    TThrottlerManagerPtr throttlerManager,
    NApi::NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TChunkBatchLocatedHandler onChunkBatchLocated,
    TChunkScraperAvailabilityPolicy availabilityPolicy,
    TLogger logger)
    : Config_(std::move(config))
    , SerializedInvoker_(std::move(serializedInvoker))
    , HeavyInvoker_(std::move(heavyInvoker))
    , ThrottlerManager_(std::move(throttlerManager))
    , Client_(std::move(client))
    , NodeDirectory_(std::move(nodeDirectory))
    , OnChunkBatchLocated_(std::move(onChunkBatchLocated))
    , AvailabilityPolicy_(availabilityPolicy)
    , Logger(std::move(logger))
{
    YT_LOG_INFO(
        "Chunk scraper initialized (MaxChunksPerRequest: %v, AvailabilityPolicy: %v, IsUnavailableFirstQueue: %v)",
        Config_->MaxChunksPerRequest,
        AvailabilityPolicy_,
        Config_->PrioritizeUnavailableChunks);
}

TChunkScraper::~TChunkScraper()
{
    Stop();
}

void TChunkScraper::Start()
{
    if (!std::exchange(IsStarted_, true)) {
        std::ranges::for_each(ScraperTasks_ | std::views::values, &TScraperTask::Start);
    }
}

void TChunkScraper::Stop()
{
    if (std::exchange(IsStarted_, false)) {
        std::ranges::for_each(ScraperTasks_ | std::views::values, &TScraperTask::Stop);
    }
}

TChunkScraper::TScraperTask& TChunkScraper::GetTaskForChunk(TChunkId chunkId)
{
    auto cellTag = CellTagFromId(chunkId);
    return *GetOrInsert(
        ScraperTasks_,
        cellTag,
        [&] {
            return New<TScraperTask>(
                Config_,
                SerializedInvoker_,
                HeavyInvoker_,
                ThrottlerManager_->GetThrottler(cellTag),
                Client_->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower, cellTag),
                NodeDirectory_,
                cellTag,
                OnChunkBatchLocated_,
                AvailabilityPolicy_,
                Logger);
        });
}

void TChunkScraper::Add(TChunkId chunkId)
{
    auto& task = GetTaskForChunk(chunkId);
    bool wasEmpty = task.GetChunkCount() == 0;
    task.Add(chunkId);
    if (wasEmpty && IsStarted_) {
        task.Start();
    }
}

void TChunkScraper::AddUnavailable(TChunkId chunkId)
{
    auto& task = GetTaskForChunk(chunkId);
    task.AddUnavailable(chunkId);
    if (IsStarted_) {
        task.Start();
    }
}

void TChunkScraper::Remove(TChunkId chunkId)
{
    if (auto it = ScraperTasks_.find(CellTagFromId(chunkId)); it != ScraperTasks_.end()) {
        it->second->Remove(chunkId);
    }
}

void TChunkScraper::OnChunkBecameUnavailable(TChunkId chunkId)
{
    if (auto it = ScraperTasks_.find(CellTagFromId(chunkId)); it != ScraperTasks_.end()) {
        it->second->OnChunkBecameUnavailable(chunkId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
