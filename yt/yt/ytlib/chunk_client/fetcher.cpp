#include "fetcher.h"
#include "private.h"
#include "input_chunk.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NObjectClient;
using namespace NApi;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TFetcherChunkScraper
    : public IFetcherChunkScraper
{
public:
    TFetcherChunkScraper(
        const TChunkScraperConfigPtr config,
        const IInvokerPtr invoker,
        TThrottlerManagerPtr throttlerManager,
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const NLogging::TLogger& logger)
        : Config_(config)
        , Invoker_(CreateSerializedInvoker(invoker))
        , ThrottlerManager_(throttlerManager)
        , Client_(client)
        , NodeDirectory_(nodeDirectory)
        , Logger(logger.WithTag("FetcherChunkScraperId: %v", TGuid::Create()))
    { }

    TFuture<void> ScrapeChunks(const THashSet<TInputChunkPtr>& chunkSpecs) override
    {
        return BIND(&TFetcherChunkScraper::DoScrapeChunks, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(chunkSpecs);
    }

    i64 GetUnavailableChunkCount() const override
    {
        return UnavailableFetcherChunkCount_;
    }

private:
    struct TFetcherChunkDescriptor
    {
        TCompactVector<NChunkClient::TInputChunkPtr, 1> ChunkSpecs;
        bool IsWaiting = true;
    };

    const TChunkScraperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TThrottlerManagerPtr ThrottlerManager_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const NLogging::TLogger Logger;

    TChunkScraperPtr Scraper_;

    THashMap<TChunkId, TFetcherChunkDescriptor> ChunkMap_;
    int UnavailableFetcherChunkCount_ = 0;
    TPromise<void> BatchLocatedPromise_ = NewPromise<void>();

    int ChunkLocatedCallCount_ = 0;

    TFuture<void> DoScrapeChunks(const THashSet<TInputChunkPtr>& chunkSpecs)
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        THashSet<TChunkId> chunkIds;
        ChunkMap_.clear();
        for (const auto& chunkSpec : chunkSpecs) {
            auto chunkId = chunkSpec->GetChunkId();
            chunkIds.insert(chunkId);
            ChunkMap_[chunkId].ChunkSpecs.push_back(chunkSpec);
        }
        UnavailableFetcherChunkCount_ = chunkIds.size();

        Scraper_ = New<TChunkScraper>(
            Config_,
            Invoker_,
            ThrottlerManager_,
            Client_,
            NodeDirectory_,
            std::move(chunkIds),
            BIND(&TFetcherChunkScraper::OnChunkLocated, MakeWeak(this)),
            Logger);
        Scraper_->Start();

        BatchLocatedPromise_ = NewPromise<void>();
        BatchLocatedPromise_.OnCanceled(BIND(&TFetcherChunkScraper::OnCanceled, MakeWeak(this)));
        return BatchLocatedPromise_;
    }

    void OnCanceled(const TError& error)
    {
        YT_LOG_DEBUG(error, "Fetcher chunk scraper canceled");
        YT_UNUSED_FUTURE(Scraper_->Stop());
    }

    void OnChunkLocated(TChunkId chunkId, const TChunkReplicaWithMediumList& replicas, bool missing)
    {
        ++ChunkLocatedCallCount_;
        if (ChunkLocatedCallCount_ >= Config_->MaxChunksPerRequest) {
            ChunkLocatedCallCount_ = 0;
            YT_LOG_DEBUG("Located another batch of chunks (Count: %v, UnavailableFetcherChunkCount: %v)",
                Config_->MaxChunksPerRequest,
                UnavailableFetcherChunkCount_);
        }

        YT_LOG_TRACE("Fetcher chunk is located (ChunkId: %v, Replicas: %v, Missing: %v)",
            chunkId,
            replicas,
            missing);

        if (missing) {
            YT_LOG_DEBUG("Chunk being scraped is missing; scraper terminated (ChunkId: %v)", chunkId);
            auto asyncError = Scraper_->Stop()
                .Apply(BIND([=] () {
                    THROW_ERROR_EXCEPTION("Chunk scraper failed: chunk %v is missing", chunkId);
                }));

            BatchLocatedPromise_.TrySetFrom(asyncError);
            return;
        }

        if (replicas.empty()) {
            return;
        }

        auto& description = GetOrCrash(ChunkMap_, chunkId);
        YT_VERIFY(!description.ChunkSpecs.empty());

        if (!std::exchange(description.IsWaiting, false)) {
            return;
        }

        YT_LOG_TRACE("Fetcher chunk is available (ChunkId: %v, Replicas: %v)",
            chunkId,
            replicas);

        // Update replicas in place for all input chunks with current chunkId.
        for (const auto& chunkSpec : description.ChunkSpecs) {
            chunkSpec->SetReplicaList(replicas);
        }

        --UnavailableFetcherChunkCount_;
        YT_VERIFY(UnavailableFetcherChunkCount_ >= 0);

        if (UnavailableFetcherChunkCount_ == 0) {
            // Wait for all scraper callbacks to finish before session completion.
            BatchLocatedPromise_.TrySetFrom(Scraper_->Stop());
            YT_LOG_DEBUG("All fetcher chunks are available");
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TFetcherChunkScraper)

////////////////////////////////////////////////////////////////////////////////

IFetcherChunkScraperPtr CreateFetcherChunkScraper(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    TThrottlerManagerPtr throttlerManager,
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    const NLogging::TLogger& logger)
{
    return New<TFetcherChunkScraper>(
        config,
        invoker,
        throttlerManager,
        client,
        nodeDirectory,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

TFetcherBase::TFetcherBase(
    TFetcherConfigPtr config,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IFetcherChunkScraperPtr chunkScraper,
    NNative::IClientPtr client,
    const NLogging::TLogger& logger)
    : Config_(std::move(config))
    , NodeDirectory_(std::move(nodeDirectory))
    , Invoker_(CreateSerializedInvoker(invoker))
    , ChunkScraper_(std::move(chunkScraper))
    , Logger(logger)
    , Client_(std::move(client))
{ }

void TFetcherBase::AddChunk(TInputChunkPtr chunk)
{
    Chunks_.push_back(chunk);
}

int TFetcherBase::GetChunkCount() const
{
    return Chunks_.size();
}

TFuture<void> TFetcherBase::Fetch()
{
    OnFetchingStarted();

    for (int chunkIndex = 0; chunkIndex < std::ssize(Chunks_); ++chunkIndex) {
        const auto& chunk = Chunks_[chunkIndex];
        if (chunk->IsDynamicStore()) {
            ProcessDynamicStore(chunkIndex);
        } else {
            UnfetchedChunkIndexes_.insert(chunkIndex);
        }
    }

    THashSet<TNodeId> nodeIds;
    for (int unfetchedChunkIndex : UnfetchedChunkIndexes_) {
        for (auto replica : Chunks_[unfetchedChunkIndex]->GetReplicaList()) {
            nodeIds.insert(replica.GetNodeId());
        }
    }

    std::vector<TFuture<const TNodeDescriptor*>> asyncNodeDescriptors;
    asyncNodeDescriptors.reserve(nodeIds.size());
    for (auto id : nodeIds) {
        asyncNodeDescriptors.push_back(NodeDirectory_->GetAsyncDescriptor(id));
    }

    ActiveTaskFuture_.Store(
        AllSucceeded(std::move(asyncNodeDescriptors))
            .AsVoid()
            .WithTimeout(Config_->NodeDirectorySynchronizationTimeout)
            .Apply(BIND(&TFetcherBase::StartFetchingRound, MakeWeak(this))
                .AsyncVia(Invoker_)));
    auto future = Promise_.ToFuture();
    if (CancelableContext_) {
        future = future.ToImmediatelyCancelable();
        CancelableContext_->PropagateTo(future);
    }
    Promise_.OnCanceled(BIND(&TFetcherBase::OnCanceled, MakeWeak(this)));
    return future;
}

void TFetcherBase::SetCancelableContext(TCancelableContextPtr cancelableContext)
{
    CancelableContext_ = std::move(cancelableContext);
}

void TFetcherBase::PerformFetchingRoundFromNode(NNodeTrackerClient::TNodeId nodeId)
{
    const auto& nodeAddress = NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress();
    YT_LOG_DEBUG(
        "Starting node fetching round (NodeId: %v, NodeAddress: %v, ChunkCount: %v)",
        nodeId,
        nodeAddress,
        NodeToChunkIndexesToFetch_[nodeId].size());
    for (auto& chunkIndexesToFetch = NodeToChunkIndexesToFetch_[nodeId]; !chunkIndexesToFetch.empty();) {
        auto oldChunkIndexesToFetchSize = chunkIndexesToFetch.size();

        auto lastIt = std::next(
            chunkIndexesToFetch.begin(),
            static_cast<i64>(std::min(static_cast<ui64>(Config_->MaxChunksPerNodeFetch), chunkIndexesToFetch.size())));
        std::vector<int> chunkIds(chunkIndexesToFetch.begin(), lastIt);
        chunkIndexesToFetch.erase(chunkIndexesToFetch.begin(), lastIt);
        // Fetch another portion of chunks from node.
        YT_LOG_DEBUG(
            "Fetching from node (NodeId: %v, NodeAddress: %v, ChunkCount: %v)",
            nodeId,
            nodeAddress,
            chunkIds.size());
        try {
            WaitFor(FetchFromNode(nodeId, std::move(chunkIds)))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Stopping node fetching round due to an error");
            throw;
        }

        if (DeadNodes_.contains(nodeId)) {
            break;
        }

        if (chunkIndexesToFetch.size() == oldChunkIndexesToFetchSize) {
            YT_LOG_DEBUG(
                "Fetching iteration did not make any progress, considering node dead (Address: %v)",
                NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress());
            DeadNodes_.insert(nodeId);
            // All unfetched chunks will be put back into the common pool after this.
            break;
        }
    }

    YT_LOG_DEBUG(
        "Node fetching round completed (NodeId: %v, NodeAddress: %v, UnfetchedChunkCount: %v)",
        nodeId,
        nodeAddress,
        NodeToChunkIndexesToFetch_[nodeId].size());

    UnfetchedChunkIndexes_.insert(
        NodeToChunkIndexesToFetch_[nodeId].begin(),
        NodeToChunkIndexesToFetch_[nodeId].end());
    NodeToChunkIndexesToFetch_[nodeId].clear();
}

void TFetcherBase::StartFetchingRound(const TError& preparationError)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(Invoker_);

    if (!preparationError.IsOK()) {
        YT_LOG_ERROR(preparationError, "Fetching preparation failed, abort fetching round");
        Promise_.TrySet(preparationError);
        return;
    }

    if (Promise_.IsCanceled()) {
        YT_LOG_DEBUG("Fetcher canceled, abort fetching round");
        return;
    }

    YT_LOG_DEBUG("Start fetching round (UnfetchedChunkCount: %v, DeadNodes: %v, DeadChunks: %v)",
        UnfetchedChunkIndexes_.size(),
        DeadNodes_.size(),
        DeadChunks_.size());

    // Unban nodes with expired ban duration.
    auto now = TInstant::Now();
    while (!BannedNodes_.empty() && BannedNodes_.begin()->first <= now) {
        auto nodeId = BannedNodes_.begin()->second;
        YT_LOG_DEBUG("Unban node (Address: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            now);

        YT_VERIFY(UnbanTime_.erase(nodeId) == 1);
        BannedNodes_.erase(BannedNodes_.begin());
    }

    // Construct address -> chunk* map.
    typedef THashMap<TNodeId, std::vector<int>> TNodeIdToChunkIndexes;
    TNodeIdToChunkIndexes nodeIdToChunkIndexes;
    THashSet<TInputChunkPtr> unavailableChunks;

    for (auto chunkIndex : UnfetchedChunkIndexes_) {
        const auto& chunk = Chunks_[chunkIndex];
        auto chunkId = chunk->GetChunkId();
        bool chunkAvailable = false;
        const auto replicas = chunk->GetReplicaList();
        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            if (!DeadNodes_.contains(nodeId) &&
                DeadChunks_.find(std::make_pair(nodeId, chunkId)) == DeadChunks_.end())
            {
                if (!UnbanTime_.contains(nodeId)) {
                    nodeIdToChunkIndexes[nodeId].push_back(chunkIndex);
                }
                chunkAvailable = true;
            }
        }
        if (!chunkAvailable) {
            if (ChunkScraper_) {
                unavailableChunks.insert(chunk);
            } else {
                Promise_.Set(TError(
                    EErrorCode::ChunkUnavailable,
                    "Unable to fetch info for chunk %v from any of nodes %v",
                    chunkId,
                    MakeFormattableView(replicas, TChunkReplicaAddressFormatter(NodeDirectory_))));
                return;
            }
        }
    }

    if (!unavailableChunks.empty()) {
        YT_VERIFY(ChunkScraper_);
        YT_LOG_DEBUG("Found unavailable chunks, starting scraper (UnavailableChunkCount: %v)",
            unavailableChunks.size());
        auto error = WaitFor(ChunkScraper_->ScrapeChunks(std::move(unavailableChunks)));
        YT_LOG_DEBUG("All unavailable chunks are located");
        DeadNodes_.clear();
        DeadChunks_.clear();
        Invoker_->Invoke(
            BIND(&TFetcherBase::OnFetchingRoundCompleted, MakeWeak(this), /*backoff*/ false, error));
        return;
    }

    UnfetchedChunkIndexes_.clear();

    // Sort nodes by number of chunks (in decreasing order).
    std::vector<TNodeIdToChunkIndexes::iterator> nodeIts;
    for (auto it = nodeIdToChunkIndexes.begin(); it != nodeIdToChunkIndexes.end(); ++it) {
        nodeIts.push_back(it);
    }
    std::sort(
        nodeIts.begin(),
        nodeIts.end(),
        [=] (const TNodeIdToChunkIndexes::iterator& lhs, const TNodeIdToChunkIndexes::iterator& rhs) {
            return lhs->second.size() > rhs->second.size();
        });

    // Pick nodes greedily.
    std::vector<TFuture<void>> asyncResults;
    THashSet<int> requestedChunkIndexes;
    for (const auto& it : nodeIts) {
        for (int chunkIndex : it->second) {
            if (requestedChunkIndexes.find(chunkIndex) == requestedChunkIndexes.end()) {
                YT_VERIFY(requestedChunkIndexes.insert(chunkIndex).second);
                NodeToChunkIndexesToFetch_[it->first].insert(chunkIndex);
            }
        }

        if (NodeToChunkIndexesToFetch_[it->first].empty()) {
            continue;
        }

        asyncResults.push_back(BIND(&TFetcherBase::PerformFetchingRoundFromNode, MakeWeak(this), it->first)
            .AsyncVia(Invoker_)
            .Run());
    }

    bool backoff = asyncResults.empty();

    auto future = AllSucceeded(asyncResults);
    future.Subscribe(
        BIND(&TFetcherBase::OnFetchingRoundCompleted, MakeWeak(this), backoff)
            .Via(Invoker_));
    ActiveTaskFuture_.Store(std::move(future));
}

IChannelPtr TFetcherBase::GetNodeChannel(TNodeId nodeId)
{
    const auto& descriptor = NodeDirectory_->GetDescriptor(nodeId);
    return Client_->GetChannelFactory()->CreateChannel(descriptor);
}

void TFetcherBase::OnChunkFailed(TNodeId nodeId, int chunkIndex, const TError& error)
{
    const auto& chunk = Chunks_[chunkIndex];
    auto chunkId = chunk->GetChunkId();
    const auto& address = NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress();

    if (error.FindMatching(NYT::EErrorCode::Timeout)) {
        YT_LOG_DEBUG(error, "Timed out fetching chunk info (ChunkId: %v, Address: %v)", chunkId, address);
        YT_VERIFY(NodeToChunkIndexesToFetch_[nodeId].insert(chunkIndex).second);
    } else {
        YT_LOG_DEBUG(error, "Error fetching chunk info (ChunkId: %v, Address: %v)", chunkId, address);
        DeadChunks_.emplace(nodeId, chunkId);
        YT_VERIFY(UnfetchedChunkIndexes_.insert(chunkIndex).second);
    }
}

void TFetcherBase::OnNodeFailed(TNodeId nodeId, const std::vector<int>& chunkIndexes)
{
    YT_LOG_DEBUG("Error fetching chunks from node (Address: %v, ChunkCount: %v)",
        NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
        chunkIndexes.size());

    DeadNodes_.insert(nodeId);
    UnfetchedChunkIndexes_.insert(chunkIndexes.begin(), chunkIndexes.end());
}

void TFetcherBase::OnRequestThrottled(TNodeId nodeId, const std::vector<int>& chunkIndexes)
{
    auto nodeAddress = NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress();
    YT_LOG_DEBUG("Fetch request throttled by node (Address: %v, ChunkCount: %v)",
        nodeAddress,
        chunkIndexes.size());

    auto unbanTime = TInstant::Zero();
    if (UnbanTime_.contains(nodeId)) {
        unbanTime = UnbanTime_[nodeId];
        YT_VERIFY(BannedNodes_.erase(std::make_pair(unbanTime, nodeId)) == 1);
        YT_VERIFY(UnbanTime_.erase(nodeId) == 1);
    }

    unbanTime = std::max(unbanTime, TInstant::Now() + Config_->NodeBanDuration);

    YT_LOG_DEBUG("Node banned (Address: %v, UnbanTime: %v)",
        nodeAddress,
        unbanTime);

    YT_VERIFY(BannedNodes_.emplace(unbanTime, nodeId).second);
    YT_VERIFY(UnbanTime_.emplace(nodeId, unbanTime).second);

    UnfetchedChunkIndexes_.insert(chunkIndexes.begin(), chunkIndexes.end());
}

void TFetcherBase::OnFetchingRoundCompleted(bool backoff, const TError& error)
{
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Fetching failed");
        Promise_.Set(error);
        return;
    }

    if (UnfetchedChunkIndexes_.empty()) {
        YT_LOG_DEBUG("Fetching complete");
        OnFetchingCompleted();
        Promise_.Set(TError());
        return;
    }

    YT_LOG_DEBUG("Fetching round completed (UnfetchedChunkCount: %v)", UnfetchedChunkIndexes_.size());

    if (backoff) {
        TDelayedExecutor::WaitForDuration(Config_->BackoffTime);
    }

    ActiveTaskFuture_.Store(BIND(&TFetcherBase::StartFetchingRound, MakeWeak(this))
        .AsyncVia(Invoker_)
        .Run(TError{}));
}

void TFetcherBase::OnFetchingStarted()
{ }

void TFetcherBase::OnFetchingCompleted()
{ }

void TFetcherBase::OnCanceled(const TError& error)
{
    YT_LOG_DEBUG(error, "Fetcher canceled");
    if (auto future = ActiveTaskFuture_.Load()) {
        future.Cancel(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
