#include "stdafx.h"
#include "fetcher_base.h"
#include "chunk_replica.h"
#include "chunk_spec.h"
#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_scraper.h>
#include <ytlib/node_tracker_client/node_directory.h>

#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>

#include <core/rpc/retrying_channel.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TScrapeChunksSession
    : public TRefCounted
{
public:
    TScrapeChunksSession(
        const TChunkScraperConfigPtr config,
        const IInvokerPtr invoker,
        const NConcurrency::IThroughputThrottlerPtr throttler,
        NRpc::IChannelPtr masterChannel,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const NLogging::TLogger& logger = ChunkClientLogger)
        : Scraper_(
            New<TChunkScraper>(
                config,
                invoker,
                throttler,
                masterChannel,
                nodeDirectory,
                yhash_set<TChunkId>(),
                BIND(&TScrapeChunksSession::OnChunkLocated, MakeWeak(this)),
                logger))
        , Logger(logger)
    { }

    TFuture<void> ScrapeChunks(
        yhash_set<TRefCountedChunkSpecPtr> chunkSpecs)
    {
        yhash_set<TChunkId> chunkIds;
        for (const auto& chunkSpec : chunkSpecs) {
            auto chunkId = NYT::FromProto<TChunkId>(chunkSpec->chunk_id());
            chunkIds.insert(chunkId);
            ChunkMap_[chunkId].ChunkSpecs.push_back(chunkSpec);
        }
        UnavailableFetcherChunkCount_ = chunkIds.size();
        Scraper_->Reset(std::move(chunkIds));
        BatchLocatedPromise_ = NewPromise<void>();
        return BatchLocatedPromise_;
    }

private:
    void OnChunkLocated(const TChunkId& chunkId, const TChunkReplicaList& replicas)
    {
        LOG_TRACE("Fetcher chunk is located (ChunkId: %v, Replicas: %v)", chunkId, replicas.size());
        if (replicas.empty())
            return;

        auto it = ChunkMap_.find(chunkId);
        YCHECK(it != ChunkMap_.end());

        auto& description = it->second;
        YCHECK(!description.ChunkSpecs.empty());

        if (!description.IsWaiting)
            return;

        description.IsWaiting = false;

        LOG_TRACE("Fetcher chunk is available (ChunkId: %v, Replicas: %v)", chunkId, replicas.size());

        // Update replicas in place for all input chunks with current chunkId.
        for (auto& chunkSpec : description.ChunkSpecs) {
            chunkSpec->mutable_replicas()->Clear();
            ToProto(chunkSpec->mutable_replicas(), replicas);
        }

        --UnavailableFetcherChunkCount_;
        YCHECK(UnavailableFetcherChunkCount_ >= 0);

        if (UnavailableFetcherChunkCount_ == 0) {
            Scraper_->Stop();
            BatchLocatedPromise_.Set();
        }
    }

    struct TFetcherChunkDescriptor
    {
        SmallVector<NChunkClient::TRefCountedChunkSpecPtr, 1> ChunkSpecs;
        bool IsWaiting = true;
    };

    TChunkScraperPtr Scraper_;
    NLogging::TLogger Logger;

    yhash_map<NChunkClient::TChunkId,TFetcherChunkDescriptor> ChunkMap_;
    int UnavailableFetcherChunkCount_ = 0;
    TPromise<void> BatchLocatedPromise_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TScrapeChunksSession)

////////////////////////////////////////////////////////////////////////////////

TScrapeChunksCallback CreateScrapeChunksSessionCallback(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    const NConcurrency::IThroughputThrottlerPtr throttler,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NLogging::TLogger& logger)
{
    auto scrapeChunksSession = New<TScrapeChunksSession>(
        config,
        invoker,
        throttler,
        masterChannel,
        nodeDirectory,
        logger);
    return BIND(&TScrapeChunksSession::ScrapeChunks, scrapeChunksSession)
        .AsyncVia(invoker);
}

////////////////////////////////////////////////////////////////////////////////

TFetcherBase::TFetcherBase(
    TFetcherConfigPtr config,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TScrapeChunksCallback scraperCallback,
    const NLogging::TLogger& logger)
    : Config_(config)
    , NodeDirectory_(nodeDirectory)
    , Invoker_(invoker)
    , ScraperCallback_(scraperCallback)
    , Logger(logger)
{ }

void TFetcherBase::AddChunk(TRefCountedChunkSpecPtr chunk)
{
    YCHECK(UnfetchedChunkIndexes_.insert(static_cast<int>(Chunks_.size())).second);
    Chunks_.push_back(chunk);
}

TFuture<void> TFetcherBase::Fetch()
{
    BIND(&TFetcherBase::StartFetchingRound, MakeWeak(this))
        .Via(Invoker_)
        .Run();
    return Promise_;
}

void TFetcherBase::StartFetchingRound()
{
    LOG_DEBUG("Start fetching round (UnfetchedChunkCount: %v, DeadNodes: %v, DeadChunks: %v)",
        UnfetchedChunkIndexes_.size(),
        DeadNodes_.size(),
        DeadChunks_.size());

    // Construct address -> chunk* map.
    typedef yhash_map<TNodeId, std::vector<int> > TNodeIdToChunkIndexes;
    TNodeIdToChunkIndexes nodeIdToChunkIndexes;
    yhash_set<TRefCountedChunkSpecPtr> unavailableChunks;

    for (auto chunkIndex : UnfetchedChunkIndexes_) {
        const auto& chunk = Chunks_[chunkIndex];
        auto chunkId = NYT::FromProto<TChunkId>(chunk->chunk_id());
        bool chunkAvailable = false;
        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunk->replicas());
        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            if (DeadNodes_.find(nodeId) == DeadNodes_.end() &&
                DeadChunks_.find(std::make_pair(nodeId, chunkId)) == DeadChunks_.end())
            {
                nodeIdToChunkIndexes[nodeId].push_back(chunkIndex);
                chunkAvailable = true;
            }
        }
        if (!chunkAvailable) {
            if (ScraperCallback_) {
                unavailableChunks.insert(chunk);
            } else {
                Promise_.Set(TError(
                    "Unable to fetch info for chunk %v from any of nodes [%v]",
                    chunkId,
                    JoinToString(replicas, TChunkReplicaAddressFormatter(NodeDirectory_))));
                return;
            }
        }
    }

    if (!unavailableChunks.empty() && ScraperCallback_) {
        LOG_DEBUG("Found unavailable chunks, starting scraper (UnavailableChunkCount: %v)",
            unavailableChunks.size());
        auto error = WaitFor(ScraperCallback_.Run(std::move(unavailableChunks)));
        LOG_DEBUG("All unavailable chunks are located");
        DeadNodes_.clear();
        DeadChunks_.clear();
        BIND(&TFetcherBase::OnFetchingRoundCompleted, MakeWeak(this))
            .Via(Invoker_)
            .Run(error);
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
    yhash_set<int> requestedChunkIndexes;
    for (const auto& it : nodeIts) {
        std::vector<int> chunkIndexes;
        for (int chunkIndex : it->second) {
            if (requestedChunkIndexes.find(chunkIndex) == requestedChunkIndexes.end()) {
                YCHECK(requestedChunkIndexes.insert(chunkIndex).second);
                chunkIndexes.push_back(chunkIndex);
            }
        }

        // Send the request, if not empty.
        if (!chunkIndexes.empty()) {
            asyncResults.push_back(FetchFromNode(it->first, std::move(chunkIndexes)));
        }
    }

    Combine(asyncResults).Subscribe(
        BIND(&TFetcherBase::OnFetchingRoundCompleted, MakeWeak(this))
            .Via(Invoker_));
}

IChannelPtr TFetcherBase::GetNodeChannel(TNodeId nodeId)
{
    const auto& descriptor = NodeDirectory_->GetDescriptor(nodeId);
    auto channel = LightNodeChannelFactory->CreateChannel(descriptor.GetInterconnectAddress());
    return CreateRetryingChannel(Config_->NodeChannel, channel);
}

void TFetcherBase::OnChunkFailed(TNodeId nodeId, int chunkIndex, const TError& error)
{
    const auto& chunk = Chunks_[chunkIndex];
    auto chunkId = NYT::FromProto<TChunkId>(chunk->chunk_id());

    LOG_DEBUG(error, "Error fetching chunk info (ChunkId: %v, Address: %v)",
        chunkId,
        NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress());

    DeadChunks_.insert(std::make_pair(nodeId, chunkId));
    YCHECK(UnfetchedChunkIndexes_.insert(chunkIndex).second);
}

void TFetcherBase::OnNodeFailed(TNodeId nodeId, const std::vector<int>& chunkIndexes)
{
    LOG_DEBUG("Error fetching chunks from node (Address: %v, ChunkCount: %v)",
        NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
        chunkIndexes.size());

    DeadNodes_.insert(nodeId);
    UnfetchedChunkIndexes_.insert(chunkIndexes.begin(), chunkIndexes.end());
}

void TFetcherBase::OnFetchingRoundCompleted(const TError& error)
{
    if (!error.IsOK()) {
        LOG_ERROR(error, "Fetching failed");
        Promise_.Set(error);
        return;
    }

    if (UnfetchedChunkIndexes_.empty()) {
        LOG_DEBUG("Fetching complete");
        Promise_.Set(TError());
        return;
    }

    StartFetchingRound();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
