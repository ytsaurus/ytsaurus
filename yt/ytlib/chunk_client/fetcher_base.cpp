#include "stdafx.h"
#include "fetcher_base.h"
#include "chunk_replica.h"
#include "chunk_spec.h"
#include "config.h"
#include "private.h"

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

TFetcherBase::TFetcherBase(
    TFetcherConfigPtr config,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    const NLog::TLogger& logger)
    : Config_(config)
    , NodeDirectory_(nodeDirectory)
    , Invoker_(invoker)
    , Logger(logger)
{ }

void TFetcherBase::AddChunk(TRefCountedChunkSpecPtr chunk)
{
    YCHECK(UnfetchedChunkIndexes_.insert(static_cast<int>(Chunks_.size())).second);
    Chunks_.push_back(chunk);
}

TFuture<void> TFetcherBase::Fetch()
{
    StartFetchingRound();
    return Promise_;
}

void TFetcherBase::StartFetchingRound()
{
    LOG_DEBUG("Start fetching round (UnfetchedChunkCount: %v)",
        UnfetchedChunkIndexes_.size());

    // Construct address -> chunk* map.
    typedef yhash_map<TNodeId, std::vector<int> > TNodeIdToChunkIndexes;
    TNodeIdToChunkIndexes nodeIdToChunkIndexes;

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
            Promise_.Set(TError(
                "Unable to fetch info for chunk %v from any of nodes [%v]",
                chunkId,
                JoinToString(replicas, TChunkReplicaAddressFormatter(NodeDirectory_))));
            return;
        }
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
    auto channel = LightNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress());
    return CreateRetryingChannel(Config_->NodeChannel, channel);
}

void TFetcherBase::OnChunkFailed(TNodeId nodeId, int chunkIndex)
{
    const auto& chunk = Chunks_[chunkIndex];
    auto chunkId = NYT::FromProto<TChunkId>(chunk->chunk_id());

    YCHECK(DeadChunks_.insert(std::make_pair(nodeId, chunkId)).second);
    YCHECK(UnfetchedChunkIndexes_.insert(chunkIndex).second);
}

void TFetcherBase::OnNodeFailed(TNodeId nodeId, const std::vector<int>& chunkIndexes)
{
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
