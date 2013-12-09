#ifndef CHUNK_INFO_COLLECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_info_collector.h"
#endif
#undef CHUNK_INFO_COLLECTOR_INL_H_

#include <core/concurrency/parallel_awaiter.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TFetcher>
TChunkInfoCollector<TFetcher>::TChunkInfoCollector(
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    TFetcherPtr fetcher,
    IInvokerPtr invoker)
    : NodeDirectory(nodeDirectory)
    , Fetcher(fetcher)
    , Invoker(invoker)
    , Promise(NewPromise<TError>())
{ }

template <class TFetcher>
void TChunkInfoCollector<TFetcher>::AddChunk(
    NChunkClient::TRefCountedChunkSpecPtr chunk)
{
    YCHECK(UnfetchedChunkIndexes.insert(static_cast<int>(Chunks.size())).second);
    Chunks.push_back(chunk);
}

template <class TFetcher>
TAsyncError TChunkInfoCollector<TFetcher>::Run()
{
    Fetcher->Prepare(Chunks);
    SendRequests();
    return Promise;
}

template <class TFetcher>
void TChunkInfoCollector<TFetcher>::SendRequests()
{
    auto& Logger = Fetcher->GetLogger();

    // Construct address -> chunk* map.
    typedef yhash_map<NNodeTrackerClient::TNodeId, std::vector<int> > TNodeIdToChunkIndexes;
    TNodeIdToChunkIndexes nodeIdToChunkIndexes;

    for (auto chunkIndex : UnfetchedChunkIndexes) {
        const auto& chunk = Chunks[chunkIndex];
        auto chunkId = NYT::FromProto<NChunkClient::TChunkId>(chunk->chunk_id());
        bool chunkAvailable = false;
        auto replicas = FromProto<NChunkClient::TChunkReplica, NChunkClient::TChunkReplicaList>(chunk->replicas());
        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            if (DeadNodeIds.find(nodeId) == DeadNodeIds.end() &&
                DeadChunkIds.find(std::make_pair(nodeId, chunkId)) == DeadChunkIds.end())
            {
                nodeIdToChunkIndexes[nodeId].push_back(chunkIndex);
                chunkAvailable = true;
            }
        }
        if (!chunkAvailable) {
            Promise.Set(TError("Unable to fetch info for chunk %s from any of nodes [%s]",
                ~ToString(chunkId),
                ~JoinToString(replicas, NChunkClient::TChunkReplicaAddressFormatter(NodeDirectory))));
            return;
        }
    }

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
    auto awaiter = New<NConcurrency::TParallelAwaiter>(Invoker);
    yhash_set<int> requestedChunkIndexes;
    for (const auto& it : nodeIts) {
        auto nodeId = it->first;
        const auto& descriptor = NodeDirectory->GetDescriptor(nodeId);
        const auto& address = descriptor.Address;

        Fetcher->CreateNewRequest(descriptor);

        auto& addressChunkIndexes = it->second;
        std::vector<int> requestChunkIndexes;
        for (int chunkIndex : addressChunkIndexes) {
            if (requestedChunkIndexes.find(chunkIndex) == requestedChunkIndexes.end()) {
                YCHECK(requestedChunkIndexes.insert(chunkIndex).second);

                auto& chunk = Chunks[chunkIndex];
                if (Fetcher->AddChunkToRequest(nodeId, chunk)) {
                    requestChunkIndexes.push_back(chunkIndex);
                } else {
                    // We are not going to fetch info for this chunk.
                    YCHECK(UnfetchedChunkIndexes.erase(chunkIndex) == 1);
                }
            }
        }

        // Send the request, if not empty.
        if (!requestChunkIndexes.empty()) {
            LOG_DEBUG("Requesting chunk info for %d chunks from %s",
                static_cast<int>(requestChunkIndexes.size()),
                ~address);

            awaiter->Await(
                Fetcher->InvokeRequest(),
                BIND(
                    &TChunkInfoCollector<TFetcher>::OnResponse,
                    MakeStrong(this),
                    nodeId,
                    std::move(requestChunkIndexes)));
        }
    }
    awaiter->Complete(BIND(
        &TChunkInfoCollector<TFetcher>::OnEndRound,
        MakeStrong(this)));

    LOG_INFO("Done, %d requests sent", awaiter->GetRequestCount());
}

template <class TFetcher>
void TChunkInfoCollector<TFetcher>::OnResponse(
    NNodeTrackerClient::TNodeId nodeId,
    const std::vector<int>& chunkIndexes,
    typename TFetcher::TResponsePtr rsp)
{
    auto& Logger = Fetcher->GetLogger();
    const auto& descriptor = NodeDirectory->GetDescriptor(nodeId);
    const auto& address = descriptor.Address;

    if (rsp->IsOK()) {
        for (int index = 0; index < static_cast<int>(chunkIndexes.size()); ++index) {
            int chunkIndex = chunkIndexes[index];
            auto& chunk = Chunks[chunkIndex];
            auto chunkId = NYT::FromProto<NChunkClient::TChunkId>(chunk->chunk_id());

            auto result = Fetcher->ProcessResponseItem(rsp, index, chunk);
            if (result.IsOK()) {
                YCHECK(UnfetchedChunkIndexes.erase(chunkIndex) == 1);
            } else {
                LOG_WARNING(result, "Unable to fetch info for chunk %s from %s",
                    ~ToString(chunkId),
                    ~address);
                YCHECK(DeadChunkIds.insert(std::make_pair(nodeId, chunkId)).second);
            }
        }
        LOG_DEBUG("Received chunk info from %s", ~address);
    } else {
        LOG_WARNING(*rsp, "Error requesting chunk info from %s",
            ~address);
        YCHECK(DeadNodeIds.insert(nodeId).second);
    }
}

template <class TFetcher>
void TChunkInfoCollector<TFetcher>::OnEndRound()
{
    auto& Logger = Fetcher->GetLogger();

    if (UnfetchedChunkIndexes.empty()) {
        LOG_INFO("All info is fetched");
        Promise.Set(TError());
    } else {
        LOG_DEBUG("Chunk info for %d chunks is still unfetched",
            static_cast<int>(UnfetchedChunkIndexes.size()));
        SendRequests();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
