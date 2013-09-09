#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/chunk_spec.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TFetcher>
class TChunkInfoCollector
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TFetcher> TFetcherPtr;

    TChunkInfoCollector(
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        TFetcherPtr fetcher,
        IInvokerPtr invoker);

    void AddChunk(NChunkClient::TRefCountedChunkSpecPtr chunk);
    TAsyncError Run();

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    TFetcherPtr Fetcher;
    IInvokerPtr Invoker;

    TAsyncErrorPromise Promise;

    //! All chunks for which info is to be fetched.
    std::vector<NChunkClient::TRefCountedChunkSpecPtr> Chunks;

    //! Indexes of chunks for which no info is fetched yet.
    yhash_set<int> UnfetchedChunkIndexes;

    //! Ids of nodes that failed to reply.
    yhash_set<NNodeTrackerClient::TNodeId> DeadNodeIds;

    //! |(nodeId, chunkId)| pairs for which an error was returned from the node.
    std::set< std::pair<NNodeTrackerClient::TNodeId, NChunkClient::TChunkId> > DeadChunkIds;

    void SendRequests();
    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes,
        typename TFetcher::TResponsePtr rsp);
    void OnEndRound();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define CHUNK_INFO_COLLECTOR_INL_H_
#include "chunk_info_collector-inl.h"
#undef CHUNK_INFO_COLLECTOR_INL_H_
