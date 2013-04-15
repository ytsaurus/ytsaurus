#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/node_tracker_client/public.h>
#include <ytlib/chunk_client/input_chunk.h>

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

    void AddChunk(NChunkClient::TRefCountedInputChunkPtr chunk);
    TFuture< TValueOrError<void> > Run();

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    TFetcherPtr Fetcher;
    IInvokerPtr Invoker;

    TPromise< TValueOrError<void> > Promise;

    //! All chunks for which info is to be fetched.
    std::vector<NChunkClient::TRefCountedInputChunkPtr> Chunks;

    //! Indexes of chunks for which no info is fetched yet.
    yhash_set<int> UnfetchedChunkIndexes;

    //! Addresses of nodes that failed to reply.
    yhash_set<Stroka> DeadNodes;

    //! |(address, chunkId)| pairs for which an error was returned from the node.
    // XXX(babenko): need to specialize hash to use yhash_set
    std::set< std::pair<Stroka, NChunkClient::TChunkId> > DeadChunkIds;

    void SendRequests();
    void OnResponse(
        const NNodeTrackerClient::TNodeDescriptor& descriptor,
        std::vector<int> chunkIndexes,
        typename TFetcher::TResponsePtr rsp);
    void OnEndRound();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define CHUNK_INFO_COLLECTOR_INL_H_
#include "chunk_info_collector-inl.h"
#undef CHUNK_INFO_COLLECTOR_INL_H_
