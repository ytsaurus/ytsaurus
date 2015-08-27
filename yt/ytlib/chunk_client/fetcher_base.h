#pragma once

#include "public.h"

#include <ytlib/node_tracker_client/public.h>

#include <core/misc/guid.h>
#include <core/misc/error.h>

#include <core/rpc/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

typedef TCallback<TFuture<void>(yhash_set<TRefCountedChunkSpecPtr> chunkSpecs)> TScrapeChunksCallback;

TScrapeChunksCallback CreateScrapeChunksSessionCallback(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    const NConcurrency::IThroughputThrottlerPtr throttler,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

class TFetcherBase
    : public TRefCounted
{
public:
    TFetcherBase(
        TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        TScrapeChunksCallback scraperCallback,
        const NLogging::TLogger& logger);

    virtual void AddChunk(TRefCountedChunkSpecPtr chunk);
    virtual TFuture<void> Fetch();

protected:
    const TFetcherConfigPtr Config_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const IInvokerPtr Invoker_;

    //! All chunks for which info is to be fetched.
    std::vector<TRefCountedChunkSpecPtr> Chunks_;
    TScrapeChunksCallback ScraperCallback_;
    NLogging::TLogger Logger;


    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) = 0;

    NRpc::IChannelPtr GetNodeChannel(NNodeTrackerClient::TNodeId nodeId);

    void StartFetchingRound();

    void OnChunkFailed(
        NNodeTrackerClient::TNodeId nodeId,
        int chunkIndex,
        const TError& error);
    void OnNodeFailed(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes);

private:
    //! Indexes of chunks for which no info is fetched yet.
    yhash_set<int> UnfetchedChunkIndexes_;

    //! Ids of nodes that failed to reply.
    yhash_set<NNodeTrackerClient::TNodeId> DeadNodes_;

    //! |(nodeId, chunkId)| pairs for which an error was returned from the node.
    std::set< std::pair<NNodeTrackerClient::TNodeId, TChunkId> > DeadChunks_;

    TPromise<void> Promise_ = NewPromise<void>();


    void OnFetchingRoundCompleted(const TError& error);
    void OnChunkLocated(const TChunkId& chunkId, const TChunkReplicaList& replicas);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
