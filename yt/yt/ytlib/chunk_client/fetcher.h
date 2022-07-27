#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IFetcherChunkScraper
    : public virtual TRefCounted
{
    //! Returns future, which gets set when all chunks become available.
    virtual TFuture<void> ScrapeChunks(const THashSet<TInputChunkPtr>& chunkSpecs) = 0;

    //! Number of currently unavailable chunks.
    virtual i64 GetUnavailableChunkCount() const = 0;
};

IFetcherChunkScraperPtr CreateFetcherChunkScraper(
    const TChunkScraperConfigPtr config,
    const IInvokerPtr invoker,
    TThrottlerManagerPtr throttlerManager,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct IFetcher
    : public virtual TRefCounted
{
    virtual void AddChunk(TInputChunkPtr chunk) = 0;
    virtual int GetChunkCount() const = 0;
    virtual void SetCancelableContext(TCancelableContextPtr cancelableContext) = 0;
    virtual TFuture<void> Fetch() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFetcherBase
    : public virtual IFetcher
{
public:
    TFetcherBase(
        TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        const NLogging::TLogger& logger);

    void AddChunk(TInputChunkPtr chunk) override;
    int GetChunkCount() const override;

    TFuture<void> Fetch() override;

    //! Set cancelable context for the fetcher.
    //! NB: if invoker is cancelable, do not ever forget to provide its cancelable context;
    //! otherwise internal promise inside fetcher may never be set and WaitFor on the fetch future
    //! will never succeed, leading to fiber leak (refer to YT-11643 for example).
    void SetCancelableContext(TCancelableContextPtr cancelableContext) override;

protected:
    const TFetcherConfigPtr Config_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const IInvokerPtr Invoker_;
    const IFetcherChunkScraperPtr ChunkScraper_;
    const NLogging::TLogger Logger;

    //! All chunks for which info is to be fetched.
    std::vector<TInputChunkPtr> Chunks_;

    virtual void ProcessDynamicStore(int chunkIndex) = 0;

    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) = 0;

    virtual void OnFetchingStarted();
    virtual void OnFetchingCompleted();

    NRpc::IChannelPtr GetNodeChannel(NNodeTrackerClient::TNodeId nodeId);

    void StartFetchingRound(const TError& error);

    // NB: Timeouts are retried on the same node.
    void OnChunkFailed(
        NNodeTrackerClient::TNodeId nodeId,
        int chunkIndex,
        const TError& error);
    void OnNodeFailed(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes);
    void OnRequestThrottled(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes);

private:
    void OnCanceled(const TError& error);

    NApi::NNative::IClientPtr Client_;

    // NB: At each moment in time a chunk index is in exactly one of three possible states:
    //     - Stored in |UnfetchedChunkIndexes_|
    //     - Stored in |NodeToChunkIndexesToFetch_|
    //     - Currently being fetched by |FetchFromNode|
    //! Indexes of chunks for which no info is fetched yet.
    THashSet<int> UnfetchedChunkIndexes_;
    //! Indexes of chunks to be fetched by each node in the current round.
    THashMap<NNodeTrackerClient::TNodeId, THashSet<int>> NodeToChunkIndexesToFetch_;

    //! Ids of nodes that failed to reply.
    THashSet<NNodeTrackerClient::TNodeId> DeadNodes_;

    //! |(nodeId, chunkId)| pairs for which an error was returned from the node.
    std::set<std::pair<NNodeTrackerClient::TNodeId, TChunkId>> DeadChunks_;

    //! |(unbanTime, nodeId)| pairs for nodes that throttled our requests.
    std::set<std::pair<TInstant, NNodeTrackerClient::TNodeId>> BannedNodes_;

    //! NodeId -> UnbanTime for nodes that throttled our requests.
    THashMap<NNodeTrackerClient::TNodeId, TInstant> UnbanTime_;

    TCancelableContextPtr CancelableContext_;

    TAtomicObject<TFuture<void>> ActiveTaskFuture_;

    TPromise<void> Promise_ = NewPromise<void>();

    void OnFetchingRoundCompleted(bool backoff, const TError& error);
    void OnChunkLocated(TChunkId chunkId, const TChunkReplicaList& replicas);

    //! Fetches all chunks in |NodeToChunkIndexesToFetch_| via calls to |FetchFromNode|.
    //! Timeouts are retried until they result in success or another error.
    void PerformFetchingRoundFromNode(NNodeTrackerClient::TNodeId nodeId);
};

DEFINE_REFCOUNTED_TYPE(IFetcherChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
