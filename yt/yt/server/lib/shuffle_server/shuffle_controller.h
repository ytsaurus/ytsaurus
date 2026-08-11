#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>

#include <yt/yt/ytlib/push_based_shuffle_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

//! A storage/lifetime handle for a shuffle controller of either mode.
struct IShuffleController
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(IShuffleController)

////////////////////////////////////////////////////////////////////////////////

//! Pull-based shuffle: writers write their own chunks and register them; readers
//! fetch the chunk slices for a partition.
struct IPullBasedShuffleController
    : public IShuffleController
{
    virtual TFuture<void> RegisterChunks(
        std::vector<NChunkClient::TInputChunkPtr> chunks,
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData) = 0;

    virtual TFuture<std::vector<NChunkClient::TInputChunkSlicePtr>> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> logicalWriterIndexRange) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPullBasedShuffleController)

////////////////////////////////////////////////////////////////////////////////

//! Result of a push-based fetch: the partition's journal chunks plus the set of
//! valid writer ids used by the reader to filter superseded writer registrations.
struct TPushBasedFetchResult
{
    //! Raw (chunk_id, replicas) pairs, bypassing TInputChunk because push-based
    //! chunks are journal chunks without misc meta (row_count etc.) at fetch time.
    std::vector<NDistributedChunkSessionClient::TSlotChunkInfo> Chunks;
    std::vector<i32> ValidWriterIds;
};

struct TWriterRegistration
{
    i32 WriterId = 0;
    std::vector<NDistributedChunkSessionClient::TReadySession> ReadySessions;
};

//! Push-based shuffle: writers push records into shared per-partition journal
//! chunk sessions; readers fetch the partition's chunks plus the valid writer set.
struct IPushBasedShuffleController
    : public IShuffleController
{
    virtual TFuture<TWriterRegistration> RegisterWriter(
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData) = 0;

    virtual TFuture<NDistributedChunkSessionClient::TSessionDescriptor> GetPartitionWriteSession(
        int partitionIndex,
        std::optional<NChunkClient::TSessionId> excludedSessionId) = 0;

    virtual TFuture<TPushBasedFetchResult> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> logicalWriterIndexRange) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPushBasedShuffleController)

////////////////////////////////////////////////////////////////////////////////

IPullBasedShuffleControllerPtr CreatePullBasedShuffleController(
    int partitionCount,
    IInvokerPtr invoker,
    NApi::ITransactionPtr transaction);

IPushBasedShuffleControllerPtr CreatePushBasedShuffleController(
    int partitionCount,
    IInvokerPtr invoker,
    NApi::NNative::IClientPtr client,
    NApi::ITransactionPtr transaction,
    std::string account,
    std::string medium,
    int replicationFactor,
    NPushBasedShuffleClient::TPushShuffleConfigPtr pushConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
