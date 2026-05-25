#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <atomic>
#include <optional>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReadResult
{
    std::vector<TSharedRef> Records;
    bool Finished = false;
};

struct TDistributedChunkSessionReaderStatistics
    : public TRefCounted
{
    std::atomic<i64> MasterRefreshCount = 0;
    std::atomic<i64> ActiveReplicaProgressQueryCount = 0;
    std::atomic<i64> SealedDetectedCount = 0;
    std::atomic<i64> ComputeQuorumInfoCount = 0;
    std::atomic<i64> ComputeQuorumInfoSuccessCount = 0;
    std::atomic<i64> ReadBlocksCount = 0;
    std::atomic<i64> ErrorAttemptCount = 0;
    std::atomic<i64> PollIterationCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TDistributedChunkSessionReaderStatistics)

////////////////////////////////////////////////////////////////////////////////

//! Reads opaque records from a single chunk produced by a distributed chunk session.
/*!
 *  The chunk does not need to be sealed: the reader can keep up with an actively
 *  written chunk and converges on the sealed-chunk view once writes complete.
 *
 *  Delivery contract:
 *    - records are returned in journal-index order;
 *    - the cursor advances monotonically, so no record index is delivered twice;
 *    - the stream may include uncommitted records — upper layers must filter or deduplicate.
 *
 *  Threading:
 *    - all methods may be called from any thread;
 *    - `Read` calls must not overlap: each must complete before the next is issued.
 */
struct IDistributedChunkSessionReader
    : public TRefCounted
{
    //! Returns at most one batch of records starting at the current cursor.
    //! `Finished` is set once the cursor reaches the effective end of the range.
    virtual TFuture<TChunkReadResult> Read() = 0;

    //! Signals that no further writes will occur, so the reader can stop probing
    //! replicas for new data. If `finalRecordCount` is provided, it is used as the
    //! chunk size and the reader skips the `ComputeQuorumInfo` round trip.
    virtual void SetAllWritersFinished(std::optional<i64> finalRecordCount = {}) = 0;

    //! Returns a read-only view of the live statistics counters.
    virtual TDistributedChunkSessionReaderStatisticsConstPtr GetStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionReader)

//! Creates a single-chunk distributed-session reader.
IDistributedChunkSessionReaderPtr CreateDistributedChunkSessionReader(
    TDistributedChunkSessionReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    NChunkClient::TChunkId chunkId,
    NChunkClient::TChunkReplicaList replicas,
    int readQuorum,
    i64 startRecordIndex,
    std::optional<i64> rangeEndRecordIndex,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
