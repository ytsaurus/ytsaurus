#pragma once

#include "chunk_pool.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/statistics.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/logging/serializable_logger.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TPushBasedShuffleChunkPoolOptions
{
    int PartitionCount = 0;
    i64 TargetUncompressedDataSizePerJob = 0;
    i64 MaxDataSliceCountPerJob = 0;

    //! Compressed size over uncompressed size, as elsewhere in YT, so within (0, 1];
    //! the estimate is uncompressed = compressed / ratio. Used only when neither the
    //! session nor the pool has observed progress.
    double SealFallbackCompressionRatio = 0.0;

    //! Used to estimate row count only when neither the session nor the pool
    //! has observed progress.
    i64 SealFallbackRowCountPerRecord = 0;

    NLogging::TSerializableLogger Logger;

    PHOENIX_DECLARE_TYPE(TPushBasedShuffleChunkPoolOptions, 0x84bced23);
};

////////////////////////////////////////////////////////////////////////////////

struct IPushBasedShuffleChunkPool
    : public virtual IShuffleChunkPool
{
    virtual void RegisterChunkWriteSession(
        int partitionIndex,
        NChunkClient::TChunkId chunkId,
        const NChunkClient::TChunkReplicaWithMediumList& replicas) = 0;

    //! #progress is cumulative since the session start and must grow componentwise;
    //! a nontrivial update must advance the record count. Every increment must also
    //! carry at least one unit of each measure per record.
    virtual void UpdateChunkWriteSession(
        NChunkClient::TChunkId chunkId,
        const NDistributedChunkSessionClient::TDistributedChunkSessionProgress& progress) = 0;

    //! #progress follows the same contract as in UpdateChunkWriteSession.
    virtual void FinishChunkWriteSession(
        NChunkClient::TChunkId chunkId,
        const NDistributedChunkSessionClient::TDistributedChunkSessionProgress& progress) = 0;

    //! #summary is cumulative and may exceed reported progress, which excludes on-disk
    //! padding; its unobserved suffix must still carry at least one compressed byte per
    //! record. Slices emitted for that suffix are approximate.
    virtual void FinishChunkWriteSessionFromSeal(
        NChunkClient::TChunkId chunkId,
        const NDistributedChunkSessionClient::TSessionSealSummary& summary) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPushBasedShuffleChunkPool)

////////////////////////////////////////////////////////////////////////////////

IPushBasedShuffleChunkPoolPtr CreatePushBasedShuffleChunkPool(
    const TPushBasedShuffleChunkPoolOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
