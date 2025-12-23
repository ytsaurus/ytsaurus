#pragma once

#include "public.h"
#include "client_block_cache.h"
#include "chunk_reader_allowing_repair.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaList seedReplicas);

// TODO(discuss in PR): we need the last argument to be of type TChunkReplicaWithMedium,
// as we need the medium index to read offshore data. 8 months ago there was a commit which stops
// passing the medium index into chunk readers, so we need to understand how to handle this. Maybe
// (partially) revert the commit?
IChunkReaderAllowingRepairPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaWithMediumList seedReplicas);

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReaderThrottlingAdapter(
    IChunkReaderPtr underlyingReader,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
