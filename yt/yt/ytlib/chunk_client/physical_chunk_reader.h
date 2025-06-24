#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreatePhysicalChunkReader(
    TPhysicalChunkReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaWithMediumList seedReplicas);

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreatePhysicalChunkReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
