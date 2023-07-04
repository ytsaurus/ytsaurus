#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::IChunkReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    NChunkClient::TChunkId chunkId,
    NErasure::ECodec codecId,
    const NChunkClient::TChunkReplicaWithMediumList& replicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
