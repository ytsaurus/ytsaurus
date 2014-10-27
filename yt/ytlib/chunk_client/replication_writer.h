#pragma once

#include "public.h"

#include <core/concurrency/throughput_throttler.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    const TChunkReplicaList& targets,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NRpc::IChannelPtr masterChannel,
    EWriteSessionType sessionType = EWriteSessionType::User,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
