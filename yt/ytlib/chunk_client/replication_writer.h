#pragma once

#include "public.h"
#include "config.h"
#include "async_writer.h"

#include <ytlib/concurrency/throughput_throttler.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    const std::vector<NNodeTrackerClient::TNodeDescriptor>& targets,
    EWriteSessionType sessionType = EWriteSessionType::User,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
