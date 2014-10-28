#pragma once

#include "public.h"

#include <core/erasure/public.h>

#include <core/rpc/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ICodec* codec,
    const std::vector<IChunkWriterPtr>& writers);

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NRpc::IChannelPtr masterChannel,
    EWriteSessionType sessionType);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

