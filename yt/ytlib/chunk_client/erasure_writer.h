#pragma once

#include "public.h"

#include <core/erasure/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ICodec* codec,
    const std::vector<IWriterPtr>& writers);

std::vector<IWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NRpc::IChannelPtr masterChannel,
    EWriteSessionType sessionType);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

