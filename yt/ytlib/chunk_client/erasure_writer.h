#pragma once

#include "public.h"

#include <core/erasure/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ICodec* codec,
    const std::vector<IAsyncWriterPtr>& writers);

std::vector<IAsyncWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    std::vector<NNodeTrackerClient::TNodeDescriptor> targets,
    EWriteSessionType sessionType);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

