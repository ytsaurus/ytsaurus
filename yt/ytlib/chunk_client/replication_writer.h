#pragma once

#include "public.h"
#include "config.h"
#include "async_writer.h"

#include <ytlib/erasure_codecs/codec.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr GetReplicationWriter(
    const TReplicationWriterConfigPtr& config,
    const TChunkId& chunkId,
    const std::vector<TNodeDescriptor>& targets);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
