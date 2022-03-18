#pragma once

#include "public.h"

#include <yt/yt/library/erasure/impl/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RepairErasedPartsStriped(
    TErasureReaderConfigPtr config,
    const NErasure::ICodec* codec,
    std::vector<IChunkReaderAllowingRepairPtr> partReaders,
    std::vector<IChunkWriterPtr> partWriters,
    TChunkReaderMemoryManagerPtr memoryManager,
    IBlockCachePtr blockCache,
    TClientChunkReadOptions chunkReadOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
