#pragma once

#include "public.h"

#include "chunk_reader.h"

#include <yt/yt/library/erasure/impl/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RepairErasedPartsStriped(
    TErasureReaderConfigPtr config,
    const NErasure::ICodec* codec,
    std::vector<IChunkReaderAllowingRepairPtr> partReaders,
    std::vector<IChunkWriterPtr> partWriters,
    TChunkReaderMemoryManagerPtr memoryManager,
    IChunkReader::TReadBlocksOptions readBlocksOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
