#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/erasure/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateNonReparingErasureReader(
    const std::vector<IAsyncReaderPtr>& dataBlocksReaders);

IAsyncReaderPtr CreateReparingErasureReader(
    const NErasure::ICodec* codec,
    const std::vector<int>& erasedIndices,
    int chunkIndex,
    const std::vector<IAsyncReaderPtr>& readers);

///////////////////////////////////////////////////////////////////////////////

TAsyncError RepairErasedBlocks(
    NErasure::ICodec* codec,
    const NErasure::TBlockIndexList& erasedIndices,
    const std::vector<IAsyncReaderPtr>& readers,
    const std::vector<IAsyncWriterPtr>& writers);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

