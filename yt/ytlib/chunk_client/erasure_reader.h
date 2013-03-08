#pragma once

#include "public.h"
#include "private.h"

#include <ytlib/rpc/public.h>
#include <ytlib/erasure_codecs/codec.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateErasureReader(
    const std::vector<IAsyncReaderPtr>& dataBlocksReaders);

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateErasureRepairReader(
    const NErasure::ICodec* codec,
    const std::vector<int>& erasedIndices,
    int chunkIndex,
    const std::vector<IAsyncReaderPtr>& readers);

///////////////////////////////////////////////////////////////////////////////

TAsyncError RepairErasedBlocks(
    const NErasure::ICodec* codec,
    const std::vector<int>& erasedIndices,
    const std::vector<IAsyncReaderPtr>& readers,
    const std::vector<IAsyncWriterPtr>& writers);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

