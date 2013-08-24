#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/actions/signal.h>

#include <ytlib/erasure/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateNonReparingErasureReader(
    const std::vector<IAsyncReaderPtr>& dataBlocksReaders);

typedef TCallback<void(double)> TRepairProgressHandler;

TAsyncError RepairErasedParts(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<IAsyncReaderPtr>& readers,
    const std::vector<IAsyncWriterPtr>& writers,
    TRepairProgressHandler onProgress = TRepairProgressHandler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

