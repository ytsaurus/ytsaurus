#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/signal.h>

#include <core/erasure/public.h>

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

