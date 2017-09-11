#pragma once

#include "public.h"

#include <yt/core/erasure/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRepairingReader(
    NErasure::ICodec *codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderPtr>& dataBlocksReaders);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
