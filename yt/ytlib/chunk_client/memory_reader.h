#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateMemoryReader(
    TRefCountedChunkMetaPtr meta,
    std::vector<TBlock> blocks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
