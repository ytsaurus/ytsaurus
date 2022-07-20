#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateMemoryReader(
    TRefCountedChunkMetaPtr meta,
    std::vector<TBlock> blocks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
