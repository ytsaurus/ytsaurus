#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateCacheReader(
    const TChunkId& chunkId,
    IBlockCachePtr blockCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
