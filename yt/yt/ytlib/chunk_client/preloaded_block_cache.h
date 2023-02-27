#pragma once

#include "public.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IBlockCachePtr GetPreloadedBlockCache(IChunkReaderPtr chunkReader);

IBlockCachePtr GetPreloadedBlockCache(TChunkId chunkId, const std::vector<NChunkClient::TBlock>& blocks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
