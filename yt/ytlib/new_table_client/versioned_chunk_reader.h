#pragma once

#include "public.h"

#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr uncompressedBlockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NChunkClient::TReadLimit lowerLimit,
    NChunkClient::TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
