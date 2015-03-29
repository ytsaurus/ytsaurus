#pragma once

#include "public.h"

#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NChunkClient::TReadLimit lowerLimit,
    NChunkClient::TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

//! Creates a versioned chunk reader for a given set of keys.
/*! 
 *  Number of rows readable via this reader is equal to the number of passed keys.
 *  NB! Some rows may be null, if coresponding key is absent.
 *
 * \param keys A sorted vector of keys to be read. Caller must ensure key liveness during the reader lifetime.
*/
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const std::vector<TKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
