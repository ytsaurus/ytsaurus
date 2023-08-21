#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt/core/misc/linear_probe.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkLookupHashTable final
    : public TLinearProbeHashTable
{
    using TLinearProbeHashTable::TLinearProbeHashTable;
};

Y_FORCE_INLINE ui64 PackBlockAndRowIndexes(ui16 blockIndex, ui32 rowIndex);
Y_FORCE_INLINE std::pair<ui16, ui32> UnpackBlockAndRowIndexes(ui64 value);

DEFINE_REFCOUNTED_TYPE(TChunkLookupHashTable)

////////////////////////////////////////////////////////////////////////////////

TChunkLookupHashTablePtr CreateChunkLookupHashTableForColumnarFormat(
    IVersionedReaderPtr reader,
    size_t chunkRowCount);

TChunkLookupHashTablePtr CreateChunkLookupHashTable(
    NChunkClient::TChunkId chunkId,
    int startBlockIndex,
    int endBlockIndex,
    NChunkClient::IBlockCachePtr blockCache,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TKeyComparer& keyComparer);

TChunkLookupHashTablePtr CreateChunkLookupHashTable(
    NChunkClient::TChunkId chunkId,
    int startBlockIndex,
    const std::vector<NChunkClient::TBlock>& blocks,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TKeyComparer& keyComparer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define CHUNK_LOOKUP_HASH_TABLE_INL_H_
#include "chunk_lookup_hash_table-inl.h"
#undef CHUNK_LOOKUP_HASH_TABLE_INL_H_
