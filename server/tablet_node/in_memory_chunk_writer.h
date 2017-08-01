#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemalessChunkWriterPtr CreateInMemorySchemalessChunkWriter(
    NTableClient::TChunkWriterConfigPtr config,
    NTableClient::TChunkWriterOptionsPtr options,
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    NChunkClient::IChunkWriterPtr chunkWriter,
    const NTableClient::TChunkTimestamps& chunkTimestamps,
    NChunkClient::IBlockCachePtr blockCache);

NTableClient::IVersionedChunkWriterPtr CreateInMemoryVersionedChunkWriter(
    NTableClient::TChunkWriterConfigPtr config,
    NTableClient::TChunkWriterOptionsPtr options,
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache);

NTableClient::IVersionedMultiChunkWriterPtr CreateInMemoryVersionedMultiChunkWriter(
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    NTableClient::IVersionedMultiChunkWriterPtr underlyingWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT


