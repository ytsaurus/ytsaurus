#include "public.h"

#include <yt/ytlib/table_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NTableClient::IVersionedChunkWriterPtr CreateVersionedChunkInMemoryWriter(
    NTableClient::TChunkWriterConfigPtr config,
    NTableClient::TChunkWriterOptionsPtr options,
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache);

NTableClient::IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkInMemoryWriter(
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    NTableClient::IVersionedMultiChunkWriterPtr underlyingWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT


