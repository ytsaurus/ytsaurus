#include "ordered_chunk_store.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTabletNode {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NApi;
using namespace NDataNode;

////////////////////////////////////////////////////////////////////////////////

TOrderedChunkStore::TOrderedChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    IBlockCachePtr blockCache,
    TChunkRegistryPtr chunkRegistry,
    TChunkBlockManagerPtr chunkBlockManager,
    IClientPtr client,
    const TNullable<TNodeDescriptor>& localDescriptor)
    : TStoreBase(config, id, tablet)
    , TChunkStoreBase(
        config,
        id,
        tablet,
        blockCache,
        chunkRegistry,
        chunkBlockManager,
        client,
        localDescriptor)
    , TOrderedStoreBase(config, id, tablet)
{
    LOG_DEBUG("Ordered chunk store created");
}

TOrderedChunkStore::~TOrderedChunkStore()
{
    LOG_DEBUG("Ordered chunk store destroyed");
}

TOrderedChunkStorePtr TOrderedChunkStore::AsOrderedChunk()
{
    return this;
}

EStoreType TOrderedChunkStore::GetType() const
{
    return EStoreType::OrderedChunk;
}

EInMemoryMode TOrderedChunkStore::GetInMemoryMode() const
{
    return EInMemoryMode::None;
}

void TOrderedChunkStore::SetInMemoryMode(EInMemoryMode mode)
{ }

void TOrderedChunkStore::Preload(TInMemoryChunkDataPtr chunkData)
{ }

ISchemafulReaderPtr TOrderedChunkStore::CreateReader(
    i64 lowerRowIndex,
    i64 upperRowIndex,
    const TTableSchema& schema,
    const TWorkloadDescriptor& workloadDescriptor)
{
    YUNIMPLEMENTED();
}

IBlockCachePtr TOrderedChunkStore::GetBlockCache()
{
    return GetNullBlockCache();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
