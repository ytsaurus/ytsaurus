#include "ordered_chunk_store.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTabletNode {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TOrderedChunkStore::TOrderedChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    NCellNode::TBootstrap* bootstrap)
    : TStoreBase(config, id, tablet)
    , TChunkStoreBase(config, id, tablet, bootstrap)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
