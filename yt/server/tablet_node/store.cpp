#include "stdafx.h"
#include "store.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TDynamicMemoryStorePtr IStore::AsDynamicMemory()
{
    auto* result = dynamic_cast<TDynamicMemoryStore*>(this);
    YCHECK(result);
    return result;
}

TChunkStorePtr IStore::AsChunk()
{
    auto* result = dynamic_cast<TChunkStore*>(this);
    YCHECK(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

Stroka TStoreIdFormatter::operator()(const IStorePtr& store) const
{
    return ToString(store->GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
