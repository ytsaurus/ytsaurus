#include "stdafx.h"
#include "store.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

EStoreState IStore::GetPersistentState() const
{
    auto state = GetState();
    switch (state) {
        case EStoreState::Flushing:
        case EStoreState::FlushFailed:
            return EStoreState::PassiveDynamic;

        case EStoreState::Compacting:
        case EStoreState::CompactionFailed:
            return EStoreState::Persistent;

        default:
            return state;
    }
}

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

} // namespace NTabletNode
} // namespace NYT
