#include "stdafx.h"
#include "store.h"

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
