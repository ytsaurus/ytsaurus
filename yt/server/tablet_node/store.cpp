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
        default:
            return state;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
