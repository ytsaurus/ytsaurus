#include "tablet.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

bool TTablet::IsPhysicallySorted() const
{
    return GetPhysicallySorted();
}

TStore* TTablet::FindActiveStore() const
{
    if (IsPhysicallySorted()) {
        for (const auto& store : Eden_->Stores()) {
            if (store->GetStoreState() == EStoreState::ActiveDynamic) {
                return store.get();
            }
        }
    } else {
        for (const auto& store : Stores_) {
            if (store->GetStoreState() == EStoreState::ActiveDynamic) {
                return store.get();
            }
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
