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

void TTablet::CopyMetaFrom(const TTablet* tablet)
{
    Id_ = tablet->Id_;
    CellId_ = tablet->CellId_;
    PhysicallySorted_ = tablet->PhysicallySorted_;
    Mounted_ = tablet->Mounted_;
    MountConfig_ = tablet->MountConfig_;
    MountRevision_ = tablet->MountRevision_;
    LoggingTag_ = tablet->LoggingTag_;

    OverlappingStoreCount_ = tablet->OverlappingStoreCount_;
    EdenOverlappingStoreCount_ = tablet->EdenOverlappingStoreCount_;
    CriticalPartitionCount_ = tablet->CriticalPartitionCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
