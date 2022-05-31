#include "tablet.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/server/lib/tablet_node/config.h>

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

void TTablet::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id_);
    Persist(context, CellId_);
    Persist(context, TabletCellBundle_);
    Persist(context, PhysicallySorted_);
    Persist(context, Mounted_);
    Persist<TNonNullableIntrusivePtrSerializer<>>(context, MountConfig_);
    Persist(context, MountRevision_);
    Persist(context, LoggingTag_);
    Persist(context, IsForcedRotationPossible_);
    Persist(context, IsOverflowRotationNeeded_);
    Persist(context, LastPeriodicRotationTime_);
    Persist<TVectorSerializer<TUniquePtrSerializer<>>>(context, Partitions_);
    Persist<TUniquePtrSerializer<>>(context, Eden_);
    Persist(context, OverlappingStoreCount_);
    Persist(context, EdenOverlappingStoreCount_);
    Persist(context, CriticalPartitionCount_);
    Persist<TVectorSerializer<TUniquePtrSerializer<>>>(context, Stores_);

    if (context.IsLoad()) {
        auto onPartition = [&] (auto& partition) {
            partition->SetTablet(this);
            for (auto& store : partition->Stores()) {
                store->SetTablet(this);
            }
        };
        if (Eden_) {
            onPartition(Eden_);
        }
        for (auto& partition : Partitions_) {
            onPartition(partition);
        }
        for (auto& store : Stores_) {
            store->SetTablet(this);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
