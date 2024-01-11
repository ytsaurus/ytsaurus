#pragma once

#include "partition.h"

#include <yt/yt/server/lib/lsm/tablet.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NLsm::TTablet
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TPartition>>, Partitions);
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TPartition>, Eden);
    DEFINE_BYVAL_RW_PROPERTY(TStructuredLoggerPtr, StructuredLogger);

public:
    NLsm::TTabletPtr ToLsmTablet()
    {
        YT_VERIFY(IsPhysicallySorted());

        NLsm::TTablet::Eden_ = Eden_->ToLsmPartition(this);
        for (auto& partition : Partitions_) {
            NLsm::TTablet::Partitions_.push_back(partition->ToLsmPartition(this));
        }

        return MakeStrong(this);
    }

    void ResetLsmState()
    {
        NLsm::TTablet::Eden_.release();
        for (auto& partition : NLsm::TTablet::Partitions_) {
            partition.release();
        }
        NLsm::TTablet::Partitions_.clear();

        Eden_->ResetLsmState();
        for (auto& partition : Partitions_) {
            partition->ResetLsmState();
        }
    }

    TPartition* FindPartition(TPartitionId partitionId) const
    {
        if (Eden_->GetId() == partitionId) {
            return Eden_.get();
        }
        for (auto& partition : Partitions_) {
            if (partition->GetId() == partitionId) {
                return partition.get();
            }
        }
        return nullptr;
    }

    void Persist(const TStreamPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TTablet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
