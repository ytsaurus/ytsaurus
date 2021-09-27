#include "partition.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

bool TPartition::IsEden() const
{
    return Index_ == EdenIndex;
}

void TPartition::CopyMetaFrom(const TPartition* partition)
{
    Id_ = partition->Id_;
    Index_ = partition->Index_;

    PivotKey_ = partition->PivotKey_;
    NextPivotKey_ = partition->NextPivotKey_;

    State_ = partition->State_;

    CompactionTime_ = partition->CompactionTime_;
    AllowedSplitTime_ = partition->AllowedSplitTime_;
    SamplingRequestTime_ = partition->SamplingRequestTime_;
    SamplingTime_ = partition->SamplingTime_;

    IsImmediateSplitRequested_ = partition->IsImmediateSplitRequested_;

    CompressedDataSize_ = partition->CompressedDataSize_;
    UncompressedDataSize_ = partition->UncompressedDataSize_;
}

void TPartition::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id_);
    Persist(context, Index_);
    Persist(context, PivotKey_);
    Persist(context, NextPivotKey_);
    Persist<TVectorSerializer<TUniquePtrSerializer<>>>(context, Stores_);
    Persist(context, State_);
    Persist(context, CompactionTime_);
    Persist(context, AllowedSplitTime_);
    Persist(context, SamplingRequestTime_);
    Persist(context, SamplingTime_);
    Persist(context, IsImmediateSplitRequested_);
    Persist(context, CompressedDataSize_);
    Persist(context, UncompressedDataSize_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
