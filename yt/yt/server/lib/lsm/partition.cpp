#include "partition.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
