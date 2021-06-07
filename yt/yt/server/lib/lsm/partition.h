#pragma once

#include <yt/yt/client/table_client/key.h>

#include "public.h"
#include "store.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

class TPartition
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TPartitionId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TTablet*, Tablet);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);

    DEFINE_BYREF_RW_PROPERTY(NTableClient::TLegacyOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TLegacyOwningKey, NextPivotKey);

    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TStore>>, Stores);

    DEFINE_BYVAL_RW_PROPERTY(EPartitionState, State);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CompactionTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AllowedSplitTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, SamplingRequestTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, SamplingTime);

    DEFINE_BYVAL_RW_PROPERTY(bool, IsImmediateSplitRequested);

    DEFINE_BYVAL_RW_PROPERTY(i64, CompressedDataSize);
    DEFINE_BYVAL_RW_PROPERTY(i64, UncompressedDataSize);

public:
    bool IsEden() const
    {
        return Index_ == EdenIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
