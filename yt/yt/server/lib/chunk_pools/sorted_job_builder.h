#pragma once

#include "private.h"

#include "chunk_pool.h"
#include "new_job_manager.h"
#include "job_size_tracker.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TSortedJobOptions
{
    bool EnableKeyGuarantee = false;
    // COMPAT(max42): we are keeping both comparator and prefix length in order
    // to maintain single TSortedJobOptions instead of two almost duplicating classes
    // with almost duplicating filling code in sorted task.
    NTableClient::TComparator PrimaryComparator;
    NTableClient::TComparator ForeignComparator;
    int PrimaryPrefixLength = 0;
    int ForeignPrefixLength = 0;
    bool EnablePeriodicYielder = true;
    // Used only in legacy pool.
    bool ShouldSlicePrimaryTableByKeys = false;
    bool ValidateOrder = true;

    bool ConsiderOnlyPrimarySize = false;

    std::vector<NTableClient::TLegacyKey> PivotKeys;

    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount;

    TJobSizeTrackerOptions JobSizeTrackerOptions;

    PHOENIX_DECLARE_TYPE(TSortedJobOptions, 0x54c67649);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
