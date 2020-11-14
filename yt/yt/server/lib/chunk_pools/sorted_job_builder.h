#pragma once

#include "private.h"

#include "chunk_pool.h"
#include "job_manager.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TSortedJobOptions
{
    bool EnableKeyGuarantee = false;
    int PrimaryPrefixLength = 0;
    int ForeignPrefixLength = 0;
    bool EnablePeriodicYielder = true;
    bool ShouldSlicePrimaryTableByKeys = false;

    std::vector<NTableClient::TLegacyKey> PivotKeys;

    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount;

    bool LogDetails = false;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
