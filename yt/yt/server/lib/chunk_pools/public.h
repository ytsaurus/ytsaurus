#pragma once

#include <yt/core/misc/common.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct ISortedChunkPool;

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcherFactory)

DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)
DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)

DEFINE_ENUM(EErrorCode,
    ((DataSliceLimitExceeded)             (2000))
    ((MaxDataWeightPerJobExceeded)        (2001))
    ((MaxPrimaryDataWeightPerJobExceeded) (2002))
);

constexpr double ApproximateSizesBoostFactor = 1.3;


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
