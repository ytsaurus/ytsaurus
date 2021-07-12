#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcherFactory)

DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)
DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)

DECLARE_REFCOUNTED_STRUCT(IChunkPoolInput)
DECLARE_REFCOUNTED_STRUCT(IChunkPoolOutput)
DECLARE_REFCOUNTED_STRUCT(IChunkPool)
DECLARE_REFCOUNTED_STRUCT(IShuffleChunkPool)
DECLARE_REFCOUNTED_STRUCT(ISortedChunkPool)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkPoolInput)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkPoolOutput)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkPool)
DECLARE_REFCOUNTED_STRUCT(IChunkPoolJobSplittingHost)

DEFINE_ENUM(EErrorCode,
    ((DataSliceLimitExceeded)             (2000))
    ((MaxDataWeightPerJobExceeded)        (2001))
    ((MaxPrimaryDataWeightPerJobExceeded) (2002))
);

constexpr double ApproximateSizesBoostFactor = 1.3;

////////////////////////////////////////////////////////////////////////////////

// It's quite easy to mix up input cookies with output cookies,
// so we use two following aliases to visually distinguish them.
using TInputCookie = int;
using TOutputCookie = int;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
