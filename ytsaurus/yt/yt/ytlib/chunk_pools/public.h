#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkPoolInput)
DECLARE_REFCOUNTED_STRUCT(IChunkPoolOutput)
DECLARE_REFCOUNTED_STRUCT(IChunkPool)

constexpr double ApproximateSizesBoostFactor = 1.3;

////////////////////////////////////////////////////////////////////////////////

// It's quite easy to mix up input cookies with output cookies,
// so we use two following aliases to visually distinguish them.
using TInputCookie = int;
using TOutputCookie = int;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
