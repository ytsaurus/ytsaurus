#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)

DECLARE_REFCOUNTED_CLASS(TChunkStripe)

DECLARE_REFCOUNTED_CLASS(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkPoolInput)
DECLARE_REFCOUNTED_STRUCT(IChunkPoolOutput)
DECLARE_REFCOUNTED_STRUCT(IChunkPoolOutputWithOrderedCookies)
DECLARE_REFCOUNTED_STRUCT(IChunkPool)

struct TPersistentChunkStripeStatistics;

constexpr double ApproximateSizesBoostFactor = 1.3;

////////////////////////////////////////////////////////////////////////////////

// It's quite easy to mix up input cookies with output cookies,
// so we use two following aliases to visually distinguish them.
using TInputCookie = int;
using TOutputCookie = int;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
