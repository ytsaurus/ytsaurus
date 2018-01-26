#pragma once

#include <yt/core/misc/common.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput;
struct IChunkPoolOutput;

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcherFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
