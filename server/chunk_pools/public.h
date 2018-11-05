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

// TODO(max42): add these codes to the documentation.
DEFINE_ENUM(EErrorCode,
    ((DataSliceLimitExceeded)(2000))
    ((MaxDataWeightPerJobExceeded)(2001))
    ((MaxPrimaryDataWeightPerJobExceeded)(2002))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
