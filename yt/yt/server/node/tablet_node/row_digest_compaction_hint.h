#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipelinePtr CreateRowDigestFetchPipeline(
    TSortedChunkStore* store,
    const TExponentialBackoffOptions& retryBackoffOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
