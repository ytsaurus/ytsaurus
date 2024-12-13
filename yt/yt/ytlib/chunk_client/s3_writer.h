#pragma once

#include "public.h"
#include "block_cache.h"
#include "medium_directory.h"
#include "s3_common.h"

#include <yt/yt/library/s3/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateS3Writer(
    TS3MediumDescriptorPtr mediumDescriptor,
    TS3WriterConfigPtr config,
    TSessionId sessionId,
    IBlockCachePtr blockCache = GetNullBlockCache());


// TODO(achulkov2): [PLater] Throttlers require some thought. Replication writer has a single throttler
// passed from the outside which throttlers all writes based on their size. For S3, we might
// want a per-medium throttler, either separately for reads and writes, or even a combined one.
// We could even introduce a cost throttler, which would somehow estimate the cost of
// various S3 operations based on configured constants.

// TODO(achulkov2): [PLater] Profiling, statistics.

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
