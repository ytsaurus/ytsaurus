#pragma once

#include "public.h"
#include "block_cache.h"
#include "s3_common.h"

#include <yt/yt/library/s3/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Add more options: throttlers, traffic meters (?), etc.
IChunkWriterPtr CreateS3Writer(
    // TODO(achulkov2): This is a just a stub before we have proper S3 medium support in master.
    TS3MediumStubPtr medium,
    TS3WriterConfigPtr config,
    // TODO(achulkov2): Do we need session id with medium index, or can we just use chunk id?
    TSessionId sessionId,
    IBlockCachePtr blockCache = GetNullBlockCache());


// TODO(achulkov2): Throttlers require some thought. Replication writer has a single throttler
// passed from the outside which throttlers all writes based on their size. For S3, we might
// want a per-medium throttler, either separately for reads and writes, or even a combined one.
// We could even introduce a cost throttler, which would somehow estimate the cost of
// various S3 operations based on configured constants.

// TODO(achulkov2): Profiling, statistics.

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
