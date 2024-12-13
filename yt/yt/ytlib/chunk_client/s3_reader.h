#pragma once

#include "public.h"
#include "s3_common.h"
#include "medium_directory.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateS3Reader(
    TS3MediumDescriptorPtr mediumDescriptor,
    TS3ReaderConfigPtr config,
    TChunkId chunkId);

// TODO(achulkov2): [PForReview] Fix this comment. Think about throttlers. Replication reader
// can be configured with a multiple throttlers: bandwith, RPS and medium. They typically come
// from TChunkReaderHost, but in general seem to be used very rarely.
// What throttlers do we want for S3? More importantly, where should these throttlers come from?
// Maybe we should take throttler configuration somewhere from the medium master (!) config, since
// the S3 medium doesn't actually live on any YT servers physically. These will have to be distributed
// throttlers though, local throttlers do not seem to be prevalent (yet we should still make it possible).

// TODO(achulkov2): [PLater] Profiling, statistics.

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient