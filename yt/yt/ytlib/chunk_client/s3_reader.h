#pragma once

#include "public.h"
#include "s3_common.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Keep in mind IChunkReaderAllowingRepair.
IChunkReaderPtr CreateS3Reader(
    // TODO(achulkov2): This is a just a stub before we have proper S3 medium support in master.
    TS3MediumStubPtr medium,
    TS3ReaderConfigPtr config,
    TChunkId chunkId);

// TODO(achulkov2): Think about throttlers. Replication reader can be configured with a multiple
// throttlers: bandwith, RPS and medium. They typically come from TChunkReaderHost, but in general
// seem to be used very rarely.
// What throttlers do we want for S3? More importantly, where should these throttlers come from?
// Maybe we should take throttler configuration somewhere from the medium master (!) config, since
// the S3 medium doesn't actually live on any YT servers physically. These will have to be distributed
// throttlers though, local throttlers do not seem to be prevalent (yet we should still make it possible).

// TODO(achulkov2): Profiling, statistics.

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient