#pragma once

#include "chunk_reader_statistics.h"

#include <yt/yt/client/misc/workload.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TClientChunkReadOptions
{
    TWorkloadDescriptor WorkloadDescriptor;
    TReadSessionId ReadSessionId;

    TChunkReaderStatisticsPtr ChunkReaderStatistics = New<TChunkReaderStatistics>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
