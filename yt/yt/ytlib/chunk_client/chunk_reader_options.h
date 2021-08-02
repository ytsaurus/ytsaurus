#pragma once

#include "chunk_reader_statistics.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/misc/workload.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TClientChunkReadOptions
{
    TWorkloadDescriptor WorkloadDescriptor;
    TReadSessionId ReadSessionId;

    TChunkReaderStatisticsPtr ChunkReaderStatistics = New<TChunkReaderStatistics>();
    // NB: If |HunkChunkReaderStatistics| is null and hunk chunk reading is performed,
    // relevant statistics will be updated within |ChunkReaderStatistics|.
    NTableClient::IHunkChunkReaderStatisticsPtr HunkChunkReaderStatistics;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
