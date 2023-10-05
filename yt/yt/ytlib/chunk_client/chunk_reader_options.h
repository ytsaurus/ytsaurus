#pragma once

#include "chunk_reader_statistics.h"

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TClientChunkReadOptions
{
    TWorkloadDescriptor WorkloadDescriptor;
    TReadSessionId ReadSessionId;
    NRpc::EMultiplexingBand MultiplexingBand = NRpc::EMultiplexingBand::Heavy;
    int MultiplexingParallelism = 1;
    bool UseDedicatedAllocations = false;
    bool TrackMemoryAfterSessionCompletion = false;

    TChunkReaderStatisticsPtr ChunkReaderStatistics = New<TChunkReaderStatistics>();

    // NB: If |HunkChunkReaderStatistics| is null and hunk chunk reading is
    // performed, relevant statistics will be updated within
    // |ChunkReaderStatistics|.
    NTableClient::IHunkChunkReaderStatisticsPtr HunkChunkReaderStatistics;

    // If non-null, |HedgingManager| determines hedging policy shared between various chunk readers.
    IHedgingManagerPtr HedgingManager;

    IMemoryReferenceTrackerPtr MemoryReferenceTracker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
