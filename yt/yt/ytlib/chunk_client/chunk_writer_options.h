#pragma once

#include "public.h"

#include "chunk_writer_statistics.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TClientChunkWriteOptions
{
    TChunkWriterStatisticsPtr ChunkWriterStatistics = New<TChunkWriterStatistics>();
    TJobIoMeterPtr JobIoMeter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
