#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/profiling/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void ProfileChunkWriter(
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::NProto::TDataStatistics&,
    const NChunkClient::TCodecStatistics& codecStatistics,
    NProfiling::TTagId methodTag);

void ProfileChunkReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::NProto::TDataStatistics& dataStatistics,
    const NChunkClient::TCodecStatistics& codecStatistics,
    const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics,
    NProfiling::TTagId methodTag);

void ProfileDynamicMemoryUsage(
    const NProfiling::TTagIdList& tags,
    ETabletDynamicMemoryType memoryType,
    i64 memoryUsage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
