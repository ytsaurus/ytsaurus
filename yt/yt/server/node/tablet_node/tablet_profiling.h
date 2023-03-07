#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/client/chunk_client/data_statistics.h>

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

class TWriterProfiler
    : public TIntrinsicRefCounted
{
public:
    TWriterProfiler() = default;

    void Profile(const TTabletSnapshotPtr& tabletSnapshot, NProfiling::TTagId tag);
    void Update(const NTableClient::IVersionedMultiChunkWriterPtr& writer);
    void Update(const NChunkClient::IChunkWriterBasePtr& writer);

private:
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics CodecStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TWriterProfiler)

/////////////////////////////////////////////////////////////////////////////

class TReaderProfiler
    : public TIntrinsicRefCounted
{
public:
    TReaderProfiler() = default;

    void Profile(const TTabletSnapshotPtr& tabletSnapshot, NProfiling::TTagId tag);
    void Update(
        const NTableClient::IVersionedReaderPtr& reader,
        const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics);

    void SetCompressedDataSize(i64 compressedDataSize);
    void SetCodecStatistics(const NChunkClient::TCodecStatistics& codecStatistics);
    void SetChunkReaderStatistics(const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics);

private:
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics CodecStatistics_;
    NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TReaderProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
