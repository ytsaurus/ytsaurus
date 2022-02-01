#include "cached_versioned_chunk_meta.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/bloom_filter.h>

namespace NYT::NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient;

using NYT::FromProto;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

TCachedVersionedChunkMeta::TCachedVersionedChunkMeta(const NChunkClient::NProto::TChunkMeta& chunkMeta)
    : TColumnarChunkMeta(chunkMeta)
{
    if (ChunkType_ != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Incorrect chunk type: actual %Qlv, expected %Qlv",
            ChunkType_,
            EChunkType::Table);
    }

    if (ChunkFormat_ != EChunkFormat::TableVersionedSimple &&
        ChunkFormat_ != EChunkFormat::TableVersionedColumnar &&
        ChunkFormat_ != EChunkFormat::TableUnversionedColumnar &&
        ChunkFormat_ != EChunkFormat::TableSchemalessHorizontal)
    {
        THROW_ERROR_EXCEPTION("Incorrect chunk format %Qlv",
            ChunkFormat_);
    }

    if (auto optionalHunkChunkRefsExt = FindProtoExtension<THunkChunkRefsExt>(chunkMeta.extensions())) {
        HunkChunkRefsExt_ = std::move(*optionalHunkChunkRefsExt);
    }
}

TCachedVersionedChunkMetaPtr TCachedVersionedChunkMeta::Create(const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta)
{
    return New<TCachedVersionedChunkMeta>(*chunkMeta);
}

i64 TCachedVersionedChunkMeta::GetMemoryUsage() const
{
    return TColumnarChunkMeta::GetMemoryUsage()
        + PreparedMetaSize_;
}

TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> TCachedVersionedChunkMeta::GetPreparedChunkMeta()
{
    if (!PreparedMeta_) {
        YT_VERIFY(GetChunkFormat() == NChunkClient::EChunkFormat::TableVersionedColumnar);

        auto preparedMeta = New<NNewTableClient::TPreparedChunkMeta>();
        auto size = preparedMeta->Prepare(GetChunkSchema(), ColumnMeta());

        if (PreparedMeta_.SwapIfCompare(nullptr, preparedMeta)) {
            PreparedMetaSize_ = size;

            if (MemoryTrackerGuard_) {
                MemoryTrackerGuard_.IncrementSize(size);
            }
        }
    }

    return PreparedMeta_.Acquire();
}

void TCachedVersionedChunkMeta::PrepareColumnarMeta()
{
    GetPreparedChunkMeta();
    ClearColumnMeta();

    if (MemoryTrackerGuard_) {
        MemoryTrackerGuard_.SetSize(GetMemoryUsage());
    }
}

int TCachedVersionedChunkMeta::GetChunkKeyColumnCount() const
{
    return GetChunkSchema()->GetKeyColumnCount();
}


void TCachedVersionedChunkMeta::TrackMemory(const IMemoryUsageTrackerPtr& memoryTracker)
{
    MemoryTrackerGuard_ = TMemoryUsageTrackerGuard::Acquire(
        memoryTracker,
        GetMemoryUsage());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
