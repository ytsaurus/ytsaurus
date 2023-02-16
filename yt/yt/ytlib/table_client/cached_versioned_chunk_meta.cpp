#include "cached_versioned_chunk_meta.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TCachedVersionedChunkMeta::TCachedVersionedChunkMeta(
    bool prepareColumnarMeta,
    const IMemoryUsageTrackerPtr& memoryTracker,
    const NChunkClient::NProto::TChunkMeta& chunkMeta)
    : TColumnarChunkMeta(chunkMeta)
    , ColumnarMetaPrepared_(prepareColumnarMeta && ChunkFormat_ == EChunkFormat::TableVersionedColumnar)
{
    if (ChunkType_ != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Incorrect chunk type: actual %Qlv, expected %Qlv",
            ChunkType_,
            EChunkType::Table);
    }

    if (ChunkFormat_ != EChunkFormat::TableVersionedSimple &&
        ChunkFormat_ != EChunkFormat::TableVersionedSlim &&
        ChunkFormat_ != EChunkFormat::TableVersionedColumnar &&
        ChunkFormat_ != EChunkFormat::TableVersionedIndexed &&
        ChunkFormat_ != EChunkFormat::TableUnversionedColumnar &&
        ChunkFormat_ != EChunkFormat::TableUnversionedSchemalessHorizontal)
    {
        THROW_ERROR_EXCEPTION("Incorrect chunk format %Qlv",
            ChunkFormat_);
    }

    if (auto optionalHunkChunkRefsExt = FindProtoExtension<THunkChunkRefsExt>(chunkMeta.extensions())) {
        HunkChunkRefsExt_ = std::move(*optionalHunkChunkRefsExt);
    }
    if (auto optionalHunkChunkMetasExt = FindProtoExtension<THunkChunkMetasExt>(chunkMeta.extensions())) {
        HunkChunkMetasExt_ = std::move(*optionalHunkChunkMetasExt);
    }

    if (ColumnarMetaPrepared_) {
        GetPreparedChunkMeta();
        ClearColumnMeta();
    }

    if (memoryTracker) {
        MemoryTrackerGuard_ = TMemoryUsageTrackerGuard::Acquire(
            memoryTracker,
            GetMemoryUsage());
    }
}

TCachedVersionedChunkMetaPtr TCachedVersionedChunkMeta::Create(
    bool prepareColumnarMeta,
    const IMemoryUsageTrackerPtr& memoryTracker,
    const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta)
{
    return New<TCachedVersionedChunkMeta>(
        prepareColumnarMeta,
        memoryTracker,
        *chunkMeta);
}

bool TCachedVersionedChunkMeta::IsColumnarMetaPrepared() const
{
    return ColumnarMetaPrepared_;
}

i64 TCachedVersionedChunkMeta::GetMemoryUsage() const
{
    return TColumnarChunkMeta::GetMemoryUsage()
        + HunkChunkRefsExt().SpaceUsedLong()
        + HunkChunkMetasExt().SpaceUsedLong()
        + PreparedMetaSize_;
}

TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> TCachedVersionedChunkMeta::GetPreparedChunkMeta()
{
    if (!PreparedMeta_) {
        YT_VERIFY(GetChunkFormat() == NChunkClient::EChunkFormat::TableVersionedColumnar);

        auto preparedMeta = New<NNewTableClient::TPreparedChunkMeta>();
        auto size = preparedMeta->Prepare(GetChunkSchema(), ColumnMeta());

        void* expectedPreparedMeta = nullptr;
        if (PreparedMeta_.CompareAndSwap(expectedPreparedMeta, preparedMeta)) {
            PreparedMetaSize_ = size;

            if (MemoryTrackerGuard_) {
                MemoryTrackerGuard_.IncrementSize(size);
            }
        }
    }

    return PreparedMeta_.Acquire();
}

int TCachedVersionedChunkMeta::GetChunkKeyColumnCount() const
{
    return GetChunkSchema()->GetKeyColumnCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
