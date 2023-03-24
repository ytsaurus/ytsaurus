#include "cached_versioned_chunk_meta.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

THashTableChunkIndexMeta::THashTableChunkIndexMeta(const TTableSchemaPtr& schema)
    : IndexedBlockFormatDetail(schema)
{ }

THashTableChunkIndexMeta::TChunkIndexBlockMeta::TChunkIndexBlockMeta(
    int blockIndex,
    const TIndexedVersionedBlockFormatDetail& indexedBlockFormatDetail,
    const NProto::THashTableChunkIndexSystemBlockMeta& hashTableChunkIndexSystemBlockMetaExt)
    : BlockIndex(blockIndex)
    , FormatDetail(
        hashTableChunkIndexSystemBlockMetaExt.seed(),
        hashTableChunkIndexSystemBlockMetaExt.slot_count(),
        indexedBlockFormatDetail.GetGroupCount(),
        /*groupReorderingEnabled*/ false)
    , BlockLastKey(FromProto<TLegacyOwningKey>(hashTableChunkIndexSystemBlockMetaExt.last_key()))
{ }

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


    if (auto optionalSystemBlockMetaExt = FindProtoExtension<TSystemBlockMetaExt>(chunkMeta.extensions())) {
        ParseHashTableChunkIndexMeta(*optionalSystemBlockMetaExt);
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

TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> TCachedVersionedChunkMeta::GetPreparedChunkMeta(NNewTableClient::IBlockDataProvider* blockProvider)
{
    auto currentMeta = PreparedMeta_.Acquire();
    TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> newPreparedMeta = nullptr;
    while (!currentMeta || (blockProvider && !currentMeta->FullNewMeta)) {
        if (!newPreparedMeta) {
            newPreparedMeta = New<NNewTableClient::TPreparedChunkMeta>();
            newPreparedMeta->FullNewMeta = blockProvider;
            newPreparedMeta->Prepare(GetChunkSchema(), ColumnMeta(), DataBlockMeta(), blockProvider);
        }

        void* rawCurrentMeta = currentMeta.Get();
        if (PreparedMeta_.CompareAndSwap(rawCurrentMeta, newPreparedMeta)) {
            if (MemoryTrackerGuard_) {
                MemoryTrackerGuard_.IncrementSize(newPreparedMeta->Size);
                if (currentMeta) {
                    MemoryTrackerGuard_.IncrementSize(-currentMeta->Size);
                }
            }
            currentMeta = newPreparedMeta;
            break;
        } else {
            currentMeta = PreparedMeta_.Acquire();
        }
    }

    return currentMeta;
}

int TCachedVersionedChunkMeta::GetChunkKeyColumnCount() const
{
    return GetChunkSchema()->GetKeyColumnCount();
}

void TCachedVersionedChunkMeta::ParseHashTableChunkIndexMeta(
    const TSystemBlockMetaExt& systemBlockMetaExt)
{
    std::vector<std::pair<int, THashTableChunkIndexSystemBlockMeta>> blockMetas;

    for (int blockIndex = 0; blockIndex < systemBlockMetaExt.system_blocks_size(); ++blockIndex) {
        const auto& systemBlockMeta = systemBlockMetaExt.system_blocks(blockIndex);
        if (systemBlockMeta.HasExtension(THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext)) {
            blockMetas.emplace_back(
                DataBlockMeta()->data_blocks_size() + blockIndex,
                systemBlockMeta.GetExtension(
                    THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext));
        }
    }

    if (blockMetas.empty()) {
        return;
    }

    HashTableChunkIndexMeta_.emplace(GetChunkSchema());
    HashTableChunkIndexMeta_->ChunkIndexBlockMetas.reserve(blockMetas.size());
    for (const auto& [blockIndex, blockMeta] : blockMetas) {
        HashTableChunkIndexMeta_->ChunkIndexBlockMetas.emplace_back(
            blockIndex,
            HashTableChunkIndexMeta_->IndexedBlockFormatDetail,
            blockMeta);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
