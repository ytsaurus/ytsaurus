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

THashTableChunkIndexMeta::TBlockMeta::TBlockMeta(
    int blockIndex,
    const TIndexedVersionedBlockFormatDetail& indexedBlockFormatDetail,
    const THashTableChunkIndexSystemBlockMeta& hashTableChunkIndexSystemBlockMetaExt)
    : BlockIndex(blockIndex)
    , FormatDetail(
        hashTableChunkIndexSystemBlockMetaExt.seed(),
        hashTableChunkIndexSystemBlockMetaExt.slot_count(),
        indexedBlockFormatDetail.GetGroupCount(),
        /*groupReorderingEnabled*/ false)
    , BlockLastKey(FromProto<TLegacyOwningKey>(hashTableChunkIndexSystemBlockMetaExt.last_key()))
{ }

i64 THashTableChunkIndexMeta::GetMemoryUsage() const
{
    return sizeof(IndexedBlockFormatDetail) + sizeof(BlockMetas[0]) * BlockMetas.size();
}

TXorFilterMeta::TBlockMeta::TBlockMeta(
    int blockIndex,
    const TXorFilterSystemBlockMeta& xorFilterSystemBlockMetaExt)
    : BlockIndex(blockIndex)
    , BlockLastKey(FromProto<TLegacyOwningKey>(xorFilterSystemBlockMetaExt.last_key()))
{ }

i64 TXorFilterMeta::GetMemoryUsage() const
{
    return sizeof(KeyPrefixLength) + sizeof(BlockMetas[0]) * BlockMetas.size();
}

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
        ParseXorFilterMeta(*optionalSystemBlockMetaExt);
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
    i64 xorFilterMetaSize = 0;
    for (const auto& [_, xorFilterMeta] : XorFilterMetaByLength_) {
        xorFilterMetaSize += xorFilterMeta.GetMemoryUsage();
    }

    return TColumnarChunkMeta::GetMemoryUsage()
        + PreparedMetaSize_.load()
        + (HashTableChunkIndexMeta_ ? HashTableChunkIndexMeta_->GetMemoryUsage() : 0)
        + xorFilterMetaSize;
}

TIntrusivePtr<NColumnarChunkFormat::TPreparedChunkMeta> TCachedVersionedChunkMeta::GetPreparedChunkMeta(
    NColumnarChunkFormat::IBlockDataProvider* blockProvider)
{
    auto currentMeta = PreparedMeta_.Acquire();
    TIntrusivePtr<NColumnarChunkFormat::TPreparedChunkMeta> newPreparedMeta = nullptr;
    while (!currentMeta || (blockProvider && !currentMeta->FullNewMeta)) {
        if (!newPreparedMeta) {
            if (ColumnGroupInfos()) {
                newPreparedMeta = NColumnarChunkFormat::TPreparedChunkMeta::FromSegmentMetasStoredInBlocks(
                    ColumnGroupInfos(),
                    DataBlockMeta());

                auto fromProtoMeta = NColumnarChunkFormat::TPreparedChunkMeta::FromProtoSegmentMetas(
                    ChunkSchema_,
                    ColumnMeta(),
                    DataBlockMeta(),
                    blockProvider);

                NColumnarChunkFormat::TPreparedChunkMeta::VerifyEquality(
                    *fromProtoMeta,
                    *newPreparedMeta,
                    DataBlockMeta());
            } else {
                newPreparedMeta = NColumnarChunkFormat::TPreparedChunkMeta::FromProtoSegmentMetas(
                    ChunkSchema_,
                    ColumnMeta(),
                    DataBlockMeta(),
                    blockProvider);
            }
        }

        void* rawCurrentMeta = currentMeta.Get();
        if (PreparedMeta_.CompareAndSwap(rawCurrentMeta, newPreparedMeta)) {
            PreparedMetaSize_.store(newPreparedMeta->Size);
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
    return ChunkSchema_->GetKeyColumnCount();
}

const TXorFilterMeta* TCachedVersionedChunkMeta::FindXorFilterByLength(int keyPrefixLength) const
{
    if (XorFilterMetaByLength_.empty()) {
        return nullptr;
    }

    auto it = XorFilterMetaByLength_.upper_bound(keyPrefixLength);
    if (it == XorFilterMetaByLength_.begin()) {
        return nullptr;
    }
    return &(--it)->second;
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

    HashTableChunkIndexMeta_.emplace(ChunkSchema_);
    HashTableChunkIndexMeta_->BlockMetas.reserve(blockMetas.size());
    for (const auto& [blockIndex, blockMeta] : blockMetas) {
        HashTableChunkIndexMeta_->BlockMetas.emplace_back(
            blockIndex,
            HashTableChunkIndexMeta_->IndexedBlockFormatDetail,
            blockMeta);
    }
}

void TCachedVersionedChunkMeta::ParseXorFilterMeta(
    const TSystemBlockMetaExt& systemBlockMetaExt)
{
    for (int blockIndex = 0; blockIndex < systemBlockMetaExt.system_blocks_size(); ++blockIndex) {
        const auto& systemBlockMeta = systemBlockMetaExt.system_blocks(blockIndex);
        if (systemBlockMeta.HasExtension(TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext)) {
            auto xorFilterBlockExt = systemBlockMeta.GetExtension(
                TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext);

            auto keyPrefixLength = xorFilterBlockExt.has_key_prefix_length()
                ? xorFilterBlockExt.key_prefix_length()
                : GetChunkKeyColumnCount();

            auto& xorFilterMeta = XorFilterMetaByLength_[keyPrefixLength];
            xorFilterMeta.KeyPrefixLength = keyPrefixLength;
            xorFilterMeta.BlockMetas.emplace_back(
                DataBlockMeta()->data_blocks_size() + blockIndex,
                xorFilterBlockExt);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
