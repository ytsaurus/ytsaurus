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

constinit const auto Logger = TableClientLogger;

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
        ParseSystemBlocksMeta(*optionalSystemBlockMetaExt);
    }

    if (ColumnarMetaPrepared_) {
        GetPreparedChunkMeta();
        ColumnMeta_.Reset();
        DataBlockMeta_.Reset();
    }

    // Do not keep large columnar statistics
    LargeColumnarStatisticsExt_.reset();

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

                if (ColumnMeta()) {
                    auto fromProtoMeta = NColumnarChunkFormat::TPreparedChunkMeta::FromProtoSegmentMetas(
                        ChunkSchema_,
                        ColumnMeta(),
                        DataBlockMeta(),
                        blockProvider);

                    NColumnarChunkFormat::TPreparedChunkMeta::VerifyEquality(
                        *fromProtoMeta,
                        *newPreparedMeta,
                        DataBlockMeta());
                }
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
                MemoryTrackerGuard_.IncreaseSize(newPreparedMeta->Size);
                if (currentMeta) {
                    MemoryTrackerGuard_.IncreaseSize(-currentMeta->Size);
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

void TCachedVersionedChunkMeta::ParseSystemBlocksMeta(const NProto::TSystemBlockMetaExt& systemBlockMetaExt)
{
    std::vector<std::pair<int, THashTableChunkIndexSystemBlockMeta>> hashTableChunkIndexBlockMetas;

    for (int systemBlockIndex = 0; systemBlockIndex < systemBlockMetaExt.system_blocks_size(); ++systemBlockIndex) {
        const auto& systemBlockMeta = systemBlockMetaExt.system_blocks(systemBlockIndex);
        int blockIndex = DataBlockMeta()->data_blocks_size() + systemBlockIndex;

        if (systemBlockMeta.HasExtension(THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext)) {
            hashTableChunkIndexBlockMetas.emplace_back(
                blockIndex,
                systemBlockMeta.GetExtension(
                    THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext));
        } else if (systemBlockMeta.HasExtension(TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext)) {
            auto xorFilterBlockExt = systemBlockMeta.GetExtension(
                TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext);

            auto keyPrefixLength = xorFilterBlockExt.has_key_prefix_length()
                ? xorFilterBlockExt.key_prefix_length()
                : GetChunkKeyColumnCount();

            auto& xorFilterMeta = XorFilterMetaByLength_[keyPrefixLength];
            xorFilterMeta.KeyPrefixLength = keyPrefixLength;
            xorFilterMeta.BlockMetas.emplace_back(
                blockIndex,
                xorFilterBlockExt);
        } else if (systemBlockMeta.HasExtension(TMinHashDigestSystemBlockMeta::min_hash_digest_block_meta)) {
            if (MinHashDigestBlockIndex_) {
                YT_LOG_ALERT("There are two blocks with min hash digest (FirstBlockIndex: %v, SecondBlockIndex: %v)",
                    *MinHashDigestBlockIndex_,
                    blockIndex);
            }

            MinHashDigestBlockIndex_ = blockIndex;
        }
    }

    if (hashTableChunkIndexBlockMetas.empty()) {
        return;
    }

    HashTableChunkIndexMeta_.emplace(ChunkSchema_);
    HashTableChunkIndexMeta_->BlockMetas.reserve(hashTableChunkIndexBlockMetas.size());
    for (const auto& [blockIndex, blockMeta] : hashTableChunkIndexBlockMetas) {
        HashTableChunkIndexMeta_->BlockMetas.emplace_back(
            blockIndex,
            HashTableChunkIndexMeta_->IndexedBlockFormatDetail,
            blockMeta);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
