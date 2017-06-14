#include "chunk_slice.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "key_set.h"

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTableClient::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TChunkSlice::TChunkSlice(
    const NProto::TSliceRequest& sliceReq,
    const NProto::TChunkMeta& meta,
    const TOwningKey& lowerKey,
    const TOwningKey& upperKey,
    TNullable<i64> dataSize,
    TNullable<i64> rowCount)
{
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta.extensions());
    DataSize_ = miscExt.uncompressed_data_size();
    RowCount_ = miscExt.row_count();

    if (rowCount && dataSize && (DataSize_ != *dataSize || RowCount_ != *rowCount)) {
        DataSize_ = *dataSize;
        RowCount_ = *rowCount;
        SizeOverridden_ = true;
    }

    if (sliceReq.has_lower_limit()) {
        LowerLimit_ = sliceReq.lower_limit();
    }

    if (sliceReq.has_upper_limit()) {
        UpperLimit_ = sliceReq.upper_limit();
    }

    if (lowerKey) {
        LowerLimit_.MergeLowerKey(lowerKey);
    }

    if (upperKey) {
        UpperLimit_.MergeUpperKey(upperKey);
    }
}

TChunkSlice::TChunkSlice(
    const TChunkSlice& chunkSlice,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : LowerLimit_(chunkSlice.LowerLimit())
    , UpperLimit_(chunkSlice.UpperLimit())
    , DataSize_(dataSize)
    , RowCount_(upperRowIndex - lowerRowIndex)
    , SizeOverridden_(true)
{
    LowerLimit_.SetRowIndex(lowerRowIndex);
    UpperLimit_.SetRowIndex(upperRowIndex);
}

TChunkSlice::TChunkSlice(
    const NProto::TSliceRequest& sliceReq,
    const NProto::TChunkMeta& meta,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : DataSize_(dataSize)
    , SizeOverridden_(true)
{
    if (sliceReq.has_lower_limit()) {
        LowerLimit_ = sliceReq.lower_limit();
    }
    LowerLimit_.MergeLowerRowIndex(lowerRowIndex);

    if (sliceReq.has_upper_limit()) {
        UpperLimit_ = sliceReq.upper_limit();
    }
    UpperLimit_.MergeUpperRowIndex(upperRowIndex);

    RowCount_ = UpperLimit_.GetRowIndex() - LowerLimit_.GetRowIndex();
}

void TChunkSlice::SliceEvenly(
    std::vector<TChunkSlice>& result,
    i64 sliceDataSize) const
{
    YCHECK(sliceDataSize > 0);

    i64 lowerRowIndex = LowerLimit_.HasRowIndex() ? LowerLimit_.GetRowIndex() : 0;
    i64 upperRowIndex = UpperLimit_.HasRowIndex() ? UpperLimit_.GetRowIndex() : RowCount_;

    i64 rowCount = upperRowIndex - lowerRowIndex;
    int count = std::max(std::min(DataSize_ / sliceDataSize, rowCount), i64(1));

    for (int i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = lowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = lowerRowIndex + rowCount * (i + 1) / count;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            result.emplace_back(
                *this,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                (DataSize_ + count - 1) / count);
        }
    }
}

void TChunkSlice::SetKeys(const NTableClient::TOwningKey& lowerKey, const NTableClient::TOwningKey& upperKey)
{
    LowerLimit_.MergeLowerKey(lowerKey);
    UpperLimit_.MergeUpperKey(upperKey);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkSlice& slice)
{
    return Format("LowerLimit: %v, UpperLimit: %v, RowCount: %v, DataSize: %v",
        slice.LowerLimit(),
        slice.UpperLimit(),
        slice.GetRowCount(),
        slice.GetDataSize());
}

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkSlicer
    : private TNonCopyable
{
public:
    TSortedChunkSlicer(const NProto::TSliceRequest& sliceReq, const NProto::TChunkMeta& meta)
        : SliceReq_(sliceReq)
        , Meta_(meta)
        , LowerLimit_(sliceReq.lower_limit())
        , UpperLimit_(sliceReq.upper_limit())
    {
        auto chunkFormat = ETableChunkFormat(Meta_.version());
        switch (chunkFormat) {
            case ETableChunkFormat::SchemalessHorizontal:
            case ETableChunkFormat::UnversionedColumnar:
            case ETableChunkFormat::VersionedSimple:
            case ETableChunkFormat::VersionedColumnar:
                break;
            default:
                auto chunkId = FromProto<TChunkId>(SliceReq_.chunk_id());
                THROW_ERROR_EXCEPTION("Unsupported format %Qlv for chunk %v",
                    ETableChunkFormat(Meta_.version()),
                    chunkId);
        }

        YCHECK(FindBoundaryKeys(Meta_, &MinKey_, &MaxKey_));

        auto miscExt = GetProtoExtension<NProto::TMiscExt>(Meta_.extensions());
        i64 chunkDataSize = miscExt.uncompressed_data_size();
        i64 chunkRowCount = miscExt.row_count();

        YCHECK(chunkRowCount > 0);

        i64 dataSizePerRow = std::max((i64)1, chunkDataSize / chunkRowCount);

        auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(Meta_.extensions());

        IndexKeys_.reserve(blockMetaExt.blocks_size() + 0);
        for (int i = 0; i < blockMetaExt.blocks_size(); ++i) {
            YCHECK(i == blockMetaExt.blocks(i).block_index());
            auto indexKey = FromProto<TOwningKey>(blockMetaExt.blocks(i).last_key());
            i64 chunkRowCount = blockMetaExt.blocks(i).chunk_row_count();
            i64 rowCount = IndexKeys_.empty()
                ? chunkRowCount
                : chunkRowCount - IndexKeys_.back().ChunkRowCount;

            IndexKeys_.push_back({
                indexKey,
                rowCount,
                chunkRowCount,
                rowCount * dataSizePerRow});
        }

        BeginIndex_ = 0;
        if (LowerLimit_.HasRowIndex() || LowerLimit_.HasKey()) {
            BeginIndex_ = std::distance(
                IndexKeys_.begin(),
                std::lower_bound(
                    IndexKeys_.begin(),
                    IndexKeys_.end(),
                    LowerLimit_,
                    [] (const TIndexKey& indexKey, const NChunkClient::TReadLimit& limit) {
                        return (limit.HasRowIndex() && indexKey.ChunkRowCount < limit.GetRowIndex())
                            || (limit.HasKey() && indexKey.Key < limit.GetKey());
                    }));
        }

        EndIndex_ = IndexKeys_.size();
        if (UpperLimit_.HasRowIndex() || UpperLimit_.HasKey()) {
            EndIndex_ = std::distance(
                IndexKeys_.begin(),
                std::upper_bound(
                    IndexKeys_.begin() + BeginIndex_,
                    IndexKeys_.end(),
                    UpperLimit_,
                    [] (const NChunkClient::TReadLimit& limit, const TIndexKey& indexKey) {
                        return (limit.HasRowIndex() && limit.GetRowIndex() < indexKey.ChunkRowCount)
                            || (limit.HasKey() && limit.GetKey() < indexKey.Key);
                    }));
        }
        if (EndIndex_ < IndexKeys_.size()) {
            ++EndIndex_;
        }
    }

    std::vector<TChunkSlice> SliceByKeys(i64 sliceDataSize, int keyColumnCount)
    {
        // Leave only key prefix.
        const auto& lowerKey = BeginIndex_ > 0 ? IndexKeys_[BeginIndex_ - 1].Key : MinKey_;
        auto lowerKeyPrefix = GetKeyPrefix(lowerKey, keyColumnCount);

        std::vector<TChunkSlice> slices;

        if (EndIndex_ - BeginIndex_ < 2) {
            // Too small distance between given read limits.
            auto upperKeyPrefix = GetKeyPrefixSuccessor(MaxKey_, keyColumnCount);
            slices.emplace_back(SliceReq_, Meta_, lowerKeyPrefix, upperKeyPrefix);
            return slices;
        }

        i64 startRowIndex = BeginIndex_ > 0 ? IndexKeys_[BeginIndex_ - 1].ChunkRowCount + 1 : 0;
        if (LowerLimit_.HasRowIndex()) {
            startRowIndex = std::max(startRowIndex, LowerLimit_.GetRowIndex());
        }

        i64 upperRowIndex = IndexKeys_[EndIndex_ - 1].ChunkRowCount;
        if (UpperLimit_.HasRowIndex()) {
            upperRowIndex = std::min(upperRowIndex, UpperLimit_.GetRowIndex());
        }

        i64 dataSize = 0;
        i64 sliceRowCount = 0;
        for (i64 currentIndex = BeginIndex_; currentIndex < EndIndex_; ++currentIndex) {
            i64 rowCount = IndexKeys_[currentIndex].RowCount;
            if (startRowIndex > IndexKeys_[currentIndex].ChunkRowCount - IndexKeys_[currentIndex].RowCount) {
                rowCount = std::max(IndexKeys_[currentIndex].ChunkRowCount - startRowIndex, i64(0));
            }
            if (upperRowIndex < IndexKeys_[currentIndex].ChunkRowCount) {
                rowCount = std::max(rowCount - (IndexKeys_[currentIndex].ChunkRowCount - upperRowIndex), i64(0));
            }
            if (rowCount != IndexKeys_[currentIndex].RowCount) {
                i64 dataPerRow = IndexKeys_[currentIndex].DataSize / IndexKeys_[currentIndex].RowCount;
                dataSize += rowCount * dataPerRow;
            } else {
                dataSize += IndexKeys_[currentIndex].DataSize;
            }
            sliceRowCount += rowCount;

            const auto& key = IndexKeys_[currentIndex].Key;

            // Wait until some key to split
            if (currentIndex < EndIndex_ - 1) {
                const auto& nextIndexKey = IndexKeys_[currentIndex + 1].Key;
                if (CompareRows(key, lowerKeyPrefix, keyColumnCount) == 0 ||
                    CompareRows(nextIndexKey, key, keyColumnCount) == 0)
                {
                    continue;
                }
            }

            if (dataSize > sliceDataSize || currentIndex == EndIndex_ - 1) {
                YCHECK(CompareRows(lowerKeyPrefix, key) <= 0);

                auto upperKeyPrefix = GetKeyPrefixSuccessor(key, keyColumnCount);
                slices.emplace_back(SliceReq_, Meta_, lowerKeyPrefix, upperKeyPrefix, dataSize, sliceRowCount);

                lowerKeyPrefix = upperKeyPrefix;
                startRowIndex = IndexKeys_[currentIndex].ChunkRowCount;
                dataSize = 0;
                sliceRowCount = 0;
            }
        }
        return slices;
    }

    // Slice by rows with keys estimates.
    std::vector<TChunkSlice> SliceByRows(i64 sliceDataSize, int keyColumnCount)
    {
        // Leave only key prefix.
        const auto& lowerKey = BeginIndex_ > 0 ? IndexKeys_[BeginIndex_ - 1].Key : MinKey_;
        auto lowerKeyPrefix = GetKeyPrefix(lowerKey, keyColumnCount);

        std::vector<TChunkSlice> slices;

        if (EndIndex_ - BeginIndex_ < 2) {
            // Too small distance between given read limits.
            auto upperKeyPrefix = GetKeyPrefixSuccessor(MaxKey_, keyColumnCount);
            slices.emplace_back(SliceReq_, Meta_, lowerKeyPrefix, upperKeyPrefix);
            return slices;
        }

        i64 startRowIndex = BeginIndex_ > 0 ? IndexKeys_[BeginIndex_ - 1].ChunkRowCount + 1 : 0;
        if (LowerLimit_.HasRowIndex()) {
            startRowIndex = std::max(startRowIndex, LowerLimit_.GetRowIndex());
        }
        i64 upperRowIndex = IndexKeys_[EndIndex_ - 1].ChunkRowCount;
        if (UpperLimit_.HasRowIndex()) {
            upperRowIndex = std::min(upperRowIndex, UpperLimit_.GetRowIndex());
        }
        i64 dataSize = 0;
        i64 sliceRowCount = 0;

        for (i64 currentIndex = BeginIndex_; currentIndex < EndIndex_; ++currentIndex) {
            i64 rowCount = IndexKeys_[currentIndex].RowCount;
            if (startRowIndex > IndexKeys_[currentIndex].ChunkRowCount - IndexKeys_[currentIndex].RowCount) {
                rowCount = std::max(IndexKeys_[currentIndex].ChunkRowCount - startRowIndex, i64(0));
            }
            if (upperRowIndex < IndexKeys_[currentIndex].ChunkRowCount) {
                rowCount = std::max(rowCount - (IndexKeys_[currentIndex].ChunkRowCount - upperRowIndex), i64(0));
            }
            if (rowCount != IndexKeys_[currentIndex].RowCount) {
                i64 dataPerRow = IndexKeys_[currentIndex].DataSize / IndexKeys_[currentIndex].RowCount;
                dataSize += rowCount * dataPerRow;
            } else {
                dataSize += IndexKeys_[currentIndex].DataSize;
            }
            sliceRowCount += rowCount;

            const auto& key = IndexKeys_[currentIndex].Key;

            if (dataSize > sliceDataSize || currentIndex == EndIndex_ - 1) {
                YCHECK(CompareRows(lowerKeyPrefix, key) <= 0);

                auto upperKeyPrefix = GetKeyPrefixSuccessor(key, keyColumnCount);

                auto slice = TChunkSlice(SliceReq_, Meta_, startRowIndex, startRowIndex + sliceRowCount, dataSize);
                slice.SetKeys(lowerKeyPrefix, upperKeyPrefix);
                slice.SliceEvenly(slices, sliceDataSize);

                lowerKeyPrefix = GetKeyPrefix(key, keyColumnCount);
                startRowIndex = IndexKeys_[currentIndex].ChunkRowCount;
                dataSize = 0;
                sliceRowCount = 0;
            }
        }
        return slices;
    }

private:
    struct TIndexKey
    {
        TOwningKey Key;
        i64 RowCount;
        i64 ChunkRowCount;
        i64 DataSize;
    };

    const NProto::TSliceRequest& SliceReq_;
    const NProto::TChunkMeta& Meta_;
    NChunkClient::TReadLimit LowerLimit_;
    NChunkClient::TReadLimit UpperLimit_;

    TOwningKey MinKey_;
    TOwningKey MaxKey_;
    std::vector<TIndexKey> IndexKeys_;
    i64 BeginIndex_ = 0;
    i64 EndIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlice> SliceChunk(
    const NProto::TSliceRequest& sliceReq,
    const NProto::TChunkMeta& meta,
    i64 sliceDataSize,
    int keyColumnCount,
    bool sliceByKeys)
{
    TSortedChunkSlicer slicer(sliceReq, meta);
    if (sliceByKeys) {
        return slicer.SliceByKeys(sliceDataSize, keyColumnCount);
    } else {
        return slicer.SliceByRows(sliceDataSize, keyColumnCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkSlice* protoChunkSlice, const TChunkSlice& chunkSlice)
{
    if (!IsTrivial(chunkSlice.LowerLimit())) {
        ToProto(protoChunkSlice->mutable_lower_limit(), chunkSlice.LowerLimit());
    }
    if (!IsTrivial(chunkSlice.UpperLimit())) {
        ToProto(protoChunkSlice->mutable_upper_limit(), chunkSlice.UpperLimit());
    }
    if (chunkSlice.GetSizeOverridden()) {
        protoChunkSlice->set_uncompressed_data_size_override(chunkSlice.GetDataSize());
        protoChunkSlice->set_row_count_override(chunkSlice.GetRowCount());
    }
}

void ToProto(
    const TKeySetWriterPtr& keysWriter,
    NProto::TChunkSlice* protoChunkSlice,
    const TChunkSlice& chunkSlice)
{
    if (chunkSlice.LowerLimit().HasKey()) {
        int index = keysWriter->WriteKey(chunkSlice.LowerLimit().GetKey());
        protoChunkSlice->mutable_lower_limit()->set_key_index(index);
    }

    if (chunkSlice.LowerLimit().HasRowIndex()) {
        protoChunkSlice->mutable_lower_limit()->set_row_index(chunkSlice.LowerLimit().GetRowIndex());
    }

    if (chunkSlice.UpperLimit().HasKey()) {
        int index = keysWriter->WriteKey(chunkSlice.UpperLimit().GetKey());
        protoChunkSlice->mutable_upper_limit()->set_key_index(index);
    }

    if (chunkSlice.UpperLimit().HasRowIndex()) {
        protoChunkSlice->mutable_upper_limit()->set_row_index(chunkSlice.UpperLimit().GetRowIndex());
    }

    if (chunkSlice.GetSizeOverridden()) {
        protoChunkSlice->set_uncompressed_data_size_override(chunkSlice.GetDataSize());
        protoChunkSlice->set_row_count_override(chunkSlice.GetRowCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
