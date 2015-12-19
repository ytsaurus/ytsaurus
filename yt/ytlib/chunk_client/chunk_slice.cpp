#include "chunk_slice.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "schema.h"

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTableClient::NProto;

using NProto::TSizeOverrideExt;

////////////////////////////////////////////////////////////////////////////////

const int DefaultPartIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TChunkSlice::TChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
    : ChunkSpec_(chunkSpec)
{
    GetStatistics(*chunkSpec, &DataSize_, &RowCount_);

    if (ChunkSpec_->has_lower_limit()) {
        LowerLimit_ = ChunkSpec_->lower_limit();
    }

    if (ChunkSpec_->has_upper_limit()) {
        UpperLimit_ = ChunkSpec_->upper_limit();
    }

    if (lowerKey) {
        LowerLimit_.MergeLowerKey(*lowerKey);
    }

    if (upperKey) {
        UpperLimit_.MergeUpperKey(*upperKey);
    }
}

TChunkSlice::TChunkSlice(
    const TIntrusivePtr<const TChunkSlice>& chunkSlice,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
    : ChunkSpec_(chunkSlice->GetChunkSpec())
    , LowerLimit_(chunkSlice->LowerLimit())
    , UpperLimit_(chunkSlice->UpperLimit())
    , PartIndex_(chunkSlice->GetPartIndex())
    , SizeOverridden_(chunkSlice->GetSizeOverridden())
    , RowCount_(chunkSlice->GetRowCount())
    , DataSize_(chunkSlice->GetDataSize())
{
    if (lowerKey) {
        LowerLimit_.MergeLowerKey(*lowerKey);
    }

    if (upperKey) {
        UpperLimit_.MergeUpperKey(*upperKey);
    }
}

TChunkSlice::TChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    int partIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : ChunkSpec_(std::move(chunkSpec))
    , PartIndex_(partIndex)
{
    if (ChunkSpec_->has_lower_limit()) {
        LowerLimit_ = ChunkSpec_->lower_limit();
    }

    if (ChunkSpec_->has_upper_limit()) {
        UpperLimit_ = ChunkSpec_->upper_limit();
    }

    LowerLimit_.MergeLowerRowIndex(lowerRowIndex);
    UpperLimit_.MergeUpperRowIndex(upperRowIndex);
    
    SetRowCount(UpperLimit_.GetRowIndex() - LowerLimit_.GetRowIndex());
    SetDataSize(dataSize);
}

TChunkSlice::TChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const NProto::TChunkSlice& protoChunkSlice)
    : TChunkSlice(chunkSpec, Null, Null)
{
    LowerLimit_.MergeLowerLimit(protoChunkSlice.lower_limit());
    UpperLimit_.MergeUpperLimit(protoChunkSlice.upper_limit());
    PartIndex_ = protoChunkSlice.part_index();

    if (protoChunkSlice.has_size_override_ext()) {
        SetRowCount(protoChunkSlice.size_override_ext().row_count());
        SetDataSize(protoChunkSlice.size_override_ext().uncompressed_data_size());
    }
}

std::vector<TChunkSlicePtr> TChunkSlice::SliceEvenly(i64 sliceDataSize) const
{
    std::vector<TChunkSlicePtr> result;

    YCHECK(sliceDataSize > 0);

    i64 rowCount = GetRowCount();

    i64 lowerRowIndex = LowerLimit_.HasRowIndex() ? LowerLimit_.GetRowIndex() : 0;
    i64 upperRowIndex = UpperLimit_.HasRowIndex() ? UpperLimit_.GetRowIndex() : rowCount;

    rowCount = upperRowIndex - lowerRowIndex;
    int count = std::max(std::min(GetDataSize() / sliceDataSize, rowCount), i64(1));

    for (int i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = lowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = lowerRowIndex + rowCount * (i + 1) / count;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto chunkSlice = New<TChunkSlice>(this);
            chunkSlice->LowerLimit_.SetRowIndex(sliceLowerRowIndex);
            chunkSlice->UpperLimit_.SetRowIndex(sliceUpperRowIndex);
            chunkSlice->SetRowCount(sliceUpperRowIndex - sliceLowerRowIndex);
            chunkSlice->SetDataSize((GetDataSize() + count - 1) / count);
            result.emplace_back(std::move(chunkSlice));
        }
    }

    return result;
}

i64 TChunkSlice::GetLocality(int replicaPartIndex) const
{
    i64 result = GetDataSize();

    if (PartIndex_ == DefaultPartIndex) {
        // For erasure chunks without specified part index,
        // data size is assumed to be split evenly between data parts.
        auto codecId = NErasure::ECodec(ChunkSpec_->erasure_codec());
        if (codecId != NErasure::ECodec::None) {
            auto* codec = NErasure::GetCodec(codecId);
            int dataPartCount = codec->GetDataPartCount();
            result = (result + dataPartCount - 1) / dataPartCount;
        }
    } else if (PartIndex_ != replicaPartIndex) {
        result = 0;
    }

    return result;
}

int TChunkSlice::GetPartIndex() const
{
    return PartIndex_;
}

i64 TChunkSlice::GetMaxBlockSize() const
{
    auto miscExt = GetProtoExtension<NProto::TMiscExt>(ChunkSpec_->chunk_meta().extensions());
    return miscExt.max_block_size();
}

bool TChunkSlice::GetSizeOverridden() const
{
    return SizeOverridden_;
}

i64 TChunkSlice::GetDataSize() const
{
    return DataSize_;
}

i64 TChunkSlice::GetRowCount() const
{
    return RowCount_;
}

void TChunkSlice::SetDataSize(i64 dataSize)
{
    DataSize_ = dataSize;
    SizeOverridden_ = true;
}

void TChunkSlice::SetRowCount(i64 rowCount)
{
    RowCount_ = rowCount;
    SizeOverridden_ = true;
}

void TChunkSlice::SetKeys(const NTableClient::TOwningKey& lowerKey, const NTableClient::TOwningKey& upperKey)
{
    LowerLimit_.MergeLowerKey(lowerKey);
    UpperLimit_.MergeUpperKey(upperKey);
}

void TChunkSlice::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkSpec_);
    Persist(context, PartIndex_);
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, RowCount_);
    Persist(context, DataSize_);
    Persist(context, SizeOverridden_);
}

////////////////////////////////////////////////////////////////////////////////

size_t SpaceUsed(const TChunkSlicePtr chunkSlice)
{
    return sizeof(*chunkSlice) + 
        chunkSlice->LowerLimit_.SpaceUsed() - sizeof(chunkSlice->LowerLimit_) +
        chunkSlice->UpperLimit_.SpaceUsed() - sizeof(chunkSlice->UpperLimit_);
}

Stroka ToString(TChunkSlicePtr slice)
{
    return Format(
        "LowerLimit: {%v}, UpperLimit: {%v}, RowCount: %v, DataSize: %v, PartIndex: %v",
        ToString(slice->LowerLimit()),
        ToString(slice->UpperLimit()),
        slice->GetRowCount(),
        slice->GetDataSize(),
        slice->GetPartIndex());
}

////////////////////////////////////////////////////////////////////////////////

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
{
    return New<TChunkSlice>(chunkSpec, lowerKey, upperKey);
}

TChunkSlicePtr CreateChunkSlice(
    TChunkSlicePtr chunkSlice,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
{
    return New<TChunkSlice>(chunkSlice, lowerKey, upperKey);
}

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const NProto::TChunkSlice& protoChunkSlice)
{
    return New<TChunkSlice>(chunkSpec, protoChunkSlice);
}

std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId)
{
    std::vector<TChunkSlicePtr> slices;

    i64 dataSize;
    i64 rowCount;
    GetStatistics(*chunkSpec, &dataSize, &rowCount);

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceLowerRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceUpperRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto chunkSlice = New<TChunkSlice>(
                chunkSpec,
                partIndex,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                (dataSize + dataPartCount - 1) / dataPartCount);
            slices.emplace_back(std::move(chunkSlice));
        }
    }

    return slices;
}

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkSlicer
    : private TNonCopyable
{
public:
    TSortedChunkSlicer(TRefCountedChunkSpecPtr chunkSpec)
        : ChunkSpec_(chunkSpec)
        , LowerLimit_(ChunkSpec_->lower_limit())
        , UpperLimit_(ChunkSpec_->upper_limit())
    {
        const auto& meta = ChunkSpec_->chunk_meta();

        auto chunkFormat = ETableChunkFormat(meta.version());
        switch (chunkFormat) {
            case ETableChunkFormat::Old:
            case ETableChunkFormat::SchemalessHorizontal:
            case ETableChunkFormat::VersionedSimple:
                break;
            default:
                auto chunkId = NYT::FromProto<TChunkId>(ChunkSpec_->chunk_id());
                THROW_ERROR_EXCEPTION("Unsupported format %Qlv for chunk %v",
                    ETableChunkFormat(meta.version()),
                    chunkId);
        }

        YCHECK(TryGetBoundaryKeys(meta, &MinKey_, &MaxKey_));

        auto miscExt = GetProtoExtension<NProto::TMiscExt>(meta.extensions());
        i64 chunkDataSize = miscExt.uncompressed_data_size();
        i64 chunkRowCount = miscExt.row_count();

        YCHECK(chunkRowCount > 0);

        if (chunkFormat == ETableChunkFormat::Old) {
            auto indexExt = GetProtoExtension<TIndexExt>(meta.extensions());

            i64 dataSizePerRow = chunkDataSize / chunkRowCount;
            YCHECK(dataSizePerRow > 0);

            IndexKeys_.reserve(indexExt.items_size() + 1);
            for (int i = 0; i < indexExt.items_size(); ++i) {
                TOwningKey indexKey;
                FromProto(&indexKey, indexExt.items(i).key());
                i64 rowCount = indexExt.items(i).row_index() + 1;
                if (i != 0) {
                    rowCount -= IndexKeys_.back().ChunkRowCount;
                }
                IndexKeys_.push_back({
                    indexKey,
                    rowCount,
                    indexExt.items(i).row_index() + 1,
                    rowCount * dataSizePerRow});
            }
            i64 rowCount = chunkRowCount;
            if (!IndexKeys_.empty()) {
                rowCount -= IndexKeys_.back().ChunkRowCount;
            }
            IndexKeys_.push_back({
                MaxKey_,
                rowCount,
                chunkRowCount,
                rowCount * dataSizePerRow});
        } else {
            auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(meta.extensions());

            IndexKeys_.reserve(blockMetaExt.blocks_size() + 0);
            for (int i = 0; i < blockMetaExt.blocks_size(); ++i) {
                YCHECK(i == blockMetaExt.blocks(i).block_index());
                TOwningKey indexKey;
                FromProto(&indexKey, blockMetaExt.blocks(i).last_key());
                IndexKeys_.push_back({
                    indexKey,
                    blockMetaExt.blocks(i).row_count(),
                    blockMetaExt.blocks(i).chunk_row_count(),
                    blockMetaExt.blocks(i).uncompressed_size()});
            }
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

    std::vector<TChunkSlicePtr> SliceByKeys(i64 sliceDataSize, int keyColumnCount)
    {
        // Leave only key prefix.
        const auto& lowerKey = BeginIndex_ > 0 ? IndexKeys_[BeginIndex_ - 1].Key : MinKey_;
        auto lowerKeyPrefix = GetKeyPrefix(lowerKey.Get(), keyColumnCount);

        if (EndIndex_ - BeginIndex_ < 2) {
            // Too small distance between given read limits.
            const auto upperKeyPrefix = GetKeyPrefixSuccessor(MaxKey_.Get(), keyColumnCount);
            return { New<TChunkSlice>(ChunkSpec_, lowerKeyPrefix, upperKeyPrefix) };
        }

        std::vector<TChunkSlicePtr> slices;

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
                if (CompareRows(nextIndexKey, key, keyColumnCount) == 0) {
                    continue;
                }
            }

            if (dataSize > sliceDataSize || currentIndex == EndIndex_ - 1) {
                YCHECK(CompareRows(lowerKeyPrefix, key) <= 0);

                auto upperKeyPrefix = GetKeyPrefixSuccessor(key.Get(), keyColumnCount);
                auto slice = New<TChunkSlice>(ChunkSpec_, lowerKeyPrefix, upperKeyPrefix);
                slice->SetRowCount(sliceRowCount);
                slice->SetDataSize(dataSize);
                slices.emplace_back(std::move(slice));

                lowerKeyPrefix = upperKeyPrefix;
                startRowIndex = IndexKeys_[currentIndex].ChunkRowCount;
                dataSize = 0;
                sliceRowCount = 0;
            }
        }
        return slices;
    }

    // Slice by rows with keys estimates.
    std::vector<TChunkSlicePtr> SliceByRows(i64 sliceDataSize, int keyColumnCount)
    {
        // Leave only key prefix.
        const auto& lowerKey = BeginIndex_ > 0 ? IndexKeys_[BeginIndex_ - 1].Key : MinKey_;
        auto lowerKeyPrefix = GetKeyPrefix(lowerKey.Get(), keyColumnCount);

        if (EndIndex_ - BeginIndex_ < 2) {
            // Too small distance between given read limits.
            const auto upperKeyPrefix = GetKeyPrefixSuccessor(MaxKey_.Get(), keyColumnCount);
            return { New<TChunkSlice>(ChunkSpec_, lowerKeyPrefix, upperKeyPrefix) };
        }

        std::vector<TChunkSlicePtr> slices;

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

                auto upperKeyPrefix = GetKeyPrefixSuccessor(key.Get(), keyColumnCount);
                auto slice = New<TChunkSlice>(ChunkSpec_, DefaultPartIndex, startRowIndex, startRowIndex + sliceRowCount, dataSize);
                slice->SetKeys(lowerKeyPrefix, upperKeyPrefix);
                if (dataSize >= 2 * sliceDataSize) {
                    auto subslices = slice->SliceEvenly(sliceDataSize);
                    slices.insert(slices.end(), subslices.begin(), subslices.end());
                } else {
                    slices.emplace_back(std::move(slice));
                }
                lowerKeyPrefix = GetKeyPrefix(key.Get(), keyColumnCount);
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

    TRefCountedChunkSpecPtr ChunkSpec_;
    NChunkClient::TReadLimit LowerLimit_;
    NChunkClient::TReadLimit UpperLimit_;

    TOwningKey MinKey_;
    TOwningKey MaxKey_;
    std::vector<TIndexKey> IndexKeys_;
    i64 BeginIndex_ = 0;
    i64 EndIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlicePtr> SliceChunk(
    TRefCountedChunkSpecPtr chunkSpec,
    i64 sliceDataSize,
    int keyColumnCount,
    bool sliceByKeys)
{
    TSortedChunkSlicer slicer(chunkSpec);
    if (sliceByKeys) {
        return slicer.SliceByKeys(sliceDataSize, keyColumnCount);
    } else {
        return slicer.SliceByRows(sliceDataSize, keyColumnCount);
    }
}

std::vector<TChunkSlicePtr> SliceChunkByRowIndexes(TRefCountedChunkSpecPtr chunkSpec, i64 sliceDataSize)
{
    return CreateChunkSlice(chunkSpec)->SliceEvenly(sliceDataSize);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlicePtr& chunkSlice)
{   
    // The chunk spec in the slice has arrived from master, so it can't possibly contain any extensions 
    // except misc and boundary keys (in sorted merge or reduce). Jobs request boundary keys
    // from the nodes when needed, so we remove it here, to optimize traffic from the scheduler and
    // proto serialization time.

    chunkSpec->CopyFrom(*chunkSlice->GetChunkSpec());
    RemoveProtoExtension<TBoundaryKeysExt>(chunkSpec->mutable_chunk_meta()->mutable_extensions());
    RemoveProtoExtension<TOldBoundaryKeysExt>(chunkSpec->mutable_chunk_meta()->mutable_extensions());

    if (IsNontrivial(chunkSlice->LowerLimit())) {
        ToProto(chunkSpec->mutable_lower_limit(), chunkSlice->LowerLimit());
    }

    if (IsNontrivial(chunkSlice->UpperLimit())) {
        ToProto(chunkSpec->mutable_upper_limit(), chunkSlice->UpperLimit());
    }

    if (chunkSlice->GetSizeOverridden()) {
        TSizeOverrideExt sizeOverrideExt;
        sizeOverrideExt.set_uncompressed_data_size(chunkSlice->GetDataSize());
        sizeOverrideExt.set_row_count(chunkSlice->GetRowCount());
        SetProtoExtension(chunkSpec->mutable_chunk_meta()->mutable_extensions(), sizeOverrideExt);
    }
}

void ToProto(NProto::TChunkSlice* protoChunkSlice, const TChunkSlicePtr& chunkSlice)
{
    protoChunkSlice->set_part_index(chunkSlice->GetPartIndex());
    if (IsNontrivial(chunkSlice->LowerLimit())) {
        ToProto(protoChunkSlice->mutable_lower_limit(), chunkSlice->LowerLimit());
    }
    if (IsNontrivial(chunkSlice->UpperLimit())) {
        ToProto(protoChunkSlice->mutable_upper_limit(), chunkSlice->UpperLimit());
    }
    if (chunkSlice->GetSizeOverridden()) {
        auto* sizeOverrideExt = protoChunkSlice->mutable_size_override_ext();
        sizeOverrideExt->set_uncompressed_data_size(chunkSlice->GetDataSize());
        sizeOverrideExt->set_row_count(chunkSlice->GetRowCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
