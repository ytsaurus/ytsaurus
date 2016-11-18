#include "input_slice.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "schema.h"

// ToDo(psushin): remove after merge to 19.
#include <yt/server/scheduler/helpers.h>

#include <yt/ytlib/table_client/serialize.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/numeric_helpers.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TInputSliceLimit::TInputSliceLimit(const TReadLimit& other)
{
    YCHECK(!other.HasChunkIndex());
    YCHECK(!other.HasOffset());
    if (other.HasRowIndex()) {
        RowIndex = other.GetRowIndex();
    }
    if (other.HasKey()) {
        Key = other.GetKey();
    }
}

TInputSliceLimit::TInputSliceLimit(const NProto::TReadLimit& other, const TRowBufferPtr& rowBuffer)
{
    YCHECK(!other.has_chunk_index());
    YCHECK(!other.has_offset());
    if (other.has_row_index()) {
        RowIndex = other.row_index();
    }
    if (other.has_key()) {
        NTableClient::FromProto(&Key, other.key(), rowBuffer);
    }
}

void TInputSliceLimit::MergeLowerRowIndex(i64 rowIndex)
{
    if (!RowIndex || *RowIndex < rowIndex) {
        RowIndex = rowIndex;
    }
}

void TInputSliceLimit::MergeUpperRowIndex(i64 rowIndex)
{
    if (!RowIndex || *RowIndex > rowIndex) {
        RowIndex = rowIndex;
    }
}

void TInputSliceLimit::MergeLowerKey(NTableClient::TKey key)
{
    if (!Key || Key < key) {
        Key = key;
    }
}

void TInputSliceLimit::MergeUpperKey(NTableClient::TKey key)
{
    if (!Key || Key > key) {
        Key = key;
    }
}

void TInputSliceLimit::MergeLowerLimit(const TInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        MergeLowerRowIndex(*limit.RowIndex);
    }
    if (limit.Key) {
        MergeLowerKey(limit.Key);
    }
}

void TInputSliceLimit::MergeUpperLimit(const TInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        MergeUpperRowIndex(*limit.RowIndex);
    }
    if (limit.Key) {
        MergeUpperKey(limit.Key);
    }
}

void TInputSliceLimit::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, RowIndex);
    Persist(context, Key);
}

void FormatValue(TStringBuilder* builder, const TInputSliceLimit& limit, const TStringBuf& /*format*/)
{
    builder->AppendFormat("{RowIndex: %v, Key: %v}",
        limit.RowIndex,
        limit.Key);
}

bool IsTrivial(const TInputSliceLimit& limit)
{
    return !limit.RowIndex && !limit.Key;
}

void ToProto(NProto::TReadLimit* protoLimit, const TInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        protoLimit->set_row_index(*limit.RowIndex);
    } else {
        protoLimit->clear_row_index();
    }

    if (limit.Key) {
        ToProto(protoLimit->mutable_key(), limit.Key);
    } else {
        protoLimit->clear_key();
    }
}

////////////////////////////////////////////////////////////////////////////////

TInputSlice::TInputSlice(
    const TInputChunkPtr& inputChunk,
    TKey lowerKey,
    TKey upperKey)
    : InputChunk_(inputChunk)
    , DataSize_(inputChunk->GetUncompressedDataSize())
    , RowCount_(inputChunk->GetRowCount())
{
    if (inputChunk->LowerLimit()) {
        LowerLimit_ = TInputSliceLimit(*inputChunk->LowerLimit());
    }
    if (lowerKey) {
        LowerLimit_.MergeLowerKey(lowerKey);
    }

    if (inputChunk->UpperLimit()) {
        UpperLimit_ = TInputSliceLimit(*inputChunk->UpperLimit());
    }
    if (upperKey) {
        UpperLimit_.MergeUpperKey(upperKey);
    }
}

TInputSlice::TInputSlice(
    const TInputSlice& inputSlice,
    TKey lowerKey,
    TKey upperKey)
    : InputChunk_(inputSlice.GetInputChunk())
    , LowerLimit_(inputSlice.LowerLimit())
    , UpperLimit_(inputSlice.UpperLimit())
    , PartIndex_(inputSlice.GetPartIndex())
    , SizeOverridden_(inputSlice.GetSizeOverridden())
    , DataSize_(inputSlice.GetDataSize())
    , RowCount_(inputSlice.GetRowCount())
{
    if (lowerKey) {
        LowerLimit_.MergeLowerKey(lowerKey);
    }
    if (upperKey) {
        UpperLimit_.MergeUpperKey(upperKey);
    }
}

TInputSlice::TInputSlice(
    const TInputSlice& chunkSlice,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : InputChunk_(chunkSlice.GetInputChunk())
    , LowerLimit_(chunkSlice.LowerLimit())
    , UpperLimit_(chunkSlice.UpperLimit())
{
    LowerLimit_.RowIndex = lowerRowIndex;
    UpperLimit_.RowIndex = upperRowIndex;
    SetRowCount(upperRowIndex - lowerRowIndex);
    SetDataSize(dataSize);
}

TInputSlice::TInputSlice(
    const TInputChunkPtr& inputChunk,
    int partIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : InputChunk_(inputChunk)
    , PartIndex_(partIndex)
{
    if (inputChunk->LowerLimit()) {
        LowerLimit_ = TInputSliceLimit(*inputChunk->LowerLimit());
    }
    LowerLimit_.MergeLowerRowIndex(lowerRowIndex);

    if (inputChunk->UpperLimit()) {
        UpperLimit_ = TInputSliceLimit(*inputChunk->UpperLimit());
    }
    UpperLimit_.MergeUpperRowIndex(upperRowIndex);

    SetRowCount(*UpperLimit_.RowIndex - *LowerLimit_.RowIndex);
    SetDataSize(dataSize);
}

TInputSlice::TInputSlice(
    const TInputChunkPtr& inputChunk,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSlice& protoChunkSlice)
    : TInputSlice(inputChunk)
{
    LowerLimit_.MergeLowerLimit(TInputSliceLimit(protoChunkSlice.lower_limit(), rowBuffer));
    UpperLimit_.MergeUpperLimit(TInputSliceLimit(protoChunkSlice.upper_limit(), rowBuffer));
    PartIndex_ = DefaultPartIndex;

    if (protoChunkSlice.has_row_count_override()) {
        SetRowCount(protoChunkSlice.row_count_override());
    }
    if (protoChunkSlice.has_uncompressed_data_size_override()) {
        SetDataSize(protoChunkSlice.uncompressed_data_size_override());
    }
}

std::vector<TInputSlicePtr> TInputSlice::SliceEvenly(i64 sliceDataSize, i64 sliceRowCount) const
{
    YCHECK(sliceDataSize > 0);
    YCHECK(sliceRowCount > 0);

    i64 lowerRowIndex = LowerLimit_.RowIndex.Get(0);
    i64 upperRowIndex = UpperLimit_.RowIndex.Get(GetRowCount());

    i64 rowCount = upperRowIndex - lowerRowIndex;
    
    i64 count = std::max(GetDataSize() / sliceDataSize, rowCount / sliceRowCount);
    count = std::max(std::min(count, rowCount), static_cast<i64>(1));

    std::vector<TInputSlicePtr> result;
    for (i64 i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = lowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = lowerRowIndex + rowCount * (i + 1) / count;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            result.push_back(New<TInputSlice>(
                *this,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                DivCeil(GetDataSize(), count)));
        }
    }
    return result;
}

i64 TInputSlice::GetLocality(int replicaPartIndex) const
{
    i64 result = GetDataSize();

    if (PartIndex_ == DefaultPartIndex) {
        // For erasure chunks without specified part index,
        // data size is assumed to be split evenly between data parts.
        auto codecId = InputChunk_->GetErasureCodec();
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

int TInputSlice::GetPartIndex() const
{
    return PartIndex_;
}

i64 TInputSlice::GetMaxBlockSize() const
{
    return InputChunk_->GetMaxBlockSize();
}

bool TInputSlice::GetSizeOverridden() const
{
    return SizeOverridden_;
}

i64 TInputSlice::GetDataSize() const
{
    return DataSize_;
}

i64 TInputSlice::GetRowCount() const
{
    return RowCount_;
}

void TInputSlice::SetDataSize(i64 dataSize)
{
    DataSize_ = dataSize;
    SizeOverridden_ = true;
}

void TInputSlice::SetRowCount(i64 rowCount)
{
    RowCount_ = rowCount;
    SizeOverridden_ = true;
}

void TInputSlice::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, InputChunk_);
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, PartIndex_);
    Persist(context, SizeOverridden_);
    Persist(context, RowCount_);
    Persist(context, DataSize_);
}

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TInputSlicePtr& slice)
{
    return Format("ChunkId: %v, LowerLimit: %v, UpperLimit: %v, RowCount: %v, DataSize: %v, PartIndex: %v",
        slice->GetInputChunk()->ChunkId(),
        slice->LowerLimit(),
        slice->UpperLimit(),
        slice->GetRowCount(),
        slice->GetDataSize(),
        slice->GetPartIndex());
}

bool CompareSlicesByLowerLimit(const TInputSlicePtr& slice1, const TInputSlicePtr& slice2)
{
    const auto& limit1 = slice1->LowerLimit();
    const auto& limit2 = slice2->LowerLimit();

    i64 diff = slice1->GetInputChunk()->GetRangeIndex() - slice2->GetInputChunk()->GetRangeIndex();
    if (diff != 0) {
        return diff < 0;
    }

    diff = (limit1.RowIndex.Get(0) + slice1->GetInputChunk()->GetTableRowIndex()) -
        (limit2.RowIndex.Get(0) + slice2->GetInputChunk()->GetTableRowIndex());
    if (diff != 0) {
        return diff < 0;
    }

    diff = CompareRows(limit1.Key, limit2.Key);
    return diff < 0;
}

bool CanMergeSlices(const TInputSlicePtr& slice1, const TInputSlicePtr& slice2)
{
    if (slice1->GetInputChunk()->GetRangeIndex() != slice2->GetInputChunk()->GetRangeIndex()) {
        return false;
    }

    const auto& limit1 = slice1->UpperLimit();
    const auto& limit2 = slice2->LowerLimit();

    if ((limit1.RowIndex || limit1.Key) &&
        limit1.RowIndex.operator bool() == limit2.RowIndex.operator bool() &&
        limit1.Key.operator bool() == limit2.Key.operator bool())
    {
        if (limit1.RowIndex &&
            *limit1.RowIndex + slice1->GetInputChunk()->GetTableRowIndex() !=
            *limit2.RowIndex + slice2->GetInputChunk()->GetTableRowIndex())
        {
            return false;
        }
        if (limit1.Key && limit1.Key < limit2.Key) {
            return false;
        }
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TInputSlicePtr CreateInputSlice(
    const TInputChunkPtr& inputChunk,
    TKey lowerKey,
    TKey upperKey)
{
    return New<TInputSlice>(inputChunk, lowerKey, upperKey);
}

TInputSlicePtr CreateInputSlice(
    const TInputSlice& inputSlice,
    TKey lowerKey,
    TKey upperKey)
{
    return New<TInputSlice>(inputSlice, lowerKey, upperKey);
}

TInputSlicePtr CreateInputSlice(
    const TInputChunkPtr& inputChunk,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSlice& protoChunkSlice)
{
    return New<TInputSlice>(inputChunk, rowBuffer, protoChunkSlice);
}

std::vector<TInputSlicePtr> CreateErasureInputSlices(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId)
{
    std::vector<TInputSlicePtr> slices;

    i64 dataSize = inputChunk->GetUncompressedDataSize();
    i64 rowCount = inputChunk->GetRowCount();

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceLowerRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceUpperRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto chunkSlice = New<TInputSlice>(
                inputChunk,
                partIndex,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                (dataSize + dataPartCount - 1) / dataPartCount);
            slices.emplace_back(std::move(chunkSlice));
        }
    }

    return slices;
}

std::vector<TInputSlicePtr> SliceChunkByRowIndexes(
    const TInputChunkPtr& inputChunk,
    i64 sliceDataSize,
    i64 sliceRowCount)
{
    return CreateInputSlice(inputChunk)->SliceEvenly(sliceDataSize, sliceRowCount);
}

void ToProto(NProto::TChunkSpec* chunkSpec, const TInputSlicePtr& inputSlice)
{
    // The chunk spec in the slice has arrived from master, so it can't possibly contain any extensions
    // except misc and boundary keys (in sorted merge or reduce). Jobs request boundary keys
    // from the nodes when needed, so we remove it here, to optimize traffic from the scheduler and
    // proto serialization time.

    ToProto(chunkSpec, inputSlice->GetInputChunk());

    if (!IsTrivial(inputSlice->LowerLimit())) {
        ToProto(chunkSpec->mutable_lower_limit(), inputSlice->LowerLimit());
    }

    if (!IsTrivial(inputSlice->UpperLimit())) {
        ToProto(chunkSpec->mutable_upper_limit(), inputSlice->UpperLimit());
    }

    if (inputSlice->GetSizeOverridden()) {
        chunkSpec->set_uncompressed_data_size_override(inputSlice->GetDataSize());
        chunkSpec->set_row_count_override(inputSlice->GetRowCount());
    } else {
        chunkSpec->set_uncompressed_data_size_override(inputSlice->GetInputChunk()->GetUncompressedDataSize());
        chunkSpec->set_row_count_override(inputSlice->GetInputChunk()->GetRowCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
