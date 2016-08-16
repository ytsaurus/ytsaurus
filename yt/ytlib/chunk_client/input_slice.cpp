#include "input_slice.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "input_chunk.h"

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

TInputSlice::TInputSlice(
    TInputChunkPtr inputChunk,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
    : InputChunk_(inputChunk)
    , LowerLimit_(inputChunk->LowerLimit())
    , UpperLimit_(inputChunk->UpperLimit())
    , DataSize_(inputChunk->GetUncompressedDataSize())
    , RowCount_(inputChunk->GetRowCount())
{
    if (lowerKey) {
        LowerLimit_.MergeLowerKey(*lowerKey);
    }

    if (upperKey) {
        UpperLimit_.MergeUpperKey(*upperKey);
    }
}

TInputSlice::TInputSlice(
    const TIntrusivePtr<const TInputSlice>& chunkSlice,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
    : InputChunk_(chunkSlice->GetInputChunk())
    , LowerLimit_(chunkSlice->LowerLimit())
    , UpperLimit_(chunkSlice->UpperLimit())
    , PartIndex_(chunkSlice->GetPartIndex())
    , SizeOverridden_(chunkSlice->GetSizeOverridden())
    , DataSize_(chunkSlice->GetDataSize())
    , RowCount_(chunkSlice->GetRowCount())
{
    if (lowerKey) {
        LowerLimit_.MergeLowerKey(*lowerKey);
    }

    if (upperKey) {
        UpperLimit_.MergeUpperKey(*upperKey);
    }
}

TInputSlice::TInputSlice(
    const TIntrusivePtr<const TInputSlice>& chunkSlice,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : InputChunk_(chunkSlice->GetInputChunk())
    , LowerLimit_(chunkSlice->LowerLimit())
    , UpperLimit_(chunkSlice->UpperLimit())
{
    LowerLimit_.SetRowIndex(lowerRowIndex);
    UpperLimit_.SetRowIndex(upperRowIndex);
    SetRowCount(upperRowIndex - lowerRowIndex);
    SetDataSize(dataSize);
}

TInputSlice::TInputSlice(
    TInputChunkPtr inputChunk,
    int partIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : InputChunk_(inputChunk)
    , LowerLimit_(inputChunk->LowerLimit())
    , UpperLimit_(inputChunk->UpperLimit())
    , PartIndex_(partIndex)
{
    LowerLimit_.MergeLowerRowIndex(lowerRowIndex);
    UpperLimit_.MergeUpperRowIndex(upperRowIndex);

    SetRowCount(UpperLimit_.GetRowIndex() - LowerLimit_.GetRowIndex());
    SetDataSize(dataSize);
}

TInputSlice::TInputSlice(
    TInputChunkPtr inputChunk,
    const NProto::TChunkSlice& protoChunkSlice)
    : TInputSlice(inputChunk, Null, Null)
{
    LowerLimit_.MergeLowerLimit(protoChunkSlice.lower_limit());
    UpperLimit_.MergeUpperLimit(protoChunkSlice.upper_limit());
    PartIndex_ = DefaultPartIndex;

    if (protoChunkSlice.has_size_override_ext()) {
        SetRowCount(protoChunkSlice.size_override_ext().row_count());
        SetDataSize(protoChunkSlice.size_override_ext().uncompressed_data_size());
    }
}

std::vector<TInputSlicePtr> TInputSlice::SliceEvenly(i64 sliceDataSize) const
{
    std::vector<TInputSlicePtr> result;

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
            auto chunkSlice = New<TInputSlice>(
                this,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                (GetDataSize() + count - 1) / count);
            result.emplace_back(std::move(chunkSlice));
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

void TInputSlice::Persist(NPhoenix::TPersistenceContext& context)
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

size_t SpaceUsed(const TInputSlicePtr& slice)
{
    return sizeof(*slice) +
           slice->LowerLimit_.SpaceUsed() - sizeof(slice->LowerLimit_) +
           slice->UpperLimit_.SpaceUsed() - sizeof(slice->UpperLimit_);
}

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

    if (limit1.HasRowIndex() && limit2.HasRowIndex()) {
        return limit1.GetRowIndex() + slice1->GetInputChunk()->GetTableRowIndex() <
            limit2.GetRowIndex() + slice2->GetInputChunk()->GetTableRowIndex();
    }
    if (limit1.HasKey() && limit2.HasKey()) {
        return limit1.GetKey() < limit2.GetKey();
    }
    return false;
}

bool CanMergeSlices(const TInputSlicePtr& slice1, const TInputSlicePtr& slice2)
{
    const auto& limit1 = slice1->UpperLimit();
    const auto& limit2 = slice2->LowerLimit();

    if ((limit1.HasRowIndex() || limit1.HasKey()) &&
        limit1.HasRowIndex() == limit2.HasRowIndex() &&
        limit1.HasKey() == limit2.HasKey())
    {
        if (limit1.HasRowIndex() &&
            limit1.GetRowIndex() + slice1->GetInputChunk()->GetTableRowIndex() !=
            limit2.GetRowIndex() + slice2->GetInputChunk()->GetTableRowIndex())
        {
            return false;
        }
        if (limit1.HasKey() && limit1.GetKey() != limit2.GetKey()) {
            return false;
        }
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TInputSlicePtr CreateInputSlice(
    TInputChunkPtr inputChunk,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
{
    return New<TInputSlice>(inputChunk, lowerKey, upperKey);
}

TInputSlicePtr CreateInputSlice(
    TInputSlicePtr inputSlice,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
{
    return New<TInputSlice>(inputSlice, lowerKey, upperKey);
}

TInputSlicePtr CreateInputSlice(
    TInputChunkPtr inputChunk,
    const NProto::TChunkSlice& protoChunkSlice)
{
    return New<TInputSlice>(inputChunk, protoChunkSlice);
}

std::vector<TInputSlicePtr> CreateErasureInputSlices(
    TInputChunkPtr inputChunk,
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

////////////////////////////////////////////////////////////////////////////////

std::vector<TInputSlicePtr> SliceChunkByRowIndexes(TInputChunkPtr inputChunk, i64 sliceDataSize)
{
    return CreateInputSlice(inputChunk)->SliceEvenly(sliceDataSize);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkSpec* chunkSpec, TInputSlicePtr inputSlice)
{
    // The chunk spec in the slice has arrived from master, so it can't possibly contain any extensions
    // except misc and boundary keys (in sorted merge or reduce). Jobs request boundary keys
    // from the nodes when needed, so we remove it here, to optimize traffic from the scheduler and
    // proto serialization time.

    ToProto(chunkSpec, inputSlice->GetInputChunk());

    if (IsNontrivial(inputSlice->LowerLimit())) {
        ToProto(chunkSpec->mutable_lower_limit(), inputSlice->LowerLimit());
    }

    if (IsNontrivial(inputSlice->UpperLimit())) {
        ToProto(chunkSpec->mutable_upper_limit(), inputSlice->UpperLimit());
    }

    // Since we don't serialize MiscExt into proto, we always create SizeOverrideExt to track progress.
    if (inputSlice->GetSizeOverridden()) {
        TSizeOverrideExt sizeOverrideExt;
        sizeOverrideExt.set_uncompressed_data_size(inputSlice->GetDataSize());
        sizeOverrideExt.set_row_count(inputSlice->GetRowCount());
        SetProtoExtension(chunkSpec->mutable_chunk_meta()->mutable_extensions(), sizeOverrideExt);
    } else {
        TSizeOverrideExt sizeOverrideExt;
        sizeOverrideExt.set_uncompressed_data_size(inputSlice->GetInputChunk()->GetUncompressedDataSize());
        sizeOverrideExt.set_row_count(inputSlice->GetInputChunk()->GetRowCount());
        SetProtoExtension(chunkSpec->mutable_chunk_meta()->mutable_extensions(), sizeOverrideExt);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
