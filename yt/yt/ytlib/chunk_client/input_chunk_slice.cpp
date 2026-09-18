#include "input_chunk_slice.h"

#include "private.h"
#include "input_chunk.h"

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/serialize.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

#include <cmath>

namespace NYT::NChunkClient {

using namespace NControllerAgent;
using namespace NTableClient::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TInputSliceLimit::TInputSliceLimit(
    const NProto::TReadLimit& other,
    const TRowBufferPtr& rowBuffer,
    TRange<TLegacyKey> keySet,
    TRange<TLegacyKey> keyBoundPrefixSet,
    int keyLength,
    bool isUpper)
{
    YT_VERIFY(!other.has_chunk_index());
    YT_VERIFY(!other.has_offset());
    if (other.has_row_index()) {
        RowIndex = other.row_index();
    }

    if (other.has_key_index()) {
        // COMPAT(gritukan)
        if (keyBoundPrefixSet.empty()) {
            auto row = rowBuffer->CaptureRow(keySet[other.key_index()]);
            KeyBound = KeyBoundFromLegacyRow(row, isUpper, keyLength, rowBuffer);
        } else {
            auto row = rowBuffer->CaptureRow(keyBoundPrefixSet[other.key_index()]);
            KeyBound = TKeyBound::FromRowUnchecked(
                row,
                other.key_bound_is_inclusive(),
                isUpper);
        }
    } else {
        KeyBound = TKeyBound::MakeUniversal(isUpper);
        // COMPAT(pogorelov): Old job proxies update only legacy_key when building an
        // interrupt descriptor, leaving key_bound_prefix from the original job spec.
        if (other.has_legacy_key()) {
            TUnversionedOwningRow row;
            NTableClient::FromProto(&row, other.legacy_key());
            KeyBound = KeyBoundFromLegacyRow(row, isUpper, keyLength, rowBuffer);
        } else if (other.has_key_bound_prefix()) {
            TUnversionedOwningRow row;
            NTableClient::FromProto(&row, other.key_bound_prefix());
            KeyBound.Prefix = rowBuffer->CaptureRow(row);
            KeyBound.IsUpper = isUpper;
            KeyBound.IsInclusive = other.key_bound_is_inclusive();
        }
    }
}

TInputSliceLimit::TInputSliceLimit(bool isUpper)
    : KeyBound(TKeyBound::MakeUniversal(isUpper))
{ }

void TInputSliceLimit::MergeLower(const TInputSliceLimit& other, const TComparator& comparator)
{
    if (!RowIndex || (other.RowIndex && *other.RowIndex > *RowIndex)) {
        RowIndex = other.RowIndex;
    }
    if (comparator) {
        comparator.ReplaceIfStrongerKeyBound(KeyBound, other.KeyBound);
    } else {
        YT_VERIFY(!other.KeyBound || other.KeyBound.IsUniversal());
    }
    YT_VERIFY(!KeyBound || !KeyBound.IsUpper);
}

void TInputSliceLimit::MergeUpper(const TInputSliceLimit& other, const TComparator& comparator)
{
    if (!RowIndex || (other.RowIndex && *other.RowIndex < *RowIndex)) {
        RowIndex = other.RowIndex;
    }
    if (comparator) {
        comparator.ReplaceIfStrongerKeyBound(KeyBound, other.KeyBound);
    } else {
        YT_VERIFY(!other.KeyBound || other.KeyBound.IsUniversal());
    }
    YT_VERIFY(!KeyBound || KeyBound.IsUpper);
}

bool TInputSliceLimit::IsTrivial() const
{
    return (!KeyBound || KeyBound.IsUniversal()) && !RowIndex;
}

void TInputSliceLimit::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, RowIndex);
    PHOENIX_REGISTER_FIELD(2, KeyBound);
}

void Serialize(const TInputSliceLimit& limit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("key_bound", limit.KeyBound)
            .OptionalItem("row_index", limit.RowIndex)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TInputSliceLimit& limit, TStringBuf /*spec*/)
{
    if (!limit.RowIndex && !limit.KeyBound) {
        builder->AppendChar('#');
        return;
    }
    builder->AppendChar('[');
    if (limit.RowIndex) {
        builder->AppendFormat("#%v", limit.RowIndex);
    }
    if (limit.RowIndex && limit.KeyBound) {
        builder->AppendString(", ");
    }
    if (limit.KeyBound) {
        builder->AppendFormat("%v", limit.KeyBound);
    }
    builder->AppendChar(']');
}

bool IsTrivial(const TInputSliceLimit& limit)
{
    return !limit.RowIndex && (!limit.KeyBound || limit.KeyBound.IsUniversal());
}

void ToProto(NProto::TReadLimit* protoLimit, const TInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        protoLimit->set_row_index(*limit.RowIndex);
    } else {
        protoLimit->clear_row_index();
    }

    if (!limit.KeyBound || limit.KeyBound.IsUniversal()) {
        protoLimit->clear_legacy_key();
        protoLimit->clear_key_bound_prefix();
        protoLimit->clear_key_bound_is_inclusive();
    } else {
        protoLimit->set_key_bound_is_inclusive(limit.KeyBound.IsInclusive);
        auto legacyRow = KeyBoundToLegacyRow(limit.KeyBound);
        ToProto(protoLimit->mutable_legacy_key(), legacyRow);
        ToProto(protoLimit->mutable_key_bound_prefix(), limit.KeyBound.Prefix);
    }
}

PHOENIX_DEFINE_TYPE(TInputSliceLimit);

////////////////////////////////////////////////////////////////////////////////

namespace {

TInputSliceLimit ConvertKeylessInputChunkReadLimit(
    const TInputChunk::TReadLimitHolder& readLimit,
    bool isUpper)
{
    TInputSliceLimit result(isUpper);
    if (!readLimit) {
        return result;
    }

    YT_VERIFY(!readLimit->HasChunkIndex());
    YT_VERIFY(!readLimit->HasOffset());
    YT_VERIFY(!readLimit->HasLegacyKey());

    if (readLimit->HasRowIndex()) {
        result.RowIndex = readLimit->GetRowIndex();
    }

    return result;
}

TInputSliceLimit ConvertInputChunkReadLimit(
    const TInputChunk::TReadLimitHolder& readLimit,
    const TRowBufferPtr& rowBuffer,
    const TComparator& comparator,
    bool isUpper)
{
    YT_VERIFY(rowBuffer);

    TInputSliceLimit result(isUpper);
    if (!readLimit) {
        return result;
    }

    YT_VERIFY(!readLimit->HasChunkIndex());
    YT_VERIFY(!readLimit->HasOffset());

    if (readLimit->HasRowIndex()) {
        result.RowIndex = readLimit->GetRowIndex();
    }
    if (readLimit->HasLegacyKey()) {
        YT_VERIFY(comparator.GetLength() > 0);
        result.KeyBound = KeyBoundFromLegacyRow(
            readLimit->GetLegacyKey(),
            isUpper,
            comparator.GetLength(),
            rowBuffer);
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    TInputSliceLimit lowerLimit,
    TInputSliceLimit upperLimit)
    : InputChunk_(inputChunk)
    , LowerLimit_(std::move(lowerLimit))
    , UpperLimit_(std::move(upperLimit))
    , DataWeight_(inputChunk->GetDataWeight())
    , RowCount_(inputChunk->GetRowCount())
    , CompressedDataSize_(inputChunk->GetCompressedDataSize())
    , UncompressedDataSize_(inputChunk->GetUncompressedDataSize())
{ }

TInputChunkSlice::TInputChunkSlice(const TInputChunkSlice& inputSlice)
    : InputChunk_(inputSlice.GetInputChunk())
    , LowerLimit_(inputSlice.LowerLimit())
    , UpperLimit_(inputSlice.UpperLimit())
    , SliceIndex_(inputSlice.GetSliceIndex())
    , PartIndex_(inputSlice.GetPartIndex())
    , SizeOverridden_(inputSlice.GetSizeOverridden())
    , DataWeight_(inputSlice.GetDataWeight())
    , RowCount_(inputSlice.GetRowCount())
    , CompressedDataSize_(inputSlice.GetCompressedDataSize())
    , UncompressedDataSize_(inputSlice.GetUncompressedDataSize())
{ }

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    const TComparator& comparator,
    TKeyBound lowerKeyBound,
    TKeyBound upperKeyBound)
    : InputChunk_(inputSlice.GetInputChunk())
    , LowerLimit_(inputSlice.LowerLimit())
    , UpperLimit_(inputSlice.UpperLimit())
    , SliceIndex_(inputSlice.GetSliceIndex())
    , PartIndex_(inputSlice.GetPartIndex())
    , SizeOverridden_(inputSlice.GetSizeOverridden())
    , DataWeight_(inputSlice.GetDataWeight())
    , RowCount_(inputSlice.GetRowCount())
    , CompressedDataSize_(inputSlice.GetCompressedDataSize())
    , UncompressedDataSize_(inputSlice.GetUncompressedDataSize())
{
    LowerLimit_.KeyBound = comparator.StrongerKeyBound(LowerLimit_.KeyBound, lowerKeyBound);
    UpperLimit_.KeyBound = comparator.StrongerKeyBound(UpperLimit_.KeyBound, upperKeyBound);
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& chunkSlice,
    i64 lowerRowIndex,
    std::optional<i64> upperRowIndex,
    i64 dataWeight,
    i64 compressedDataSize,
    i64 uncompressedDataSize)
    : InputChunk_(chunkSlice.GetInputChunk())
    , LowerLimit_(chunkSlice.LowerLimit())
    , UpperLimit_(chunkSlice.UpperLimit())
    , SliceIndex_(chunkSlice.GetSliceIndex())
{
    LowerLimit_.RowIndex = lowerRowIndex;
    UpperLimit_.RowIndex = upperRowIndex;

    if (upperRowIndex) {
        OverrideSize(*upperRowIndex - lowerRowIndex, dataWeight, compressedDataSize, uncompressedDataSize);
    }
}

TInputChunkSlicePtr CreateInputChunkSliceFromCompleteErasureChunkPart(
    const TInputChunkPtr& inputChunk,
    int partIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataWeight,
    i64 compressedDataSize,
    i64 uncompressedDataSize)
{
    YT_VERIFY(inputChunk->IsCompleteChunk());
    YT_VERIFY(partIndex >= 0);
    YT_VERIFY(0 <= lowerRowIndex && lowerRowIndex < upperRowIndex);
    YT_VERIFY(upperRowIndex <= inputChunk->GetRowCount());

    TInputSliceLimit lowerLimit(/*isUpper*/ false);
    lowerLimit.RowIndex = lowerRowIndex;
    TInputSliceLimit upperLimit(/*isUpper*/ true);
    upperLimit.RowIndex = upperRowIndex;

    auto chunkSlice = New<TInputChunkSlice>(
        inputChunk,
        std::move(lowerLimit),
        std::move(upperLimit));
    chunkSlice->PartIndex_ = partIndex;
    chunkSlice->OverrideSize(
        upperRowIndex - lowerRowIndex,
        std::max<i64>(1, dataWeight * inputChunk->GetDataWeightSelectivityFactor()),
        compressedDataSize,
        uncompressedDataSize);
    return chunkSlice;
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& chunkSlice,
    const TComparator& comparator,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSlice& protoChunkSlice,
    TRange<TLegacyKey> keySet,
    TRange<TLegacyKey> keyBoundPrefixes)
    : InputChunk_(chunkSlice.GetInputChunk())
    , LowerLimit_(chunkSlice.LowerLimit())
    , UpperLimit_(chunkSlice.UpperLimit())
    , SliceIndex_(chunkSlice.GetSliceIndex())
{
    LowerLimit_.MergeLower(
        TInputSliceLimit(protoChunkSlice.lower_limit(), rowBuffer, keySet, keyBoundPrefixes, comparator.GetLength(), /*isUpper*/ false),
        comparator);
    UpperLimit_.MergeUpper(
        TInputSliceLimit(protoChunkSlice.upper_limit(), rowBuffer, keySet, keyBoundPrefixes, comparator.GetLength(), /*isUpper*/ true),
        comparator);

    PartIndex_ = DefaultPartIndex;

    OverrideSize(chunkSlice.GetInputChunk(), protoChunkSlice);
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSpec& protoChunkSpec,
    const TComparator& comparator)
    : InputChunk_(inputChunk)
    , LowerLimit_(ConvertInputChunkReadLimit(inputChunk->LowerLimit(), rowBuffer, comparator, /*isUpper*/ false))
    , UpperLimit_(ConvertInputChunkReadLimit(inputChunk->UpperLimit(), rowBuffer, comparator, /*isUpper*/ true))
{
    YT_VERIFY(!protoChunkSpec.lower_limit().has_key_index());
    YT_VERIFY(!protoChunkSpec.upper_limit().has_key_index());

    static const TRange<TLegacyKey> EmptyKeys;
    LowerLimit_.MergeLower(TInputSliceLimit(
        protoChunkSpec.lower_limit(),
        rowBuffer,
        EmptyKeys,
        EmptyKeys,
        comparator.GetLength(),
        /*isUpper*/ false),
        comparator);
    UpperLimit_.MergeUpper(TInputSliceLimit(
        protoChunkSpec.upper_limit(),
        rowBuffer,
        EmptyKeys,
        EmptyKeys,
        comparator.GetLength(),
        /*isUpper*/ true),
        comparator);
    PartIndex_ = DefaultPartIndex;

    OverrideSize(inputChunk, protoChunkSpec);
}

void TInputChunkSlice::OverrideSize(const TInputChunkPtr& inputChunk, const NProto::TChunkSlice& protoChunkSlice)
{
    YT_VERIFY(
        protoChunkSlice.has_row_count_override() &&
        protoChunkSlice.has_data_weight_override() &&
        protoChunkSlice.has_compressed_data_size_override() &&
        protoChunkSlice.has_uncompressed_data_size_override());

    auto computeSize = [] (i64 sizeOverride, double selectivityFactor) {
        return std::max(1l, SignedSaturationConversion(sizeOverride * selectivityFactor));
    };

    OverrideSize(
        protoChunkSlice.row_count_override(),
        computeSize(protoChunkSlice.data_weight_override(), inputChunk->GetDataWeightSelectivityFactor()),
        computeSize(protoChunkSlice.compressed_data_size_override(), inputChunk->GetReadSizeSelectivityFactor()),
        computeSize(protoChunkSlice.uncompressed_data_size_override(), inputChunk->GetReadSizeSelectivityFactor()));
}

void TInputChunkSlice::OverrideSize(const TInputChunkPtr& inputChunk, const NProto::TChunkSpec& protoChunkSpec)
{
    if (!protoChunkSpec.has_row_count_override()) {
        YT_VERIFY(
            !protoChunkSpec.has_data_weight_override() &&
            !protoChunkSpec.has_compressed_data_size_override() &&
            !protoChunkSpec.has_uncompressed_data_size_override());
        return;
    }
    YT_VERIFY(
        protoChunkSpec.has_data_weight_override() &&
        protoChunkSpec.has_compressed_data_size_override() &&
        protoChunkSpec.has_uncompressed_data_size_override());

    // COMPAT(apollo1321): Remove in 26.1.
    if (!protoChunkSpec.use_new_override_semantics()) {
        auto computeSize = [] (i64 sizeOverride, double selectivityFactor) {
            return std::max(1l, SignedSaturationConversion(sizeOverride * selectivityFactor));
        };

        OverrideSize(
            protoChunkSpec.row_count_override(),
            computeSize(protoChunkSpec.data_weight_override(), inputChunk->GetDataWeightSelectivityFactor()),
            computeSize(protoChunkSpec.compressed_data_size_override(), inputChunk->GetReadSizeSelectivityFactor()),
            computeSize(protoChunkSpec.uncompressed_data_size_override(), inputChunk->GetReadSizeSelectivityFactor()));

        return;
    }

    OverrideSize(
        protoChunkSpec.row_count_override(),
        protoChunkSpec.data_weight_override(),
        protoChunkSpec.compressed_data_size_override(),
        protoChunkSpec.uncompressed_data_size_override());

}

std::vector<TInputChunkSlicePtr> TInputChunkSlice::SliceEvenly(i64 sliceDataWeight, i64 sliceRowCount, TRowBufferPtr rowBuffer) const
{
    YT_VERIFY(sliceDataWeight > 0);
    YT_VERIFY(sliceRowCount > 0);
    YT_VERIFY(!InputChunk_->IsSortedDynamicStore());

    if (InputChunk_->IsOrderedDynamicStore() && !UpperLimit_.RowIndex) {
        return {New<TInputChunkSlice>(*this)};
    }

    i64 lowerRowIndex = LowerLimit_.RowIndex.value_or(0);
    i64 upperRowIndex = UpperLimit_.RowIndex.value_or(InputChunk_->GetRowCount());
    upperRowIndex = std::max(lowerRowIndex, upperRowIndex);
    i64 rowCount = upperRowIndex - lowerRowIndex;

    if (rowCount == 0) {
        return {};
    }

    i64 count = std::max(DivCeil(GetDataWeight(), sliceDataWeight), DivCeil(rowCount, sliceRowCount));
    // NB(gepardo): We need to consider cases with count == 0 or rowCount == 0 carefully. The
    // latter case is considered above. In the former case, we have non-empty data and need one
    // slice, so forcefully set count to 1.
    count = std::clamp<i64>(count, 1, rowCount);

    std::vector<TInputChunkSlicePtr> result;
    result.reserve(count);
    for (i64 i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = lowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = lowerRowIndex + rowCount * (i + 1) / count;
        i64 sliceLowerDataWeight = GetDataWeight() * i / count;
        i64 sliceUpperDataWeight = GetDataWeight() * (i + 1) / count;
        i64 sliceLowerCompressedDataSize = GetCompressedDataSize() * i / count;
        i64 sliceUpperCompressedDataSize = GetCompressedDataSize() * (i + 1) / count;
        i64 sliceLowerUncompressedDataSize = GetUncompressedDataSize() * i / count;
        i64 sliceUpperUncompressedDataSize = GetUncompressedDataSize() * (i + 1) / count;
        YT_VERIFY(sliceLowerRowIndex < sliceUpperRowIndex);
        result.push_back(New<TInputChunkSlice>(
            *this,
            sliceLowerRowIndex,
            sliceUpperRowIndex,
            sliceUpperDataWeight - sliceLowerDataWeight,
            sliceUpperCompressedDataSize - sliceLowerCompressedDataSize,
            sliceUpperUncompressedDataSize - sliceLowerUncompressedDataSize));
    }
    if (rowBuffer) {
        auto& lowerBound = result.front()->LowerLimit().KeyBound;
        auto& upperBound = result.back()->UpperLimit().KeyBound;
        lowerBound.Prefix = rowBuffer->CaptureRow(lowerBound.Prefix);
        upperBound.Prefix = rowBuffer->CaptureRow(upperBound.Prefix);
    }

    return result;
}

std::pair<TInputChunkSlicePtr, TInputChunkSlicePtr> TInputChunkSlice::SplitByRowIndex(i64 splitRow) const
{
    i64 lowerRowIndex = LowerLimit_.RowIndex.value_or(0);
    i64 upperRowIndex = UpperLimit_.RowIndex.value_or(InputChunk_->GetRowCount());

    YT_VERIFY(!InputChunk_->IsSortedDynamicStore());
    YT_VERIFY(!InputChunk_->IsOrderedDynamicStore() || UpperLimit_.RowIndex);

    i64 rowCount = upperRowIndex - lowerRowIndex;

    YT_VERIFY(splitRow >= 0 && splitRow <= rowCount);

    return std::pair(
        New<TInputChunkSlice>(
            *this,
            lowerRowIndex,
            lowerRowIndex + splitRow,
            std::max<i64>(1, GetDataWeight() * 1.0 / rowCount * splitRow),
            std::max<i64>(1, GetCompressedDataSize() * 1.0 / rowCount * splitRow),
            std::max<i64>(1, GetUncompressedDataSize() * 1.0 / rowCount * splitRow)),
        New<TInputChunkSlice>(
            *this,
            lowerRowIndex + splitRow,
            upperRowIndex,
            std::max<i64>(1, GetDataWeight() * 1.0 / rowCount * (rowCount - splitRow)),
            std::max<i64>(1, GetCompressedDataSize() * 1.0 / rowCount * (rowCount - splitRow)),
            std::max<i64>(1, GetUncompressedDataSize() * 1.0 / rowCount * (rowCount - splitRow))));
}

i64 TInputChunkSlice::GetLocality(int replicaPartIndex) const
{
    i64 result = GetDataWeight();

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

int TInputChunkSlice::GetPartIndex() const
{
    return PartIndex_;
}

i64 TInputChunkSlice::GetMaxBlockSize() const
{
    return InputChunk_->GetMaxBlockSize();
}

i64 TInputChunkSlice::GetValueCount() const
{
    return InputChunk_->GetValuesPerRow() * GetRowCount();
}

bool TInputChunkSlice::GetSizeOverridden() const
{
    return SizeOverridden_;
}

i64 TInputChunkSlice::GetDataWeight() const
{
    return SizeOverridden_ ? DataWeight_ : InputChunk_->GetDataWeight();
}

i64 TInputChunkSlice::GetRowCount() const
{
    return SizeOverridden_ ? RowCount_ : InputChunk_->GetRowCount();
}

i64 TInputChunkSlice::GetCompressedDataSize() const
{
    return SizeOverridden_ ? CompressedDataSize_ : InputChunk_->GetCompressedDataSize();
}

i64 TInputChunkSlice::GetUncompressedDataSize() const
{
    return SizeOverridden_ ? UncompressedDataSize_ : InputChunk_->GetUncompressedDataSize();
}

void TInputChunkSlice::OverrideSize(i64 rowCount, i64 dataWeight, i64 compressedDataSize, i64 uncompressedDataSize)
{
    RowCount_ = rowCount;
    DataWeight_ = dataWeight;

    auto normalizeDataSize = [&] (i64 dataSize) {
        return rowCount > 0 && dataWeight > 0
            ? std::max<i64>(1, dataSize)
            : dataSize;
    };

    CompressedDataSize_ = normalizeDataSize(compressedDataSize);
    UncompressedDataSize_ = normalizeDataSize(uncompressedDataSize);
    SizeOverridden_ = true;
}

void TInputChunkSlice::ApplySamplingSelectivityFactor(double samplingSelectivityFactor)
{
    i64 rowCount = std::max<i64>(1, GetRowCount() * samplingSelectivityFactor);
    i64 dataWeight = std::max<i64>(1, GetDataWeight() * samplingSelectivityFactor);
    i64 compressedDataSize = std::max<i64>(1, GetCompressedDataSize() * samplingSelectivityFactor);
    i64 uncompressedDataSize = std::max<i64>(1, GetUncompressedDataSize() * samplingSelectivityFactor);
    OverrideSize(rowCount, dataWeight, compressedDataSize, uncompressedDataSize);
}

void TInputChunkSlice::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, InputChunk_);
    PHOENIX_REGISTER_FIELD(2, LowerLimit_);
    PHOENIX_REGISTER_FIELD(3, UpperLimit_);
    PHOENIX_REGISTER_FIELD(4, PartIndex_);
    PHOENIX_REGISTER_FIELD(5, SizeOverridden_);
    PHOENIX_REGISTER_FIELD(6, RowCount_);
    PHOENIX_REGISTER_FIELD(7, DataWeight_);
    PHOENIX_REGISTER_FIELD(8, SliceIndex_);
    PHOENIX_REGISTER_FIELD(9, CompressedDataSize_,
        .SinceVersion(static_cast<int>(ESnapshotVersion::MaxCompressedDataSizePerJob)));
    PHOENIX_REGISTER_FIELD(10, UncompressedDataSize_,
        .SinceVersion(static_cast<int>(ESnapshotVersion::InputChunkSliceUncompressedDataSize)));
}

PHOENIX_DEFINE_TYPE(TInputChunkSlice);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInputChunkSlicePtr& slice, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{ChunkId: %v, LowerLimit: %v, UpperLimit: %v, RowCount: %v, DataWeight: %v, "
        "CompressedDataSize: %v, UncompressedDataSize: %v, PartIndex: %v}",
        slice->GetInputChunk()->GetChunkId(),
        slice->LowerLimit(),
        slice->UpperLimit(),
        slice->GetRowCount(),
        slice->GetDataWeight(),
        slice->GetCompressedDataSize(),
        slice->GetUncompressedDataSize(),
        slice->GetPartIndex());
}

////////////////////////////////////////////////////////////////////////////////

TInputChunkSlicePtr CreateKeylessInputChunkSlice(const TInputChunkPtr& inputChunk)
{
    return New<TInputChunkSlice>(
        inputChunk,
        ConvertKeylessInputChunkReadLimit(inputChunk->LowerLimit(), /*isUpper*/ false),
        ConvertKeylessInputChunkReadLimit(inputChunk->UpperLimit(), /*isUpper*/ true));
}

TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    TInputSliceLimit lowerLimit,
    TInputSliceLimit upperLimit)
{
    return New<TInputChunkSlice>(inputChunk, std::move(lowerLimit), std::move(upperLimit));
}

TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const TRowBufferPtr& rowBuffer,
    const TComparator& comparator)
{
    return CreateInputChunkSlice(
        inputChunk,
        ConvertInputChunkReadLimit(inputChunk->LowerLimit(), rowBuffer, comparator, /*isUpper*/ false),
        ConvertInputChunkReadLimit(inputChunk->UpperLimit(), rowBuffer, comparator, /*isUpper*/ true));
}

TInputChunkSlicePtr CreateInputChunkSlice(const TInputChunkSlice& inputSlice)
{
    return New<TInputChunkSlice>(inputSlice);
}

TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    const TComparator& comparator,
    TKeyBound lowerKeyBound,
    TKeyBound upperKeyBound)
{
    return New<TInputChunkSlice>(inputSlice, comparator, lowerKeyBound, upperKeyBound);
}

TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NProto::TChunkSpec& protoChunkSpec,
    const TComparator& comparator)
{
    return New<TInputChunkSlice>(inputChunk, rowBuffer, protoChunkSpec, comparator);
}

std::vector<TInputChunkSlicePtr> CreateInputChunkSlicesFromCompleteErasureChunk(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId)
{
    YT_VERIFY(inputChunk->IsCompleteChunk());

    std::vector<TInputChunkSlicePtr> slices;

    i64 dataSize = inputChunk->GetUncompressedDataSize();
    i64 compressedDataSize = inputChunk->GetCompressedDataSize();
    i64 uncompressedDataSize = inputChunk->GetUncompressedDataSize();
    i64 rowCount = inputChunk->GetRowCount();

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceLowerRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceUpperRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            i64 partDataWeight = (dataSize + dataPartCount - 1) / dataPartCount;
            i64 partCompressedDataSize = (compressedDataSize + dataPartCount - 1) / dataPartCount;
            i64 partUncompressedDataSize = (uncompressedDataSize + dataPartCount - 1) / dataPartCount;

            auto chunkSlice = CreateInputChunkSliceFromCompleteErasureChunkPart(
                inputChunk,
                partIndex,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                partDataWeight,
                partCompressedDataSize,
                partUncompressedDataSize);
            slices.emplace_back(std::move(chunkSlice));
        }
    }

    return slices;
}

void InferLimitsFromBoundaryKeys(
    const TInputChunkSlicePtr& chunkSlice,
    const TRowBufferPtr& rowBuffer,
    std::optional<int> keyColumnCount,
    TComparator comparator)
{
    if (const auto& boundaryKeys = chunkSlice->GetInputChunk()->BoundaryKeys()) {
        YT_VERIFY(comparator);
        if (boundaryKeys->MinKey) {
            auto minKey = keyColumnCount
                ? GetStrictKey(boundaryKeys->MinKey, *keyColumnCount, rowBuffer)
                : boundaryKeys->MinKey;
            auto chunkLowerBound = KeyBoundFromLegacyRow(minKey, /*isUpper*/ false, comparator.GetLength(), rowBuffer);
            if (comparator.StrongerKeyBound(chunkSlice->LowerLimit().KeyBound, chunkLowerBound) == chunkLowerBound) {
                chunkLowerBound.Prefix = rowBuffer->CaptureRow(chunkLowerBound.Prefix);
                chunkSlice->LowerLimit().KeyBound = chunkLowerBound;
            }
        }
        if (boundaryKeys->MaxKey) {
            auto maxKey = keyColumnCount
                ? GetStrictKeySuccessor(boundaryKeys->MaxKey, *keyColumnCount, rowBuffer)
                : GetKeySuccessor(boundaryKeys->MaxKey, rowBuffer);
            auto chunkUpperBound = KeyBoundFromLegacyRow(maxKey, /*isUpper*/ true, comparator.GetLength(), rowBuffer);
            if (comparator.StrongerKeyBound(chunkSlice->UpperLimit().KeyBound, chunkUpperBound) == chunkUpperBound) {
                chunkUpperBound.Prefix = rowBuffer->CaptureRow(chunkUpperBound.Prefix);
                chunkSlice->UpperLimit().KeyBound = chunkUpperBound;
            }
        }
    }
}

void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkSlicePtr& inputSlice, TComparator comparator, EDataSourceType dataSourceType)
{
    // The chunk spec in the slice has arrived from master, so it can't possibly contain any extensions
    // except misc and boundary keys (in sorted merge or reduce). Jobs request boundary keys
    // from the nodes when needed, so we remove it here, to optimize traffic from the scheduler and
    // proto serialization time.

    ToProto(chunkSpec, inputSlice->GetInputChunk());

    // TODO(max42): YT-13961. Revise this logic.
    // TODO(max42): YT-14023. NB: right now we MUST keep pruning key bounds that are implied by chunk boundary keys
    // as failure to do so would break readers when reducing by shorter key than present in chunk schema.
    // Do not remove this logic unless there are no more nodes on 20.3.

    auto chunkMinKeyBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
    auto chunkMaxKeyBound = TKeyBound::MakeUniversal(/*isUpper*/ true);

    // NB: For dynamic table data slices involving dynamic stores boundary keys may contain sentinels.
    // But we do not prune limits for them anyway.
    if (const auto& boundaryKeys = inputSlice->GetInputChunk()->BoundaryKeys();
        boundaryKeys && dataSourceType == EDataSourceType::UnversionedTable)
    {
        chunkMinKeyBound = TKeyBound::FromRow(boundaryKeys->MinKey, /*isInclusive*/ true, /*isUpper*/ false);
        chunkMaxKeyBound = TKeyBound::FromRow(boundaryKeys->MaxKey, /*isInclusive*/ true, /*isUpper*/ true);
    }

    // NB: We prune non-trivial key bounds only if comparator is passed.
    // In particular, sorted controller always passes comparator. In the rest
    // of cases we do not prune it but it will not trigger YT-14023 as key lengths
    // will be proper (due to marvelous coincedence).

    if (!inputSlice->LowerLimit().IsTrivial()) {
        auto lowerLimitToSerialize = inputSlice->LowerLimit();
        if (!inputSlice->LowerLimit().KeyBound || inputSlice->LowerLimit().KeyBound.IsUniversal() ||
            (dataSourceType == EDataSourceType::UnversionedTable && comparator &&
            comparator.CompareKeyBounds(inputSlice->LowerLimit().KeyBound, chunkMinKeyBound) <= 0))
        {
            lowerLimitToSerialize.KeyBound = TKeyBound();
        }
        ToProto(chunkSpec->mutable_lower_limit(), lowerLimitToSerialize);
    }

    if (!inputSlice->UpperLimit().IsTrivial()) {
        auto upperLimitToSerialize = inputSlice->UpperLimit();
        if (!inputSlice->UpperLimit().KeyBound || inputSlice->UpperLimit().KeyBound.IsUniversal() ||
            (dataSourceType == EDataSourceType::UnversionedTable &&
            comparator && comparator.CompareKeyBounds(inputSlice->UpperLimit().KeyBound, chunkMaxKeyBound) >= 0))
        {
            upperLimitToSerialize.KeyBound = TKeyBound();
        }
        ToProto(chunkSpec->mutable_upper_limit(), upperLimitToSerialize);
    }

    chunkSpec->set_data_weight_override(inputSlice->GetDataWeight());

    // NB(psushin): always setting row_count_override is important for GetJobInputPaths handle to work properly.
    chunkSpec->set_row_count_override(inputSlice->GetRowCount());

    chunkSpec->set_compressed_data_size_override(inputSlice->GetCompressedDataSize());
    chunkSpec->set_uncompressed_data_size_override(inputSlice->GetUncompressedDataSize());
    // COMPAT(apollo1321): Remove in 26.1.
    chunkSpec->set_use_new_override_semantics(true);

    if (inputSlice->GetInputChunk()->IsDynamicStore()) {
        SetTabletId(chunkSpec, inputSlice->GetInputChunk()->GetTabletId());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
