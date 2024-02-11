#include "input_chunk_slice.h"

#include "private.h"
#include "input_chunk.h"

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/serialize.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/ytree/fluent.h>

#include <cmath>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLegacyInputSliceLimit::TLegacyInputSliceLimit(const TLegacyReadLimit& other)
{
    YT_VERIFY(!other.HasChunkIndex());
    YT_VERIFY(!other.HasOffset());
    if (other.HasRowIndex()) {
        RowIndex = other.GetRowIndex();
    }
    if (other.HasLegacyKey()) {
        Key = other.GetLegacyKey();
    }
}

TLegacyInputSliceLimit::TLegacyInputSliceLimit(
    const NProto::TReadLimit& other,
    const TRowBufferPtr& rowBuffer,
    TRange<TLegacyKey> keySet)
{
    YT_VERIFY(!other.has_chunk_index());
    YT_VERIFY(!other.has_offset());
    if (other.has_row_index()) {
        RowIndex = other.row_index();
    }
    if (other.has_legacy_key()) {
        NTableClient::FromProto(&Key, other.legacy_key(), rowBuffer);
    }
    if (other.has_key_index()) {
        Key = rowBuffer->CaptureRow(keySet[other.key_index()]);
    }
}

void TLegacyInputSliceLimit::MergeLowerRowIndex(i64 rowIndex)
{
    if (!RowIndex || *RowIndex < rowIndex) {
        RowIndex = rowIndex;
    }
}

void TLegacyInputSliceLimit::MergeUpperRowIndex(i64 rowIndex)
{
    if (!RowIndex || *RowIndex > rowIndex) {
        RowIndex = rowIndex;
    }
}

void TLegacyInputSliceLimit::MergeLowerKey(NTableClient::TLegacyKey key)
{
    if (!Key || Key < key) {
        Key = key;
    }
}

void TLegacyInputSliceLimit::MergeUpperKey(NTableClient::TLegacyKey key)
{
    if (!Key || Key > key) {
        Key = key;
    }
}

void TLegacyInputSliceLimit::MergeLowerLimit(const TLegacyInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        MergeLowerRowIndex(*limit.RowIndex);
    }
    if (limit.Key) {
        MergeLowerKey(limit.Key);
    }
}

void TLegacyInputSliceLimit::MergeUpperLimit(const TLegacyInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        MergeUpperRowIndex(*limit.RowIndex);
    }
    if (limit.Key) {
        MergeUpperKey(limit.Key);
    }
}

void TLegacyInputSliceLimit::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, RowIndex);
    Persist(context, Key);
}

TString ToString(const TLegacyInputSliceLimit& limit)
{
    return Format("RowIndex: %v, Key: %v", limit.RowIndex, limit.Key);
}

void FormatValue(TStringBuilderBase* builder, const TLegacyInputSliceLimit& limit, TStringBuf /*format*/)
{
    builder->AppendFormat("{RowIndex: %v, Key: %v}",
        limit.RowIndex,
        limit.Key);
}

bool IsTrivial(const TLegacyInputSliceLimit& limit)
{
    return !limit.RowIndex && !limit.Key;
}

void ToProto(NProto::TReadLimit* protoLimit, const TLegacyInputSliceLimit& limit)
{
    if (limit.RowIndex) {
        protoLimit->set_row_index(*limit.RowIndex);
    } else {
        protoLimit->clear_row_index();
    }

    if (limit.Key) {
        ToProto(protoLimit->mutable_legacy_key(), limit.Key);
    } else {
        protoLimit->clear_legacy_key();
    }
}

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
        if (other.has_key_bound_prefix()) {
            TUnversionedOwningRow row;
            NTableClient::FromProto(&row, other.key_bound_prefix());
            KeyBound.Prefix = row;
            KeyBound.IsUpper = isUpper;
            KeyBound.IsInclusive = other.has_key_bound_is_inclusive();
        } else if (other.has_legacy_key()) {
            TUnversionedOwningRow row;
            NTableClient::FromProto(&row, other.legacy_key());
            KeyBound = KeyBoundFromLegacyRow(row, isUpper, keyLength, rowBuffer);
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
        YT_VERIFY(!other.KeyBound);
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
        YT_VERIFY(!other.KeyBound);
    }
    YT_VERIFY(!KeyBound || KeyBound.IsUpper);
}

bool TInputSliceLimit::IsTrivial() const
{
    return (!KeyBound || KeyBound.IsUniversal()) && !RowIndex;
}

void TInputSliceLimit::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, RowIndex);
    Persist(context, KeyBound);
}

void Serialize(const TInputSliceLimit& limit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("key_bound", limit.KeyBound)
            .OptionalItem("row_index", limit.RowIndex)
        .EndMap();
}

TString ToString(const TInputSliceLimit& limit)
{
    return ToStringViaBuilder(limit);
}

void FormatValue(TStringBuilderBase* builder, const TInputSliceLimit& limit, TStringBuf /*format*/)
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

////////////////////////////////////////////////////////////////////////////////

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    TLegacyKey lowerKey,
    TLegacyKey upperKey)
    : InputChunk_(inputChunk)
    , DataWeight_(inputChunk->GetDataWeight())
    , RowCount_(inputChunk->GetRowCount())
{
    if (inputChunk->LowerLimit()) {
        LegacyLowerLimit_ = TLegacyInputSliceLimit(*inputChunk->LowerLimit());
    }
    if (lowerKey) {
        LegacyLowerLimit_.MergeLowerKey(lowerKey);
    }

    if (inputChunk->UpperLimit()) {
        LegacyUpperLimit_ = TLegacyInputSliceLimit(*inputChunk->UpperLimit());
    }
    if (upperKey) {
        LegacyUpperLimit_.MergeUpperKey(upperKey);
    }
}

TInputChunkSlice::TInputChunkSlice(const TInputChunkSlice& inputSlice)
    : InputChunk_(inputSlice.GetInputChunk())
    , SliceIndex_(inputSlice.GetSliceIndex())
    , IsLegacy(inputSlice.IsLegacy)
    , PartIndex_(inputSlice.GetPartIndex())
    , SizeOverridden_(inputSlice.GetSizeOverridden())
    , DataWeight_(inputSlice.GetDataWeight())
    , RowCount_(inputSlice.GetRowCount())
{
    if (inputSlice.IsLegacy) {
        LegacyLowerLimit_ = inputSlice.LegacyLowerLimit_;
        LegacyUpperLimit_ = inputSlice.LegacyUpperLimit_;
    } else {
        LowerLimit_ = inputSlice.LowerLimit_;
        UpperLimit_ = inputSlice.UpperLimit_;
    }
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    TLegacyKey lowerKey,
    TLegacyKey upperKey)
    : InputChunk_(inputSlice.GetInputChunk())
    , LegacyLowerLimit_(inputSlice.LegacyLowerLimit())
    , LegacyUpperLimit_(inputSlice.LegacyUpperLimit())
    , SliceIndex_(inputSlice.GetSliceIndex())
    , IsLegacy(inputSlice.IsLegacy)
    , PartIndex_(inputSlice.GetPartIndex())
    , SizeOverridden_(inputSlice.GetSizeOverridden())
    , DataWeight_(inputSlice.GetDataWeight())
    , RowCount_(inputSlice.GetRowCount())
{
    YT_VERIFY(inputSlice.IsLegacy);

    if (lowerKey) {
        LegacyLowerLimit_.MergeLowerKey(lowerKey);
    }
    if (upperKey) {
        LegacyUpperLimit_.MergeUpperKey(upperKey);
    }
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    const TComparator& comparator,
    TKeyBound lowerKeyBound,
    TKeyBound upperKeyBound)
    : InputChunk_(inputSlice.GetInputChunk())
    , LowerLimit_(inputSlice.LowerLimit())
    , UpperLimit_(inputSlice.UpperLimit())
    , SliceIndex_(inputSlice.GetSliceIndex())
    , IsLegacy(false)
    , PartIndex_(inputSlice.GetPartIndex())
    , SizeOverridden_(inputSlice.GetSizeOverridden())
    , DataWeight_(inputSlice.GetDataWeight())
    , RowCount_(inputSlice.GetRowCount())
{
    YT_VERIFY(!inputSlice.IsLegacy);

    LowerLimit_.KeyBound = comparator.StrongerKeyBound(LowerLimit_.KeyBound, lowerKeyBound);
    UpperLimit_.KeyBound = comparator.StrongerKeyBound(UpperLimit_.KeyBound, upperKeyBound);
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& chunkSlice,
    i64 lowerRowIndex,
    std::optional<i64> upperRowIndex,
    i64 dataWeight)
    : InputChunk_(chunkSlice.GetInputChunk())
    , LegacyLowerLimit_(chunkSlice.LegacyLowerLimit())
    , LegacyUpperLimit_(chunkSlice.LegacyUpperLimit())
    , LowerLimit_(chunkSlice.LowerLimit())
    , UpperLimit_(chunkSlice.UpperLimit())
    , SliceIndex_(chunkSlice.GetSliceIndex())
    , IsLegacy(chunkSlice.IsLegacy)
{
    if (IsLegacy) {
        LegacyLowerLimit_.RowIndex = lowerRowIndex;
        LegacyUpperLimit_.RowIndex = upperRowIndex;
    } else {
        LowerLimit_.RowIndex = lowerRowIndex;
        UpperLimit_.RowIndex = upperRowIndex;
    }

    if (upperRowIndex) {
        OverrideSize(*upperRowIndex - lowerRowIndex, dataWeight);
    }
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    int partIndex,
    i64 lowerRowIndex,
    std::optional<i64> upperRowIndex,
    i64 dataWeight)
    : InputChunk_(inputChunk)
    , PartIndex_(partIndex)
{
    if (inputChunk->LowerLimit()) {
        LegacyLowerLimit_ = TLegacyInputSliceLimit(*inputChunk->LowerLimit());
    }
    LegacyLowerLimit_.MergeLowerRowIndex(lowerRowIndex);

    if (inputChunk->UpperLimit()) {
        LegacyUpperLimit_ = TLegacyInputSliceLimit(*inputChunk->UpperLimit());
    }
    if (upperRowIndex) {
        LegacyUpperLimit_.MergeUpperRowIndex(*upperRowIndex);
    }

    if (LegacyUpperLimit_.RowIndex) {
        OverrideSize(*LegacyUpperLimit_.RowIndex - *LegacyLowerLimit_.RowIndex, std::max<i64>(1, dataWeight * inputChunk->GetColumnSelectivityFactor()));
    }
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSlice& protoChunkSlice,
    TRange<TLegacyKey> keySet)
    : TInputChunkSlice(inputChunk)
{
    LegacyLowerLimit_.MergeLowerLimit(TLegacyInputSliceLimit(protoChunkSlice.lower_limit(), rowBuffer, keySet));
    LegacyUpperLimit_.MergeUpperLimit(TLegacyInputSliceLimit(protoChunkSlice.upper_limit(), rowBuffer, keySet));
    PartIndex_ = DefaultPartIndex;

    if (protoChunkSlice.has_row_count_override() || protoChunkSlice.has_data_weight_override()) {
        YT_VERIFY((protoChunkSlice.has_row_count_override() && protoChunkSlice.has_data_weight_override()));
        OverrideSize(protoChunkSlice.row_count_override(), std::max<i64>(1, protoChunkSlice.data_weight_override() * inputChunk->GetColumnSelectivityFactor()));
    }
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkSlice& chunkSlice,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSlice& protoChunkSlice,
    TRange<TLegacyKey> keySet)
    : InputChunk_(chunkSlice.GetInputChunk())
    , LegacyLowerLimit_(chunkSlice.LegacyLowerLimit())
    , LegacyUpperLimit_(chunkSlice.LegacyUpperLimit())
    , SliceIndex_(chunkSlice.GetSliceIndex())
    , IsLegacy(chunkSlice.IsLegacy)
{
    YT_VERIFY(chunkSlice.IsLegacy);
    LegacyLowerLimit_.MergeLowerLimit(TLegacyInputSliceLimit(protoChunkSlice.lower_limit(), rowBuffer, keySet));
    LegacyUpperLimit_.MergeUpperLimit(TLegacyInputSliceLimit(protoChunkSlice.upper_limit(), rowBuffer, keySet));

    PartIndex_ = DefaultPartIndex;

    if (protoChunkSlice.has_row_count_override() || protoChunkSlice.has_data_weight_override()) {
        YT_VERIFY(protoChunkSlice.has_row_count_override() && protoChunkSlice.has_data_weight_override());
        OverrideSize(protoChunkSlice.row_count_override(), std::max<i64>(1, protoChunkSlice.data_weight_override() * chunkSlice.GetInputChunk()->GetColumnSelectivityFactor()));
    }
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
    , IsLegacy(chunkSlice.IsLegacy)
{
    YT_VERIFY(!chunkSlice.IsLegacy);
    LowerLimit_.MergeLower(TInputSliceLimit(protoChunkSlice.lower_limit(), rowBuffer, keySet, keyBoundPrefixes, comparator.GetLength(), /*isUpper*/ false), comparator);
    UpperLimit_.MergeUpper(TInputSliceLimit(protoChunkSlice.upper_limit(), rowBuffer, keySet, keyBoundPrefixes, comparator.GetLength(), /*isUpper*/ true), comparator);

    PartIndex_ = DefaultPartIndex;

    if (protoChunkSlice.has_row_count_override() || protoChunkSlice.has_data_weight_override()) {
        YT_VERIFY(protoChunkSlice.has_row_count_override() && protoChunkSlice.has_data_weight_override());
        OverrideSize(protoChunkSlice.row_count_override(), std::max<i64>(1, protoChunkSlice.data_weight_override() * chunkSlice.GetInputChunk()->GetColumnSelectivityFactor()));
    }
}

TInputChunkSlice::TInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const TRowBufferPtr& rowBuffer,
    const NProto::TChunkSpec& protoChunkSpec)
    : TInputChunkSlice(inputChunk)
{
    static TRange<TLegacyKey> DummyKeys;
    LegacyLowerLimit_.MergeLowerLimit(TLegacyInputSliceLimit(protoChunkSpec.lower_limit(), rowBuffer, DummyKeys));
    LegacyUpperLimit_.MergeUpperLimit(TLegacyInputSliceLimit(protoChunkSpec.upper_limit(), rowBuffer, DummyKeys));
    PartIndex_ = DefaultPartIndex;

    if (protoChunkSpec.has_row_count_override() || protoChunkSpec.has_data_weight_override()) {
        YT_VERIFY((protoChunkSpec.has_row_count_override() && protoChunkSpec.has_data_weight_override()));
        OverrideSize(protoChunkSpec.row_count_override(), std::max<i64>(1, protoChunkSpec.data_weight_override() * inputChunk->GetColumnSelectivityFactor()));
    }
}

std::vector<TInputChunkSlicePtr> TInputChunkSlice::SliceEvenly(i64 sliceDataWeight, i64 sliceRowCount, TRowBufferPtr rowBuffer) const
{
    YT_VERIFY(sliceDataWeight > 0);
    YT_VERIFY(sliceRowCount > 0);
    YT_VERIFY(!InputChunk_->IsSortedDynamicStore());

    if (InputChunk_->IsOrderedDynamicStore() && !UpperLimit_.RowIndex) {
        YT_VERIFY(!LegacyLowerLimit_.Key);
        YT_VERIFY(!LegacyUpperLimit_.Key);
        return {New<TInputChunkSlice>(*this)};
    }

    i64 lowerRowIndex;
    i64 upperRowIndex;

    if (IsLegacy) {
        lowerRowIndex = LegacyLowerLimit_.RowIndex.value_or(0);
        upperRowIndex = LegacyUpperLimit_.RowIndex.value_or(InputChunk_->GetRowCount());
    } else {
        lowerRowIndex = LowerLimit_.RowIndex.value_or(0);
        upperRowIndex = UpperLimit_.RowIndex.value_or(InputChunk_->GetRowCount());
    }
    i64 rowCount = upperRowIndex - lowerRowIndex;

    if (rowCount == 0) {
        return {};
    }

    i64 count = std::max(DivCeil(GetDataWeight(), sliceDataWeight), DivCeil(rowCount, sliceRowCount));
    // NB(gepardo): We need to consider cases with count == 0 or rowCount == 0 carefully. The
    // latter case is considered above. In the former case, we have non-empty data and need one
    // slice, so forcefully set count to 1.
    count = std::max(std::min(count, rowCount), static_cast<i64>(1));

    std::vector<TInputChunkSlicePtr> result;
    for (i64 i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = lowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = lowerRowIndex + rowCount * (i + 1) / count;
        i64 sliceLowerDataWeight = GetDataWeight() * i / count;
        i64 sliceUpperDataWeight = GetDataWeight() * (i + 1) / count;
        YT_VERIFY(sliceLowerRowIndex < sliceUpperRowIndex);
        result.push_back(New<TInputChunkSlice>(
            *this,
            sliceLowerRowIndex,
            sliceUpperRowIndex,
            sliceUpperDataWeight - sliceLowerDataWeight));
    }
    if (rowBuffer) {
        result.front()
            ->LegacyLowerLimit().Key = rowBuffer->CaptureRow(LegacyLowerLimit_.Key);
        result.back()
            ->LegacyUpperLimit().Key = rowBuffer->CaptureRow(LegacyUpperLimit_.Key);
    }

    for (const auto& slice : result) {
        YT_VERIFY(slice->IsLegacy == IsLegacy);
    }

    return result;
}

std::pair<TInputChunkSlicePtr, TInputChunkSlicePtr> TInputChunkSlice::SplitByRowIndex(i64 splitRow) const
{
    i64 lowerRowIndex;
    i64 upperRowIndex;
    if (IsLegacy) {
        lowerRowIndex = LegacyLowerLimit_.RowIndex.value_or(0);
        upperRowIndex = LegacyUpperLimit_.RowIndex.value_or(InputChunk_->GetRowCount());
    } else {
        lowerRowIndex = LowerLimit_.RowIndex.value_or(0);
        upperRowIndex = UpperLimit_.RowIndex.value_or(InputChunk_->GetRowCount());
    }

    YT_VERIFY(!InputChunk_->IsSortedDynamicStore());
    YT_VERIFY(!InputChunk_->IsOrderedDynamicStore() || UpperLimit_.RowIndex);

    i64 rowCount = upperRowIndex - lowerRowIndex;

    YT_VERIFY(splitRow >= 0 && splitRow <= rowCount);

    return std::pair(
        New<TInputChunkSlice>(
            *this,
            lowerRowIndex,
            lowerRowIndex + splitRow,
            std::max<i64>(1, GetDataWeight() * 1.0 / rowCount * splitRow)),
        New<TInputChunkSlice>(
            *this,
            lowerRowIndex + splitRow,
            upperRowIndex,
            std::max<i64>(1, GetDataWeight() * 1.0 / rowCount * (rowCount - splitRow))));
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

void TInputChunkSlice::OverrideSize(i64 rowCount, i64 dataWeight)
{
    RowCount_ = rowCount;
    DataWeight_ = dataWeight;
    SizeOverridden_ = true;
}

void TInputChunkSlice::ApplySamplingSelectivityFactor(double samplingSelectivityFactor)
{
    i64 rowCount = std::max<i64>(1, GetRowCount() * samplingSelectivityFactor);
    i64 dataWeight = std::max<i64>(1, GetDataWeight() * samplingSelectivityFactor);
    OverrideSize(rowCount, dataWeight);
}

void TInputChunkSlice::TransformToLegacy(const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(!IsLegacy);

    LegacyLowerLimit_.RowIndex = LowerLimit_.RowIndex;
    if (LowerLimit_.KeyBound.IsUniversal()) {
        LegacyLowerLimit_.Key = TLegacyKey();
    } else {
        LegacyLowerLimit_.Key = KeyBoundToLegacyRow(LowerLimit_.KeyBound, rowBuffer);
    }
    LegacyUpperLimit_.RowIndex = UpperLimit_.RowIndex;
    if (UpperLimit_.KeyBound.IsUniversal()) {
        LegacyUpperLimit_.Key = TLegacyKey();
    } else {
        LegacyUpperLimit_.Key = KeyBoundToLegacyRow(UpperLimit_.KeyBound, rowBuffer);
    }
    LowerLimit_ = TInputSliceLimit();
    UpperLimit_ = TInputSliceLimit();

    IsLegacy = true;
}

void TInputChunkSlice::TransformToNew(const TRowBufferPtr& rowBuffer, std::optional<int> keyLength)
{
    YT_VERIFY(IsLegacy);

    auto getKeyLength = [&] (const TLegacyKey& key) {
        if (keyLength) {
            return *keyLength;
        }

        return key ? static_cast<int>(key.GetCount()) : 0;
    };

    LowerLimit_.RowIndex = LegacyLowerLimit_.RowIndex;
    LowerLimit_.KeyBound = KeyBoundFromLegacyRow(LegacyLowerLimit_.Key, /*isUpper*/ false, getKeyLength(LegacyLowerLimit_.Key), rowBuffer);
    UpperLimit_.RowIndex = LegacyUpperLimit_.RowIndex;
    UpperLimit_.KeyBound = KeyBoundFromLegacyRow(LegacyUpperLimit_.Key, /*isUpper*/ true, getKeyLength(LegacyUpperLimit_.Key), rowBuffer);
    LegacyLowerLimit_ = TLegacyInputSliceLimit();
    LegacyUpperLimit_ = TLegacyInputSliceLimit();

    IsLegacy = false;
}

void TInputChunkSlice::TransformToNewKeyless()
{
    YT_VERIFY(IsLegacy);
    YT_VERIFY(!LegacyLowerLimit_.Key);
    YT_VERIFY(!LegacyUpperLimit_.Key);
    LowerLimit_.RowIndex = LegacyLowerLimit_.RowIndex;
    UpperLimit_.RowIndex = LegacyUpperLimit_.RowIndex;
    LegacyLowerLimit_ = TLegacyInputSliceLimit();
    LegacyUpperLimit_ = TLegacyInputSliceLimit();

    IsLegacy = false;
}

void TInputChunkSlice::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, InputChunk_);
    Persist(context, LegacyLowerLimit_);
    Persist(context, LegacyUpperLimit_);
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, IsLegacy);
    Persist(context, PartIndex_);
    Persist(context, SizeOverridden_);
    Persist(context, RowCount_);
    Persist(context, DataWeight_);
    Persist(context, SliceIndex_);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TInputChunkSlicePtr& slice)
{
    return Format("ChunkId: %v, LowerLimit: %v, UpperLimit: %v, RowCount: %v, DataWeight: %v, PartIndex: %v",
        slice->GetInputChunk()->GetChunkId(),
        slice->IsLegacy ? ToString(slice->LegacyLowerLimit()) : ToString(slice->LowerLimit()),
        slice->IsLegacy ? ToString(slice->LegacyUpperLimit()) : ToString(slice->UpperLimit()),
        slice->GetRowCount(),
        slice->GetDataWeight(),
        slice->GetPartIndex());
}

////////////////////////////////////////////////////////////////////////////////

TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    TLegacyKey lowerKey,
    TLegacyKey upperKey)
{
    return New<TInputChunkSlice>(inputChunk, lowerKey, upperKey);
}

TInputChunkSlicePtr CreateInputChunkSlice(const TInputChunkSlice& inputSlice)
{
    return New<TInputChunkSlice>(inputSlice);
}

TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    TLegacyKey lowerKey,
    TLegacyKey upperKey)
{
    return New<TInputChunkSlice>(inputSlice, lowerKey, upperKey);
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
    const NProto::TChunkSpec& protoChunkSpec)
{
    return New<TInputChunkSlice>(inputChunk, rowBuffer, protoChunkSpec);
}

std::vector<TInputChunkSlicePtr> CreateErasureInputChunkSlices(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId)
{
    std::vector<TInputChunkSlicePtr> slices;

    i64 dataSize = inputChunk->GetUncompressedDataSize();
    i64 rowCount = inputChunk->GetRowCount();

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceLowerRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceUpperRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto chunkSlice = New<TInputChunkSlice>(
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

void InferLimitsFromBoundaryKeys(
    const TInputChunkSlicePtr& chunkSlice,
    const TRowBufferPtr& rowBuffer,
    std::optional<int> keyColumnCount,
    TComparator comparator)
{
    if (chunkSlice->IsLegacy) {
        if (const auto& boundaryKeys = chunkSlice->GetInputChunk()->BoundaryKeys()) {
            if (keyColumnCount) {
                chunkSlice->LegacyLowerLimit().MergeLowerKey(GetStrictKey(boundaryKeys->MinKey, *keyColumnCount, rowBuffer));
                chunkSlice->LegacyUpperLimit().MergeUpperKey(GetStrictKeySuccessor(boundaryKeys->MaxKey, *keyColumnCount, rowBuffer));
            } else {
                chunkSlice->LegacyLowerLimit().MergeLowerKey(boundaryKeys->MinKey);
                chunkSlice->LegacyUpperLimit().MergeUpperKey(GetKeySuccessor(boundaryKeys->MaxKey, rowBuffer));
            }
        }
    } else {
        if (const auto& boundaryKeys = chunkSlice->GetInputChunk()->BoundaryKeys()) {
            YT_VERIFY(comparator);
            auto chunkLowerBound = KeyBoundFromLegacyRow(boundaryKeys->MinKey, /*isUpper*/ false, comparator.GetLength(), rowBuffer);
            auto chunkUpperBound = KeyBoundFromLegacyRow(GetKeySuccessor(boundaryKeys->MaxKey, rowBuffer), /*isUpper*/ true, comparator.GetLength(), rowBuffer);
            if (comparator.StrongerKeyBound(chunkSlice->LowerLimit().KeyBound, chunkLowerBound) == chunkLowerBound) {
                chunkLowerBound.Prefix = rowBuffer->CaptureRow(chunkLowerBound.Prefix);
                chunkSlice->LowerLimit().KeyBound = chunkLowerBound;
            }
            if (comparator.StrongerKeyBound(chunkSlice->UpperLimit().KeyBound, chunkUpperBound) == chunkUpperBound) {
                chunkUpperBound.Prefix = rowBuffer->CaptureRow(chunkUpperBound.Prefix);
                chunkSlice->UpperLimit().KeyBound = chunkUpperBound;
            }
        }
    }
}

std::vector<TInputChunkSlicePtr> SliceChunkByRowIndexes(
    const TInputChunkPtr& inputChunk,
    i64 sliceDataWeight,
    i64 sliceRowCount)
{
    return CreateInputChunkSlice(inputChunk)->SliceEvenly(sliceDataWeight, sliceRowCount);
}

void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkSlicePtr& inputSlice, TComparator comparator, EDataSourceType dataSourceType)
{
    // The chunk spec in the slice has arrived from master, so it can't possibly contain any extensions
    // except misc and boundary keys (in sorted merge or reduce). Jobs request boundary keys
    // from the nodes when needed, so we remove it here, to optimize traffic from the scheduler and
    // proto serialization time.

    ToProto(chunkSpec, inputSlice->GetInputChunk());

    if (inputSlice->IsLegacy) {
        if (!IsTrivial(inputSlice->LegacyLowerLimit())) {
            // NB(psushin): if lower limit key is less than min chunk key, we can eliminate it from job spec.
            // Moreover, it is important for GetJobInputPaths handle to work properly.
            bool pruneKeyLimit = dataSourceType == EDataSourceType::UnversionedTable
                && inputSlice->LegacyLowerLimit().Key
                && inputSlice->GetInputChunk()->BoundaryKeys()
                && inputSlice->LegacyLowerLimit().Key <= inputSlice->GetInputChunk()->BoundaryKeys()->MinKey;

            if (pruneKeyLimit && inputSlice->LegacyLowerLimit().RowIndex) {
                TLegacyInputSliceLimit inputSliceLimit;
                inputSliceLimit.RowIndex = inputSlice->LegacyLowerLimit().RowIndex;
                ToProto(chunkSpec->mutable_lower_limit(), inputSliceLimit);
            } else if (!pruneKeyLimit) {
                ToProto(chunkSpec->mutable_lower_limit(), inputSlice->LegacyLowerLimit());
            }
        }

        if (!IsTrivial(inputSlice->LegacyUpperLimit())) {
            // NB(psushin): if upper limit key is greater than max chunk key, we can eliminate it from job spec.
            // Moreover, it is important for GetJobInputPaths handle to work properly.
            bool pruneKeyLimit = dataSourceType == EDataSourceType::UnversionedTable
                && inputSlice->LegacyUpperLimit().Key
                && inputSlice->GetInputChunk()->BoundaryKeys()
                && inputSlice->LegacyUpperLimit().Key > inputSlice->GetInputChunk()->BoundaryKeys()->MaxKey;

            if (pruneKeyLimit && inputSlice->LegacyUpperLimit().RowIndex) {
                TLegacyInputSliceLimit inputSliceLimit;
                inputSliceLimit.RowIndex = inputSlice->LegacyUpperLimit().RowIndex;
                ToProto(chunkSpec->mutable_upper_limit(), inputSliceLimit);
            } else if (!pruneKeyLimit) {
                ToProto(chunkSpec->mutable_upper_limit(), inputSlice->LegacyUpperLimit());
            }
        }
    } else {
        // TODO(max42): YT-13961. Revise this logic.
        // TODO(max42): YT-14023. NB: right now we MUST keep pruning key bounds that are implied by chunk boundary keys
        // as failure to do so would break readers when reducing by shorter key than present in chunk schema.
        // Do not remove this logic unless there are no more nodes on 20.3.

        auto chunkMinKeyBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
        auto chunkMaxKeyBound = TKeyBound::MakeUniversal(/*isUpper*/ true);

        // NB: for dynamic table data slices involving dynamic stores boundary keys may contain sentinels.
        // But we do not prune limits for them anyway.
        if (const auto& boundaryKeys = inputSlice->GetInputChunk()->BoundaryKeys();
            boundaryKeys && dataSourceType == EDataSourceType::UnversionedTable)
        {
            chunkMinKeyBound = TKeyBound::FromRow(boundaryKeys->MinKey, /*isInclusive*/ true, /*isUpper*/ false);
            chunkMaxKeyBound = TKeyBound::FromRow(boundaryKeys->MaxKey, /*isInclusive*/ true, /*isUpper*/ true);
        }

        // NB: we prune non-trivial key bounds only if comparator is passed.
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
    }

    chunkSpec->set_data_weight_override(inputSlice->GetDataWeight());

    // NB(psushin): always setting row_count_override is important for GetJobInputPaths handle to work properly.
    chunkSpec->set_row_count_override(inputSlice->GetRowCount());
    if (inputSlice->GetInputChunk()->IsDynamicStore()) {
        SetTabletId(chunkSpec, inputSlice->GetInputChunk()->GetTabletId());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
