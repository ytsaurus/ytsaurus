#include "legacy_data_slice.h"
#include "chunk_spec.h"

#include <yt/yt/ytlib/table_client/virtual_value_directory.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/serialize.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLegacyDataSlice::TLegacyDataSlice(
    EDataSourceType type,
    TChunkSliceList chunkSlices,
    TLegacyInputSliceLimit lowerLimit,
    TLegacyInputSliceLimit upperLimit,
    std::optional<i64> tag)
    : LegacyLowerLimit_(std::move(lowerLimit))
    , LegacyUpperLimit_(std::move(upperLimit))
    , IsLegacy(true)
    , ChunkSlices(std::move(chunkSlices))
    , Type(type)
    , Tag(tag)
{
    for (const auto& chunkSlice : ChunkSlices) {
        YT_VERIFY(chunkSlice->IsLegacy);
    }
}

TLegacyDataSlice::TLegacyDataSlice(
    EDataSourceType type,
    TChunkSliceList chunkSlices,
    TInputSliceLimit lowerLimit,
    TInputSliceLimit upperLimit,
    std::optional<i64> tag)
    : LowerLimit_(lowerLimit)
    , UpperLimit_(upperLimit)
    , IsLegacy(false)
    , ChunkSlices(std::move(chunkSlices))
    , Type(type)
    , Tag(tag)
{
    for (const auto& chunkSlice : ChunkSlices) {
        YT_VERIFY(!chunkSlice->IsLegacy);
    }
}

int TLegacyDataSlice::GetChunkCount() const
{
    return ChunkSlices.size();
}

i64 TLegacyDataSlice::GetDataWeight() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetDataWeight();
    }
    return result;
}

i64 TLegacyDataSlice::GetRowCount() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetRowCount();
    }
    return result;
}

i64 TLegacyDataSlice::GetValueCount() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetValueCount();
    }
    return result;
}

i64 TLegacyDataSlice::GetMaxBlockSize() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result = std::max(result, chunkSlice->GetMaxBlockSize());
    }
    return result;
}

void TLegacyDataSlice::Persist(const NTableClient::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, IsLegacy);
    Persist(context, LegacyLowerLimit_);
    Persist(context, LegacyUpperLimit_);
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, ChunkSlices);
    Persist(context, Type);
    Persist(context, Tag);
    Persist(context, InputStreamIndex_);
    Persist(context, VirtualRowIndex);
    Persist(context, ReadRangeIndex);
    Persist(context, IsTeleportable);
}

int TLegacyDataSlice::GetTableIndex() const
{
    YT_VERIFY(ChunkSlices.size() > 0);
    return ChunkSlices[0]->GetInputChunk()->GetTableIndex();
}

int TLegacyDataSlice::GetRangeIndex() const
{
    YT_VERIFY(ChunkSlices.size() > 0);
    return ChunkSlices[0]->GetInputChunk()->GetRangeIndex();
}

TInputChunkPtr TLegacyDataSlice::GetSingleUnversionedChunk() const
{
    return GetSingleUnversionedChunkSlice()->GetInputChunk();
}

TInputChunkSlicePtr TLegacyDataSlice::GetSingleUnversionedChunkSlice() const
{
    YT_VERIFY(IsTrivial());

    return ChunkSlices[0];
}

bool TLegacyDataSlice::IsTrivial() const
{
    return Type == EDataSourceType::UnversionedTable && ChunkSlices.size() == 1;
}

bool TLegacyDataSlice::IsEmpty() const
{
    return LegacyLowerLimit_.Key && LegacyUpperLimit_.Key && LegacyLowerLimit_.Key >= LegacyUpperLimit_.Key;
}

bool TLegacyDataSlice::HasLimits() const
{
    if (IsLegacy) {
        return LegacyLowerLimit_.Key || LegacyLowerLimit_.RowIndex || LegacyUpperLimit_.Key || LegacyUpperLimit_.RowIndex;
    } else {
        return !LowerLimit_.IsTrivial() || !UpperLimit_.IsTrivial();
    }
}

std::pair<TLegacyDataSlicePtr, TLegacyDataSlicePtr> TLegacyDataSlice::SplitByRowIndex(i64 rowIndex) const
{
    YT_VERIFY(IsTrivial());
    auto slices = ChunkSlices[0]->SplitByRowIndex(rowIndex);

    auto first = CreateUnversionedInputDataSlice(slices.first);
    auto second = CreateUnversionedInputDataSlice(slices.second);

    // CreateUnversionedInputDataSlice infers key bounds both from chunk slice key bounds and
    // from data slice key bounds making resulting parts key bounds wider than our own key bounds.
    // This is undesired behavior for new sorted pool, so workaround that by simply copying
    // our them with our key bounds.
    first->LowerLimit().KeyBound = LowerLimit().KeyBound;
    first->UpperLimit().KeyBound = UpperLimit().KeyBound;
    second->LowerLimit().KeyBound = LowerLimit().KeyBound;
    second->UpperLimit().KeyBound = UpperLimit().KeyBound;

    first->CopyPayloadFrom(*this);
    second->CopyPayloadFrom(*this);

    return {std::move(first), std::move(second)};
}

void TLegacyDataSlice::CopyPayloadFrom(const TLegacyDataSlice& dataSlice)
{
    InputStreamIndex_ = dataSlice.InputStreamIndex_;
    Tag = dataSlice.Tag;
    VirtualRowIndex = dataSlice.VirtualRowIndex;
    ReadRangeIndex = dataSlice.ReadRangeIndex;
}

void TLegacyDataSlice::TransformToLegacy(const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(!IsLegacy);

    LegacyLowerLimit_.RowIndex = LowerLimit_.RowIndex;
    if (LowerLimit_.KeyBound.IsUniversal()) {
        LegacyLowerLimit_.Key = MinKey();
    } else {
        LegacyLowerLimit_.Key = KeyBoundToLegacyRow(LowerLimit_.KeyBound, rowBuffer);
    }
    LegacyUpperLimit_.RowIndex = UpperLimit_.RowIndex;
    if (UpperLimit_.KeyBound.IsUniversal()) {
        LegacyUpperLimit_.Key = MaxKey();
    } else {
        LegacyUpperLimit_.Key = KeyBoundToLegacyRow(UpperLimit_.KeyBound, rowBuffer);
    }
    LowerLimit_ = TInputSliceLimit();
    UpperLimit_ = TInputSliceLimit();

    for (auto& chunkSlice : ChunkSlices) {
        chunkSlice->TransformToLegacy(rowBuffer);
    }

    IsLegacy = true;
}

void TLegacyDataSlice::TransformToNew(const TRowBufferPtr& rowBuffer, int keyLength, bool trimChunkSliceKeys)
{
    YT_VERIFY(IsLegacy);

    LowerLimit_.RowIndex = LegacyLowerLimit_.RowIndex;
    LowerLimit_.KeyBound = KeyBoundFromLegacyRow(LegacyLowerLimit_.Key, /* isUpper */ false, keyLength, rowBuffer);
    UpperLimit_.RowIndex = LegacyUpperLimit_.RowIndex;
    UpperLimit_.KeyBound = KeyBoundFromLegacyRow(LegacyUpperLimit_.Key, /* isUpper */ true, keyLength, rowBuffer);
    LegacyLowerLimit_ = TLegacyInputSliceLimit();
    LegacyUpperLimit_ = TLegacyInputSliceLimit();

    std::optional<int> chunkSliceKeyLength;
    if (trimChunkSliceKeys) {
        chunkSliceKeyLength = keyLength;
    }
    for (auto& chunkSlice : ChunkSlices) {
        chunkSlice->TransformToNew(rowBuffer, chunkSliceKeyLength);
    }

    IsLegacy = false;
}

void TLegacyDataSlice::TransformToNewKeyless()
{
    YT_VERIFY(IsLegacy);
    YT_VERIFY(!LegacyLowerLimit_.Key);
    YT_VERIFY(!LegacyUpperLimit_.Key);
    LowerLimit_.RowIndex = LegacyLowerLimit_.RowIndex;
    UpperLimit_.RowIndex = LegacyUpperLimit_.RowIndex;
    LegacyLowerLimit_ = TLegacyInputSliceLimit();
    LegacyUpperLimit_ = TLegacyInputSliceLimit();

    for (auto& chunkSlice : ChunkSlices) {
        chunkSlice->TransformToNewKeyless();
    }

    IsLegacy = false;
}

void TLegacyDataSlice::TransformToNew(
    const NTableClient::TRowBufferPtr& rowBuffer,
    NTableClient::TComparator comparator)
{
    if (comparator) {
        TransformToNew(rowBuffer, comparator.GetLength());
    } else {
        TransformToNewKeyless();
    }
}

int TLegacyDataSlice::GetSliceIndex() const
{
    return Type == EDataSourceType::UnversionedTable
        ? ChunkSlices[0]->GetSliceIndex()
        : 0;
}

int TLegacyDataSlice::GetInputStreamIndex() const
{
    YT_VERIFY(InputStreamIndex_);
    return *InputStreamIndex_;
}

void TLegacyDataSlice::SetInputStreamIndex(int inputStreamIndex)
{
    InputStreamIndex_ = inputStreamIndex;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TLegacyDataSlicePtr& dataSlice, IYsonConsumer* consumer)
{
    YT_VERIFY(!dataSlice->IsLegacy);

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("lower_limit").Value(dataSlice->LowerLimit())
            .Item("upper_limit").Value(dataSlice->UpperLimit())
            .Item("input_stream_index").Value(dataSlice->GetInputStreamIndex())
            .OptionalItem("tag", dataSlice->Tag)
            .Item("slice_index").Value(dataSlice->GetSliceIndex())
            .Item("is_teleportable").Value(dataSlice->IsTeleportable)
            .Item("chunk_count").Value(dataSlice->GetChunkCount())
            .Item("data_weight").Value(dataSlice->GetDataWeight())
            .Item("row_count").Value(dataSlice->GetRowCount())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TLegacyDataSlicePtr& dataSlice)
{
    return Format("Type: %v, LowerLimit: %v, UpperLimit: %v, ChunkSlices: %v",
        dataSlice->Type,
        dataSlice->IsLegacy ? ToString(dataSlice->LegacyLowerLimit()) : ToString(dataSlice->LowerLimit()),
        dataSlice->IsLegacy ? ToString(dataSlice->LegacyUpperLimit()) : ToString(dataSlice->UpperLimit()),
        dataSlice->ChunkSlices);
}

////////////////////////////////////////////////////////////////////////////////

TLegacyDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkSlicePtr chunkSlice)
{
    if (chunkSlice->IsLegacy) {
        return New<TLegacyDataSlice>(
            EDataSourceType::UnversionedTable,
            TLegacyDataSlice::TChunkSliceList{chunkSlice},
            chunkSlice->LegacyLowerLimit(),
            chunkSlice->LegacyUpperLimit());
    } else {
        return New<TLegacyDataSlice>(
            EDataSourceType::UnversionedTable,
            TLegacyDataSlice::TChunkSliceList{chunkSlice},
            chunkSlice->LowerLimit(),
            chunkSlice->UpperLimit());
    }
}

TLegacyDataSlicePtr CreateVersionedInputDataSlice(const std::vector<TInputChunkSlicePtr>& inputChunkSlices)
{
    std::vector<TLegacyDataSlicePtr> dataSlices;

    YT_VERIFY(!inputChunkSlices.empty());
    TLegacyDataSlice::TChunkSliceList chunkSlices;
    std::optional<int> tableIndex;
    TLegacyInputSliceLimit lowerLimit;
    TLegacyInputSliceLimit upperLimit;
    for (const auto& inputChunkSlice : inputChunkSlices) {
        if (!tableIndex) {
            tableIndex = inputChunkSlice->GetInputChunk()->GetTableIndex();
            lowerLimit.Key = inputChunkSlice->LegacyLowerLimit().Key;
            upperLimit.Key = inputChunkSlice->LegacyUpperLimit().Key;
        } else {
            YT_VERIFY(*tableIndex == inputChunkSlice->GetInputChunk()->GetTableIndex());
            YT_VERIFY(lowerLimit.Key == inputChunkSlice->LegacyLowerLimit().Key);
            YT_VERIFY(upperLimit.Key == inputChunkSlice->LegacyUpperLimit().Key);
        }
        chunkSlices.push_back(inputChunkSlice);
    }
    return New<TLegacyDataSlice>(
        EDataSourceType::VersionedTable,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TLegacyDataSlicePtr CreateInputDataSlice(
    EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    TLegacyKey lowerKey,
    TLegacyKey upperKey)
{
    TLegacyDataSlice::TChunkSliceList chunkSlices;
    std::optional<int> tableIndex;
    for (const auto& inputChunk : inputChunks) {
        if (!tableIndex) {
            tableIndex = inputChunk->GetInputChunk()->GetTableIndex();
        } else {
            YT_VERIFY(*tableIndex == inputChunk->GetInputChunk()->GetTableIndex());
        }
        chunkSlices.push_back(CreateInputChunkSlice(*inputChunk, lowerKey, upperKey));
    }

    TLegacyInputSliceLimit lowerLimit;
    lowerLimit.Key = lowerKey;

    TLegacyInputSliceLimit upperLimit;
    upperLimit.Key = upperKey;

    return New<TLegacyDataSlice>(
        type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TLegacyDataSlicePtr CreateInputDataSlice(
    NChunkClient::EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    const TComparator& comparator,
    TKeyBound lowerBound,
    TKeyBound upperBound)
{
    TLegacyDataSlice::TChunkSliceList chunkSlices;
    std::optional<int> tableIndex;
    for (const auto& inputChunk : inputChunks) {
        if (!tableIndex) {
            tableIndex = inputChunk->GetInputChunk()->GetTableIndex();
        } else {
            YT_VERIFY(*tableIndex == inputChunk->GetInputChunk()->GetTableIndex());
        }
        chunkSlices.push_back(CreateInputChunkSlice(*inputChunk, comparator, lowerBound, upperBound));
    }

    TInputSliceLimit lowerLimit;
    lowerLimit.KeyBound = lowerBound;

    TInputSliceLimit upperLimit;
    upperLimit.KeyBound = upperBound;

    return New<TLegacyDataSlice>(
        type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TLegacyDataSlicePtr CreateInputDataSlice(const TLegacyDataSlicePtr& dataSlice)
{
    TLegacyDataSlice::TChunkSliceList chunkSlices;
    for (const auto& slice : dataSlice->ChunkSlices) {
        chunkSlices.push_back(CreateInputChunkSlice(*slice));
    }

    TLegacyDataSlicePtr newDataSlice;

    if (dataSlice->IsLegacy) {
        newDataSlice = New<TLegacyDataSlice>(
            dataSlice->Type,
            std::move(chunkSlices),
            dataSlice->LegacyLowerLimit(),
            dataSlice->LegacyUpperLimit(),
            dataSlice->Tag);
    } else {
        newDataSlice = New<TLegacyDataSlice>(
            dataSlice->Type,
            std::move(chunkSlices),
            dataSlice->LowerLimit(),
            dataSlice->UpperLimit(),
            dataSlice->Tag);
    }
    newDataSlice->CopyPayloadFrom(*dataSlice);
    return newDataSlice;
}

TLegacyDataSlicePtr CreateInputDataSlice(
    const TLegacyDataSlicePtr& dataSlice,
    TLegacyKey lowerKey,
    TLegacyKey upperKey)
{
    auto lowerLimit = dataSlice->LegacyLowerLimit();
    auto upperLimit = dataSlice->LegacyUpperLimit();

    if (lowerKey) {
        lowerLimit.MergeLowerKey(lowerKey);
    }

    if (upperKey) {
        upperLimit.MergeUpperKey(upperKey);
    }

    TLegacyDataSlice::TChunkSliceList chunkSlices;
    for (const auto& slice : dataSlice->ChunkSlices) {
        chunkSlices.push_back(CreateInputChunkSlice(*slice, lowerLimit.Key, upperLimit.Key));
    }

    auto newDataSlice = New<TLegacyDataSlice>(
        dataSlice->Type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit),
        dataSlice->Tag);
    newDataSlice->CopyPayloadFrom(*dataSlice);
    return newDataSlice;
}

TLegacyDataSlicePtr CreateInputDataSlice(
    const TLegacyDataSlicePtr& dataSlice,
    const TComparator& comparator,
    TKeyBound lowerKeyBound,
    TKeyBound upperKeyBound)
{
    YT_VERIFY(!dataSlice->IsLegacy);

    lowerKeyBound = comparator.StrongerKeyBound(dataSlice->LowerLimit().KeyBound, lowerKeyBound);
    upperKeyBound = comparator.StrongerKeyBound(dataSlice->UpperLimit().KeyBound, upperKeyBound);

    TLegacyDataSlice::TChunkSliceList chunkSlices;
    for (const auto& slice : dataSlice->ChunkSlices) {
        // NB: chunk slices are the part of physical data slice representation.
        // We intentionally do not intersect them with provided lower and upper bounds
        // because given comparator may be shorter than existing chunk slice key bounds.
        chunkSlices.push_back(CreateInputChunkSlice(*slice));
    }

    auto lowerLimit = dataSlice->LowerLimit();
    lowerLimit.KeyBound = lowerKeyBound;
    auto upperLimit = dataSlice->UpperLimit();
    upperLimit.KeyBound = upperKeyBound;

    auto newDataSlice = New<TLegacyDataSlice>(
        dataSlice->Type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit),
        dataSlice->Tag);
    newDataSlice->CopyPayloadFrom(*dataSlice);
    return newDataSlice;
}

void InferLimitsFromBoundaryKeys(
    const TLegacyDataSlicePtr& dataSlice,
    const TRowBufferPtr& rowBuffer,
    const TComparator& comparator)
{
    if (dataSlice->IsLegacy) {
        TLegacyKey minKey;
        TLegacyKey maxKey;
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            if (const auto& boundaryKeys = chunkSlice->GetInputChunk()->BoundaryKeys()) {
                if (!minKey || minKey > boundaryKeys->MinKey) {
                    minKey = boundaryKeys->MinKey;
                }
                if (!maxKey || maxKey < boundaryKeys->MaxKey) {
                    maxKey = boundaryKeys->MaxKey;
                }
            }
        }

        if (minKey) {
            dataSlice->LegacyLowerLimit().MergeLowerKey(rowBuffer->CaptureRow(minKey));
        }
        if (maxKey) {
            dataSlice->LegacyUpperLimit().MergeUpperKey(rowBuffer->CaptureRow(GetKeySuccessor(maxKey, rowBuffer)));
        }
    } else {
        YT_VERIFY(comparator);

        auto lowerBound = TKeyBound::MakeUniversal(/* isUpper */ false);
        auto upperBound = TKeyBound::MakeUniversal(/* isUpper */ true);
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            if (const auto& boundaryKeys = chunkSlice->GetInputChunk()->BoundaryKeys()) {
                auto chunkLowerBound = KeyBoundFromLegacyRow(boundaryKeys->MinKey, /* isUpper */ false, comparator.GetLength(), rowBuffer);
                auto chunkUpperBound = KeyBoundFromLegacyRow(GetKeySuccessor(boundaryKeys->MaxKey, rowBuffer), /* isUpper */ true, comparator.GetLength(), rowBuffer);
                comparator.ReplaceIfStrongerKeyBound(lowerBound, chunkLowerBound);
                comparator.ReplaceIfStrongerKeyBound(upperBound, chunkUpperBound);
            }
        }

        if (comparator.StrongerKeyBound(dataSlice->LowerLimit().KeyBound, lowerBound) == lowerBound) {
            lowerBound.Prefix = rowBuffer->CaptureRow(lowerBound.Prefix);
            dataSlice->LowerLimit().KeyBound = lowerBound;
        }
        if (comparator.StrongerKeyBound(dataSlice->UpperLimit().KeyBound, upperBound) == upperBound) {
            upperBound.Prefix = rowBuffer->CaptureRow(upperBound.Prefix);
            dataSlice->UpperLimit().KeyBound = upperBound;
        }
    }
}

void SetLimitsFromShortenedBoundaryKeys(
    const TLegacyDataSlicePtr& dataSlice,
    int prefixLength,
    const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(!dataSlice->IsLegacy);

    auto chunk = dataSlice->GetSingleUnversionedChunk();
    if (const auto& boundaryKeys = chunk->BoundaryKeys()) {
        dataSlice->LowerLimit().KeyBound = TKeyBound::FromRow(
            rowBuffer->CaptureRow(MakeRange(boundaryKeys->MinKey.Begin(), prefixLength)), /* isInclusive */ true, /* isUpper */ false);
        dataSlice->UpperLimit().KeyBound = TKeyBound::FromRow(
            rowBuffer->CaptureRow(MakeRange(boundaryKeys->MaxKey.Begin(), prefixLength)), /* isInclusive */ true, /* isUpper */ true);
    }
}

std::optional<TChunkId> IsUnavailable(
    const TLegacyDataSlicePtr& dataSlice,
    EChunkAvailabilityPolicy policy)
{
    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        if (IsUnavailable(chunkSlice->GetInputChunk(), policy)) {
            return chunkSlice->GetInputChunk()->GetChunkId();
        }
    }
    return std::nullopt;
}

bool CompareChunkSlicesByLowerLimit(const TInputChunkSlicePtr& slice1, const TInputChunkSlicePtr& slice2)
{
    const auto& limit1 = slice1->LegacyLowerLimit();
    const auto& limit2 = slice2->LegacyLowerLimit();
    i64 diff;

    diff = slice1->GetInputChunk()->GetRangeIndex() - slice2->GetInputChunk()->GetRangeIndex();
    if (diff != 0) {
        return diff < 0;
    }

    diff = (limit1.RowIndex.value_or(0) + slice1->GetInputChunk()->GetTableRowIndex()) -
           (limit2.RowIndex.value_or(0) + slice2->GetInputChunk()->GetTableRowIndex());
    if (diff != 0) {
        return diff < 0;
    }

    diff = CompareRows(limit1.Key, limit2.Key);
    return diff < 0;
}

i64 GetCumulativeRowCount(const std::vector<TLegacyDataSlicePtr>& dataSlices)
{
    i64 result = 0;
    for (const auto& dataSlice : dataSlices) {
        result += dataSlice->GetRowCount();
    }
    return result;
}

i64 GetCumulativeDataWeight(const std::vector<TLegacyDataSlicePtr>& dataSlices)
{
    i64 result = 0;
    for (const auto& dataSlice : dataSlices) {
        result += dataSlice->GetDataWeight();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TLegacyDataSlicePtr> CombineVersionedChunkSlices(const std::vector<TInputChunkSlicePtr>& chunkSlices, const TComparator& comparator)
{
    for (const auto& chunkSlice : chunkSlices) {
        YT_VERIFY(!chunkSlice->IsLegacy);
    }

    std::vector<TLegacyDataSlicePtr> dataSlices;

    std::vector<std::tuple<TKeyBound, int>> boundaries;
    boundaries.reserve(chunkSlices.size() * 2);
    for (int index = 0; index < std::ssize(chunkSlices); ++index) {
        if (!comparator.IsRangeEmpty(chunkSlices[index]->LowerLimit().KeyBound, chunkSlices[index]->UpperLimit().KeyBound)) {
            boundaries.emplace_back(chunkSlices[index]->LowerLimit().KeyBound, index);
            boundaries.emplace_back(chunkSlices[index]->UpperLimit().KeyBound, index);
        }
    }
    std::sort(boundaries.begin(), boundaries.end(), [&] (const auto& lhs, const auto& rhs) {
        const auto& [lhsBound, lhsIndex] = lhs;
        const auto& [rhsBound, rhsIndex] = rhs;
        auto result = comparator.CompareKeyBounds(lhsBound, rhsBound, /* lowerVsUpper */ 0);
        if (result != 0) {
            return result < 0;
        }
        return lhsIndex < rhsIndex;
    });
    THashSet<int> currentChunks;

    int index = 0;
    while (index < std::ssize(boundaries)) {
        const auto& boundary = boundaries[index];
        auto currentKeyBound = std::get<0>(boundary);
        auto currentKeyBoundToLower = currentKeyBound.LowerCounterpart();

        while (index < std::ssize(boundaries)) {
            const auto& boundary = boundaries[index];
            auto keyBound = std::get<0>(boundary);
            int chunkIndex = std::get<1>(boundary);
            bool isUpper = keyBound.IsUpper;

            if (comparator.CompareKeyBounds(keyBound, currentKeyBound, /* lowerVsUpper */ 0) != 0) {
                break;
            }

            if (isUpper) {
                YT_VERIFY(currentChunks.erase(chunkIndex) == 1);
            } else {
                currentChunks.insert(chunkIndex);
            }
            ++index;
        }

        if (!currentChunks.empty()) {
            std::vector<TInputChunkSlicePtr> chunks;
            for (int chunkIndex : currentChunks) {
                chunks.push_back(chunkSlices[chunkIndex]);
            }

            auto upper = index == std::ssize(boundaries) ? TKeyBound::MakeUniversal(/* isUpper */ true) : std::get<0>(boundaries[index]);
            upper = upper.UpperCounterpart();

            auto slice = CreateInputDataSlice(
                EDataSourceType::VersionedTable,
                std::move(chunks),
                comparator,
                currentKeyBoundToLower,
                upper);
            YT_VERIFY(!slice->IsLegacy);

            dataSlices.push_back(std::move(slice));
        }
    }

    return dataSlices;
}

////////////////////////////////////////////////////////////////////////////////

TString GetDataSliceDebugString(const TLegacyDataSlicePtr& dataSlice)
{
    return Format("{DS: %v.%v.%v, L: %v:%v, DW: %v}",
        dataSlice->GetInputStreamIndex(),
        dataSlice->Tag,
        dataSlice->GetSliceIndex(),
        dataSlice->LowerLimit(),
        dataSlice->UpperLimit(),
        dataSlice->GetDataWeight());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
