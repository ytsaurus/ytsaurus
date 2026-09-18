#include "data_slice.h"
#include "chunk_spec.h"

#include <yt/yt/ytlib/table_client/virtual_value_directory.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/serialize.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDataSlice::TDataSlice(
    EDataSourceType type,
    TChunkSliceList chunkSlices,
    TInputSliceLimit lowerLimit,
    TInputSliceLimit upperLimit,
    std::optional<i64> tag)
    : LowerLimit_(lowerLimit)
    , UpperLimit_(upperLimit)
    , ChunkSlices(std::move(chunkSlices))
    , Type(type)
    , Tag(tag)
{ }

int TDataSlice::GetChunkCount() const
{
    return ChunkSlices.size();
}

i64 TDataSlice::GetDataWeight() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetDataWeight();
    }
    return result;
}

i64 TDataSlice::GetRowCount() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetRowCount();
    }
    return result;
}

i64 TDataSlice::GetValueCount() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetValueCount();
    }
    return result;
}

i64 TDataSlice::GetCompressedDataSize() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetCompressedDataSize();
    }
    return result;
}

i64 TDataSlice::GetUncompressedDataSize() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetUncompressedDataSize();
    }
    return result;
}

i64 TDataSlice::GetMaxBlockSize() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result = std::max(result, chunkSlice->GetMaxBlockSize());
    }
    return result;
}

void TDataSlice::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, LowerLimit_);
    PHOENIX_REGISTER_FIELD(2, UpperLimit_);
    PHOENIX_REGISTER_FIELD(3, ChunkSlices);
    PHOENIX_REGISTER_FIELD(4, Type);
    PHOENIX_REGISTER_FIELD(5, Tag);
    PHOENIX_REGISTER_FIELD(6, InputStreamIndex_);
    PHOENIX_REGISTER_FIELD(7, VirtualRowIndex);
    PHOENIX_REGISTER_FIELD(8, ReadRangeIndex);
    PHOENIX_REGISTER_FIELD(9, IsTeleportable);
}

int TDataSlice::GetTableIndex() const
{
    YT_VERIFY(ChunkSlices.size() > 0);
    return ChunkSlices[0]->GetInputChunk()->GetTableIndex();
}

int TDataSlice::GetRangeIndex() const
{
    YT_VERIFY(ChunkSlices.size() > 0);
    return ChunkSlices[0]->GetInputChunk()->GetRangeIndex();
}

TInputChunkPtr TDataSlice::GetSingleUnversionedChunk() const
{
    return GetSingleUnversionedChunkSlice()->GetInputChunk();
}

TInputChunkSlicePtr TDataSlice::GetSingleUnversionedChunkSlice() const
{
    YT_VERIFY(IsTrivial());

    return ChunkSlices[0];
}

bool TDataSlice::IsTrivial() const
{
    return Type == EDataSourceType::UnversionedTable && ChunkSlices.size() == 1;
}

bool TDataSlice::HasLimits() const
{
    return !LowerLimit_.IsTrivial() || !UpperLimit_.IsTrivial();
}

std::pair<TDataSlicePtr, TDataSlicePtr> TDataSlice::SplitByRowIndex(i64 rowIndex) const
{
    YT_VERIFY(IsTrivial());
    auto slices = ChunkSlices[0]->SplitByRowIndex(rowIndex);

    auto first = CreateUnversionedInputDataSlice(slices.first);
    auto second = CreateUnversionedInputDataSlice(slices.second);

    // CreateUnversionedInputDataSlice infers key bounds both from chunk slice key bounds and
    // from data slice key bounds making resulting parts key bounds wider than our own key bounds.
    // Preserve the logical data-slice bounds while splitting its physical chunk slice.
    first->LowerLimit().KeyBound = LowerLimit().KeyBound;
    first->UpperLimit().KeyBound = UpperLimit().KeyBound;
    second->LowerLimit().KeyBound = LowerLimit().KeyBound;
    second->UpperLimit().KeyBound = UpperLimit().KeyBound;

    first->CopyPayloadFrom(*this);
    second->CopyPayloadFrom(*this);

    return {std::move(first), std::move(second)};
}

void TDataSlice::CopyPayloadFrom(const TDataSlice& dataSlice)
{
    InputStreamIndex_ = dataSlice.InputStreamIndex_;
    Tag = dataSlice.Tag;
    VirtualRowIndex = dataSlice.VirtualRowIndex;
    ReadRangeIndex = dataSlice.ReadRangeIndex;
}

int TDataSlice::GetSliceIndex() const
{
    return Type == EDataSourceType::UnversionedTable
        ? ChunkSlices[0]->GetSliceIndex()
        : 0;
}

int TDataSlice::GetInputStreamIndex() const
{
    YT_VERIFY(InputStreamIndex_);
    return *InputStreamIndex_;
}

void TDataSlice::SetInputStreamIndex(int inputStreamIndex)
{
    InputStreamIndex_ = inputStreamIndex;
}

PHOENIX_DEFINE_TYPE(TDataSlice);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDataSlicePtr& dataSlice, IYsonConsumer* consumer)
{
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

void FormatValue(TStringBuilderBase* builder, const TDataSlicePtr& dataSlice, TStringBuf /*spec*/)
{
    Format(
        builder,
        "Type: %v, LowerLimit: %v, UpperLimit: %v, ChunkSlices: %v",
        dataSlice->Type,
        dataSlice->LowerLimit(),
        dataSlice->UpperLimit(),
        dataSlice->ChunkSlices);
}

////////////////////////////////////////////////////////////////////////////////

TDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkSlicePtr chunkSlice)
{
    return New<TDataSlice>(
        EDataSourceType::UnversionedTable,
        TDataSlice::TChunkSliceList{chunkSlice},
        chunkSlice->LowerLimit(),
        chunkSlice->UpperLimit());
}

TDataSlicePtr CreateVersionedInputDataSlice(const std::vector<TInputChunkSlicePtr>& inputChunkSlices)
{
    YT_VERIFY(!inputChunkSlices.empty());
    TDataSlice::TChunkSliceList chunkSlices;
    std::optional<int> tableIndex;
    TInputSliceLimit lowerLimit;
    TInputSliceLimit upperLimit(/*isUpper*/ true);
    for (const auto& inputChunkSlice : inputChunkSlices) {
        if (!tableIndex) {
            tableIndex = inputChunkSlice->GetInputChunk()->GetTableIndex();
            lowerLimit.KeyBound = inputChunkSlice->LowerLimit().KeyBound;
            upperLimit.KeyBound = inputChunkSlice->UpperLimit().KeyBound;
        } else {
            YT_VERIFY(*tableIndex == inputChunkSlice->GetInputChunk()->GetTableIndex());
            YT_VERIFY(lowerLimit.KeyBound == inputChunkSlice->LowerLimit().KeyBound);
            YT_VERIFY(upperLimit.KeyBound == inputChunkSlice->UpperLimit().KeyBound);
        }
        chunkSlices.push_back(inputChunkSlice);
    }
    return New<TDataSlice>(
        EDataSourceType::VersionedTable,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TDataSlicePtr CreateInputDataSlice(
    NChunkClient::EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    const TComparator& comparator,
    TKeyBound lowerBound,
    TKeyBound upperBound)
{
    TDataSlice::TChunkSliceList chunkSlices;
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

    return New<TDataSlice>(
        type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TDataSlicePtr CreateInputDataSlice(const TDataSlicePtr& dataSlice)
{
    TDataSlice::TChunkSliceList chunkSlices;
    for (const auto& slice : dataSlice->ChunkSlices) {
        chunkSlices.push_back(CreateInputChunkSlice(*slice));
    }

    auto newDataSlice = New<TDataSlice>(
        dataSlice->Type,
        std::move(chunkSlices),
        dataSlice->LowerLimit(),
        dataSlice->UpperLimit(),
        dataSlice->Tag);
    newDataSlice->CopyPayloadFrom(*dataSlice);
    return newDataSlice;
}

TDataSlicePtr CreateInputDataSlice(
    const TDataSlicePtr& dataSlice,
    const TComparator& comparator,
    TKeyBound lowerKeyBound,
    TKeyBound upperKeyBound)
{
    lowerKeyBound = comparator.StrongerKeyBound(dataSlice->LowerLimit().KeyBound, lowerKeyBound);
    upperKeyBound = comparator.StrongerKeyBound(dataSlice->UpperLimit().KeyBound, upperKeyBound);

    TDataSlice::TChunkSliceList chunkSlices;
    for (const auto& slice : dataSlice->ChunkSlices) {
        // NB: Chunk slices are the part of physical data slice representation.
        // We intentionally do not intersect them with provided lower and upper bounds
        // because given comparator may be shorter than existing chunk slice key bounds.
        chunkSlices.push_back(CreateInputChunkSlice(*slice));
    }

    auto lowerLimit = dataSlice->LowerLimit();
    lowerLimit.KeyBound = lowerKeyBound;
    auto upperLimit = dataSlice->UpperLimit();
    upperLimit.KeyBound = upperKeyBound;

    auto newDataSlice = New<TDataSlice>(
        dataSlice->Type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit),
        dataSlice->Tag);
    newDataSlice->CopyPayloadFrom(*dataSlice);
    return newDataSlice;
}

void InferLimitsFromBoundaryKeys(
    const TDataSlicePtr& dataSlice,
    const TRowBufferPtr& rowBuffer,
    const TComparator& comparator)
{
    YT_VERIFY(comparator);

    auto lowerBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
    auto upperBound = TKeyBound::MakeUniversal(/*isUpper*/ true);
    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        if (const auto& boundaryKeys = chunkSlice->GetInputChunk()->BoundaryKeys()) {
            if (boundaryKeys->MinKey) {
                auto chunkLowerBound = KeyBoundFromLegacyRow(boundaryKeys->MinKey, /*isUpper*/ false, comparator.GetLength(), rowBuffer);
                comparator.ReplaceIfStrongerKeyBound(lowerBound, chunkLowerBound);
            }
            if (boundaryKeys->MaxKey) {
                auto chunkUpperBound = KeyBoundFromLegacyRow(GetKeySuccessor(boundaryKeys->MaxKey, rowBuffer), /*isUpper*/ true, comparator.GetLength(), rowBuffer);
                comparator.ReplaceIfStrongerKeyBound(upperBound, chunkUpperBound);
            }
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

void SetLimitsFromShortenedBoundaryKeys(
    const TDataSlicePtr& dataSlice,
    int prefixLength,
    const TRowBufferPtr& rowBuffer)
{
    auto chunk = dataSlice->GetSingleUnversionedChunk();
    if (const auto& boundaryKeys = chunk->BoundaryKeys()) {
        dataSlice->LowerLimit().KeyBound = TKeyBound::FromRow(
            rowBuffer->CaptureRow(TRange(boundaryKeys->MinKey.Begin(), prefixLength)), /*isInclusive*/ true, /*isUpper*/ false);
        dataSlice->UpperLimit().KeyBound = TKeyBound::FromRow(
            rowBuffer->CaptureRow(TRange(boundaryKeys->MaxKey.Begin(), prefixLength)), /*isInclusive*/ true, /*isUpper*/ true);
    }
}

std::optional<TChunkId> IsUnavailable(
    const TDataSlicePtr& dataSlice,
    EChunkAvailabilityPolicy policy)
{
    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        if (chunkSlice->GetInputChunk()->IsUnavailable(policy)) {
            return chunkSlice->GetInputChunk()->GetChunkId();
        }
    }
    return std::nullopt;
}

bool CompareChunkSlicesByLowerLimit(const TInputChunkSlicePtr& slice1, const TInputChunkSlicePtr& slice2)
{
    const auto& limit1 = slice1->LowerLimit();
    const auto& limit2 = slice2->LowerLimit();
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

    diff = CompareRows(limit1.KeyBound.Prefix, limit2.KeyBound.Prefix);
    return diff < 0;
}

i64 GetCumulativeRowCount(const std::vector<TDataSlicePtr>& dataSlices)
{
    i64 result = 0;
    for (const auto& dataSlice : dataSlices) {
        result += dataSlice->GetRowCount();
    }
    return result;
}

i64 GetCumulativeDataWeight(const std::vector<TDataSlicePtr>& dataSlices)
{
    i64 result = 0;
    for (const auto& dataSlice : dataSlices) {
        result += dataSlice->GetDataWeight();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSlicePtr> CombineVersionedChunkSlices(const std::vector<TInputChunkSlicePtr>& chunkSlices, const TComparator& comparator)
{
    std::vector<TDataSlicePtr> dataSlices;

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
        auto result = comparator.CompareKeyBounds(lhsBound, rhsBound, /*lowerVsUpper*/ 0);
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

            if (comparator.CompareKeyBounds(keyBound, currentKeyBound, /*lowerVsUpper*/ 0) != 0) {
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

            auto upper = index == std::ssize(boundaries) ? TKeyBound::MakeUniversal(/*isUpper*/ true) : std::get<0>(boundaries[index]);
            upper = upper.UpperCounterpart();

            auto slice = CreateInputDataSlice(
                EDataSourceType::VersionedTable,
                std::move(chunks),
                comparator,
                currentKeyBoundToLower,
                upper);
            dataSlices.push_back(std::move(slice));
        }
    }

    return dataSlices;
}

////////////////////////////////////////////////////////////////////////////////

std::string GetDataSliceDebugString(const TDataSlicePtr& dataSlice)
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
