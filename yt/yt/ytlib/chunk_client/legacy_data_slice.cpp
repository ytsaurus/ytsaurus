#include "legacy_data_slice.h"
#include "chunk_spec.h"

#include <yt/ytlib/table_client/virtual_value_directory.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/serialize.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/logging/log.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

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
    InputStreamIndex = GetTableIndex();

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
    InputStreamIndex = GetTableIndex();

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

i64 TLegacyDataSlice::GetMaxBlockSize() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result = std::max(result, chunkSlice->GetMaxBlockSize());
    }
    return result;
}

void TLegacyDataSlice::Persist(NTableClient::TPersistenceContext& context)
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
    Persist(context, InputStreamIndex);
    Persist(context, VirtualRowIndex);
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

TInputChunkPtr TLegacyDataSlice::GetSingleUnversionedChunkOrThrow() const
{
    if (!IsTrivial()) {
        THROW_ERROR_EXCEPTION("Dynamic table cannot be used in this context");
    }
    return ChunkSlices[0]->GetInputChunk();
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
    return LegacyLowerLimit_.Key || LegacyLowerLimit_.RowIndex || LegacyUpperLimit_.Key || LegacyUpperLimit_.RowIndex;
}

std::pair<TLegacyDataSlicePtr, TLegacyDataSlicePtr> TLegacyDataSlice::SplitByRowIndex(i64 rowIndex) const
{
    YT_VERIFY(IsTrivial());
    auto slices = ChunkSlices[0]->SplitByRowIndex(rowIndex);

    auto first = CreateUnversionedInputDataSlice(slices.first);
    auto second = CreateUnversionedInputDataSlice(slices.second);

    first->CopyPayloadFrom(*this);
    second->CopyPayloadFrom(*this);

    return {std::move(first), std::move(second)};
}

void TLegacyDataSlice::CopyPayloadFrom(const TLegacyDataSlice& dataSlice)
{
    InputStreamIndex = dataSlice.InputStreamIndex;
    Tag = dataSlice.Tag;
    VirtualRowIndex = dataSlice.VirtualRowIndex;
}

void TLegacyDataSlice::TransformToLegacy(const TRowBufferPtr& rowBuffer)
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

    for (auto& chunkSlice : ChunkSlices) {
        chunkSlice->TransformToLegacy(rowBuffer);
    }

    IsLegacy = true;
}

void TLegacyDataSlice::TransformToNew(const TRowBufferPtr& rowBuffer, int keyLength)
{
    YT_VERIFY(IsLegacy);

    LowerLimit_.RowIndex = LegacyLowerLimit_.RowIndex;
    LowerLimit_.KeyBound = KeyBoundFromLegacyRow(LegacyLowerLimit_.Key, /* isUpper */ false, keyLength, rowBuffer);
    UpperLimit_.RowIndex = LegacyUpperLimit_.RowIndex;
    UpperLimit_.KeyBound = KeyBoundFromLegacyRow(LegacyUpperLimit_.Key, /* isUpper */ true, keyLength, rowBuffer);
    LegacyLowerLimit_ = TLegacyInputSliceLimit();
    LegacyUpperLimit_ = TLegacyInputSliceLimit();

    for (auto& chunkSlice : ChunkSlices) {
        chunkSlice->TransformToNew(rowBuffer, keyLength);
    }

    IsLegacy = false;
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
    newDataSlice->InputStreamIndex = dataSlice->InputStreamIndex;
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
    newDataSlice->InputStreamIndex = dataSlice->InputStreamIndex;
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
        chunkSlices.push_back(CreateInputChunkSlice(*slice, comparator, lowerKeyBound, upperKeyBound));
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
    newDataSlice->InputStreamIndex = dataSlice->InputStreamIndex;
    return newDataSlice;
}

void InferLimitsFromBoundaryKeys(
    const TLegacyDataSlicePtr& dataSlice,
    const TRowBufferPtr& rowBuffer,
    const TVirtualValueDirectoryPtr& virtualValueDirectory)
{
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
    auto captureMaybeWithVirtualPrefix = [&] (TUnversionedRow row) {
        if (virtualValueDirectory && dataSlice->VirtualRowIndex) {
            auto virtualPrefix = virtualValueDirectory->Rows[*dataSlice->VirtualRowIndex];
            auto capturedRow = rowBuffer->AllocateUnversioned(row.GetCount() + virtualPrefix.GetCount());
            memcpy(capturedRow.begin(), virtualPrefix.begin(), sizeof(TUnversionedValue) * virtualPrefix.GetCount());
            memcpy(capturedRow.begin() + virtualPrefix.GetCount(), row.begin(), sizeof(TUnversionedValue) * row.GetCount());
            for (auto& value : capturedRow) {
                rowBuffer->Capture(&value);
            }
            return capturedRow;
        } else {
            return rowBuffer->Capture(row);
        }
    };

    if (minKey) {
        dataSlice->LegacyLowerLimit().MergeLowerKey(captureMaybeWithVirtualPrefix(minKey));
    }
    if (maxKey) {
        dataSlice->LegacyUpperLimit().MergeUpperKey(captureMaybeWithVirtualPrefix(GetKeySuccessor(maxKey, rowBuffer)));
    }
}

std::optional<TChunkId> IsUnavailable(const TLegacyDataSlicePtr& dataSlice, bool checkParityParts)
{
    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        if (IsUnavailable(chunkSlice->GetInputChunk(), checkParityParts)) {
            return chunkSlice->GetInputChunk()->ChunkId();
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

std::vector<TLegacyDataSlicePtr> CombineVersionedChunkSlices(const std::vector<TInputChunkSlicePtr>& chunkSlices)
{
    std::vector<TLegacyDataSlicePtr> dataSlices;

    std::vector<std::tuple<TLegacyKey, bool, int>> boundaries;
    boundaries.reserve(chunkSlices.size() * 2);
    for (int index = 0; index < chunkSlices.size(); ++index) {
        if (chunkSlices[index]->LegacyLowerLimit().Key < chunkSlices[index]->LegacyUpperLimit().Key) {
            boundaries.emplace_back(chunkSlices[index]->LegacyLowerLimit().Key, false, index);
            boundaries.emplace_back(chunkSlices[index]->LegacyUpperLimit().Key, true, index);
        }
    }
    std::sort(boundaries.begin(), boundaries.end());
    THashSet<int> currentChunks;

    int index = 0;
    while (index < boundaries.size()) {
        const auto& boundary = boundaries[index];
        auto currentKey = std::get<0>(boundary);

        while (index < boundaries.size()) {
            const auto& boundary = boundaries[index];
            auto key = std::get<0>(boundary);
            int chunkIndex = std::get<2>(boundary);
            bool isUpper = std::get<1>(boundary);

            if (key != currentKey) {
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

            auto upper = index == boundaries.size() ? MaxKey().Get() : std::get<0>(boundaries[index]);

            auto slice = CreateInputDataSlice(
                EDataSourceType::VersionedTable,
                std::move(chunks),
                currentKey,
                upper);

            dataSlices.push_back(std::move(slice));
        }
    }

    return dataSlices;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

