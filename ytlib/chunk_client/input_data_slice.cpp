#include "input_data_slice.h"
#include "chunk_spec.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/serialize.h>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TInputDataSlice::TInputDataSlice(
    EDataSourceType type,
    TChunkSliceList chunkSlices,
    TInputSliceLimit lowerLimit,
    TInputSliceLimit upperLimit,
    TNullable<i64> tag)
    : LowerLimit_(std::move(lowerLimit))
    , UpperLimit_(std::move(upperLimit))
    , ChunkSlices(std::move(chunkSlices))
    , Type(type)
    , Tag(tag)
{
    InputStreamIndex = GetTableIndex();
}

int TInputDataSlice::GetChunkCount() const
{
    return ChunkSlices.size();
}

i64 TInputDataSlice::GetDataWeight() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetDataWeight();
    }
    return result;
}

i64 TInputDataSlice::GetRowCount() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result += chunkSlice->GetRowCount();
    }
    return result;
}

i64 TInputDataSlice::GetMaxBlockSize() const
{
    i64 result = 0;
    for (const auto& chunkSlice : ChunkSlices) {
        result = std::max(result, chunkSlice->GetMaxBlockSize());
    }
    return result;
}

void TInputDataSlice::Persist(NTableClient::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, ChunkSlices);
    Persist(context, Type);
    Persist(context, Tag);
    Persist(context, InputStreamIndex);
}

int TInputDataSlice::GetTableIndex() const
{
    YCHECK(ChunkSlices.size() > 0);
    return ChunkSlices[0]->GetInputChunk()->GetTableIndex();
}

TInputChunkPtr TInputDataSlice::GetSingleUnversionedChunkOrThrow() const
{
    if (!IsTrivial()) {
        THROW_ERROR_EXCEPTION("Dynamic table cannot be used in this context");
    }
    return ChunkSlices[0]->GetInputChunk();
}

bool TInputDataSlice::IsTrivial() const
{
    return Type == EDataSourceType::UnversionedTable && ChunkSlices.size() == 1;
}

bool TInputDataSlice::IsEmpty() const
{
    return LowerLimit_.Key && UpperLimit_.Key && LowerLimit_.Key >= UpperLimit_.Key;
}

bool TInputDataSlice::HasLimits() const
{
    return LowerLimit_.Key || LowerLimit_.RowIndex || UpperLimit_.Key || UpperLimit_.RowIndex;
}

std::pair<TInputDataSlicePtr, TInputDataSlicePtr> TInputDataSlice::SplitByRowIndex(i64 rowIndex) const
{
    YCHECK(IsTrivial());
    auto slices = ChunkSlices[0]->SplitByRowIndex(rowIndex);

    return std::make_pair(CreateUnversionedInputDataSlice(slices.first),
        CreateUnversionedInputDataSlice(slices.second));
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TInputDataSlicePtr& dataSlice)
{
    return Format("Type: %v, LowerLimit: %v, UpperLimit: %v, ChunkSlices: %v",
        dataSlice->Type,
        dataSlice->LowerLimit(),
        dataSlice->UpperLimit(),
        dataSlice->ChunkSlices);
}

void ToProto(
    NProto::TDataSliceDescriptor* dataSliceDescriptor,
    TInputDataSlicePtr inputDataSlice)
{
    for (const auto& slice : inputDataSlice->ChunkSlices) {
        auto* chunk = dataSliceDescriptor->add_chunks();
        ToProto(chunk, slice, inputDataSlice->Type);
        if (inputDataSlice->Tag) {
            chunk->set_data_slice_tag(*inputDataSlice->Tag);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TInputDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkSlicePtr chunkSlice)
{
    return New<TInputDataSlice>(
        EDataSourceType::UnversionedTable,
        TInputDataSlice::TChunkSliceList{chunkSlice},
        chunkSlice->LowerLimit(),
        chunkSlice->UpperLimit());
}

TInputDataSlicePtr CreateVersionedInputDataSlice(const std::vector<TInputChunkSlicePtr>& inputChunks)
{
    std::vector<TInputDataSlicePtr> dataSlices;

    YCHECK(!inputChunks.empty());
    TInputDataSlice::TChunkSliceList chunkSlices;
    TNullable<int> tableIndex;
    TInputSliceLimit lowerLimit;
    TInputSliceLimit upperLimit;
    for (const auto& inputChunk : inputChunks) {
        if (!tableIndex) {
            tableIndex = inputChunk->GetInputChunk()->GetTableIndex();
            lowerLimit.Key = inputChunk->LowerLimit().Key;
            upperLimit.Key = inputChunk->UpperLimit().Key;
        } else {
            YCHECK(*tableIndex == inputChunk->GetInputChunk()->GetTableIndex());
            YCHECK(lowerLimit.Key == inputChunk->LowerLimit().Key);
            YCHECK(upperLimit.Key == inputChunk->UpperLimit().Key);
        }
        chunkSlices.push_back(inputChunk);
    }
    return New<TInputDataSlice>(
        EDataSourceType::VersionedTable,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TInputDataSlicePtr CreateInputDataSlice(
    EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    TKey lowerKey,
    TKey upperKey)
{
    TInputDataSlice::TChunkSliceList chunkSlices;
    TNullable<int> tableIndex;
    for (const auto& inputChunk : inputChunks) {
        if (!tableIndex) {
            tableIndex = inputChunk->GetInputChunk()->GetTableIndex();
        } else {
            YCHECK(*tableIndex == inputChunk->GetInputChunk()->GetTableIndex());
        }
        chunkSlices.push_back(CreateInputChunkSlice(*inputChunk, lowerKey, upperKey));
    }

    TInputSliceLimit lowerLimit;
    lowerLimit.Key = lowerKey;

    TInputSliceLimit upperLimit;
    upperLimit.Key = upperKey;

    return New<TInputDataSlice>(
        type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit));
}

TInputDataSlicePtr CreateInputDataSlice(
    const TInputDataSlicePtr& dataSlice,
    TKey lowerKey,
    TKey upperKey)
{
    auto lowerLimit = dataSlice->LowerLimit();
    auto upperLimit = dataSlice->UpperLimit();

    if (lowerKey) {
        lowerLimit.MergeLowerKey(lowerKey);
    }

    if (upperKey) {
        upperLimit.MergeUpperKey(upperKey);
    }

    //FIXME(savrus) delay chunkSpec limits until ToProto
    TInputDataSlice::TChunkSliceList chunkSlices;
    for (const auto& slice : dataSlice->ChunkSlices) {
        chunkSlices.push_back(CreateInputChunkSlice(*slice, lowerLimit.Key, upperLimit.Key));
    }

    auto newDataSlice = New<TInputDataSlice>(
        dataSlice->Type,
        std::move(chunkSlices),
        std::move(lowerLimit),
        std::move(upperLimit),
        dataSlice->Tag);
    newDataSlice->InputStreamIndex = dataSlice->InputStreamIndex;
    return newDataSlice;
}

void InferLimitsFromBoundaryKeys(const TInputDataSlicePtr& dataSlice, const TRowBufferPtr& rowBuffer)
{
    TKey minKey;
    TKey maxKey;
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
        dataSlice->LowerLimit().MergeLowerKey(rowBuffer->Capture(minKey));
    }
    if (maxKey) {
        dataSlice->UpperLimit().MergeUpperKey(GetKeySuccessor(maxKey, rowBuffer));
    }
}

TNullable<TChunkId> IsUnavailable(const TInputDataSlicePtr& dataSlice, bool checkParityParts)
{
    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        if (IsUnavailable(chunkSlice->GetInputChunk(), checkParityParts)) {
            return chunkSlice->GetInputChunk()->ChunkId();
        }
    }
    return Null;
}

bool CompareDataSlicesByLowerLimit(const TInputDataSlicePtr& slice1, const TInputDataSlicePtr& slice2)
{
    const auto& limit1 = slice1->LowerLimit();
    const auto& limit2 = slice2->LowerLimit();
    i64 diff;

    if (slice1->IsTrivial() && slice2->IsTrivial()) {
        diff = slice1->ChunkSlices[0]->GetInputChunk()->GetRangeIndex() - slice2->ChunkSlices[0]->GetInputChunk()->GetRangeIndex();
        if (diff != 0) {
            return diff < 0;
        }

        diff = (limit1.RowIndex.Get(0) + slice1->ChunkSlices[0]->GetInputChunk()->GetTableRowIndex()) -
            (limit2.RowIndex.Get(0) + slice2->ChunkSlices[0]->GetInputChunk()->GetTableRowIndex());
        if (diff != 0) {
            return diff < 0;
        }
    }

    diff = CompareRows(limit1.Key, limit2.Key);
    return diff < 0;
}

bool CanMergeSlices(const TInputDataSlicePtr& slice1, const TInputDataSlicePtr& slice2)
{
    //FIXME(savrus) really&
    if (!slice1->IsTrivial() || !slice2->IsTrivial()) {
        return false;
    }

    if (slice1->ChunkSlices[0]->GetInputChunk()->GetRangeIndex() != slice2->ChunkSlices[0]->GetInputChunk()->GetRangeIndex()) {
        return false;
    }

    const auto& limit1 = slice1->UpperLimit();
    const auto& limit2 = slice2->LowerLimit();

    if ((limit1.RowIndex || limit1.Key) &&
        limit1.RowIndex.operator bool() == limit2.RowIndex.operator bool() &&
        limit1.Key.operator bool() == limit2.Key.operator bool())
    {
        if (limit1.RowIndex &&
            *limit1.RowIndex + slice1->ChunkSlices[0]->GetInputChunk()->GetTableRowIndex() !=
            *limit2.RowIndex + slice2->ChunkSlices[0]->GetInputChunk()->GetTableRowIndex())
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

i64 GetCumulativeRowCount(const std::vector<TInputDataSlicePtr>& dataSlices)
{
    i64 result = 0;
    for (const auto& dataSlice : dataSlices) {
        result += dataSlice->GetRowCount();
    }
    return result;
}

i64 GetCumulativeDataWeight(const std::vector<TInputDataSlicePtr>& dataSlices)
{
    i64 result = 0;
for (const auto& dataSlice : dataSlices) {
        result += dataSlice->GetDataWeight();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TInputDataSlicePtr> CombineVersionedChunkSlices(const std::vector<TInputChunkSlicePtr>& chunkSlices)
{
    std::vector<TInputDataSlicePtr> dataSlices;

    std::vector<std::tuple<TKey, bool, int>> boundaries;
    boundaries.reserve(chunkSlices.size() * 2);
    for (int index = 0; index < chunkSlices.size(); ++index) {
        boundaries.emplace_back(chunkSlices[index]->LowerLimit().Key, false, index);
        boundaries.emplace_back(chunkSlices[index]->UpperLimit().Key, true, index);
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
                currentChunks.erase(chunkIndex);
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

} // namespace NChunkClient
} // namespace NYT

