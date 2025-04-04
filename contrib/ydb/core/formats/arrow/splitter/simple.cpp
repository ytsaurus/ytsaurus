#include "simple.h"

#include <contrib/ydb/core/formats/arrow/size_calcer.h>

#include <contrib/ydb/library/formats/arrow/splitter/similar_packer.h>
#include <contrib/ydb/library/formats/arrow/validation/validation.h>

#include <util/string/join.h>

namespace NKikimr::NArrow::NSplitter {

std::vector<TSaverSplittedChunk> TSimpleSplitter::Split(
    const std::shared_ptr<arrow::Array>& data, const std::shared_ptr<arrow::Field>& field, const ui32 maxBlobSize) const {
    AFL_VERIFY(data);
    AFL_VERIFY(field);
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{ field });
    auto batch = arrow::RecordBatch::Make(schema, data->length(), { data });
    return Split(batch, maxBlobSize);
}

class TSplitChunk {
private:
    std::shared_ptr<arrow::RecordBatch> Data;
    YDB_READONLY_DEF(std::optional<TSaverSplittedChunk>, Result);
    ui32 SplitFactor = 0;
    ui32 Iterations = 0;
    ui32 MaxBlobSize = 8 * 1024 * 1024;
    NAccessor::TColumnSaver ColumnSaver;

public:
    TSplitChunk(const ui32 baseSplitFactor, const ui32 maxBlobSize, const std::shared_ptr<arrow::RecordBatch>& data,
        const NAccessor::TColumnSaver& columnSaver)
        : Data(data)
        , SplitFactor(baseSplitFactor)
        , MaxBlobSize(maxBlobSize)
        , ColumnSaver(columnSaver) {
        AFL_VERIFY(Data && Data->num_rows());
        AFL_VERIFY(SplitFactor);
    }

    TSplitChunk(const ui32 baseSplitFactor, const ui32 maxBlobSize, const std::shared_ptr<arrow::RecordBatch>& data, TString&& serializedData,
        const NAccessor::TColumnSaver& columnSaver)
        : Data(data)
        , Result(TSaverSplittedChunk(data, std::move(serializedData)))
        , SplitFactor(baseSplitFactor)
        , MaxBlobSize(maxBlobSize)
        , ColumnSaver(columnSaver) {
        AFL_VERIFY(Data && Data->num_rows());
        AFL_VERIFY(SplitFactor);
    }

    std::vector<TSplitChunk> Split() {
        while (true) {
            AFL_VERIFY(!Result);
            AFL_VERIFY(++Iterations < 100);
            AFL_VERIFY(SplitFactor <= Data->num_rows())("factor", SplitFactor)("records", Data->num_rows())("iteration", Iterations)(
                                        "size", NArrow::GetBatchDataSize(Data));
            bool found = false;
            std::vector<TSplitChunk> result;
            if (SplitFactor == 1) {
                TString blob = ColumnSaver.Apply(Data);
                if (blob.size() < MaxBlobSize) {
                    Result = TSaverSplittedChunk(Data, std::move(blob));
                    found = true;
                    result.emplace_back(*this);
                } else {
                    TBatchSerializationStat stats(blob.size(), Data->num_rows(), NArrow::GetBatchDataSize(Data));
                    SplitFactor = stats.PredictOptimalSplitFactor(Data->num_rows(), MaxBlobSize).value_or(1);
                    if (SplitFactor == 1) {
                        SplitFactor = 2;
                    }
                    AFL_VERIFY(Data->num_rows() > 1);
                }
            } else {
                TLinearSplitInfo linearSplitting = TSimpleSplitter::GetLinearSplittingByMax(Data->num_rows(), Data->num_rows() / SplitFactor);
                TStringBuilder sb;
                std::optional<ui32> badStartPosition;
                ui32 badBatchRecordsCount = 0;
                ui64 badBatchSerializedSize = 0;
                ui32 badBatchCount = 0;
                for (auto it = linearSplitting.StartIterator(); it.IsValid(); it.Next()) {
                    auto slice = Data->Slice(it.GetPosition(), it.GetCurrentPackSize());
                    TString blob = ColumnSaver.Apply(slice);
                    if (blob.size() >= MaxBlobSize) {
                        if (!badStartPosition) {
                            badStartPosition = it.GetPosition();
                        }
                        badBatchSerializedSize += blob.size();
                        badBatchRecordsCount += it.GetCurrentPackSize();
                        ++badBatchCount;
                        Y_ABORT_UNLESS(!linearSplitting.IsMinimalGranularity());
                    } else {
                        if (badStartPosition) {
                            AFL_VERIFY(badBatchRecordsCount && badBatchCount)("count", badBatchCount)("records", badBatchRecordsCount);
                            auto badSlice = Data->Slice(*badStartPosition, badBatchRecordsCount);
                            TBatchSerializationStat stats(badBatchSerializedSize, badBatchRecordsCount, Max<ui32>());
                            result.emplace_back(
                                std::max<ui32>(stats.PredictOptimalSplitFactor(badBatchRecordsCount, MaxBlobSize).value_or(1), badBatchCount) +
                                    1,
                                MaxBlobSize, badSlice, ColumnSaver);
                            badStartPosition = {};
                            badBatchRecordsCount = 0;
                            badBatchCount = 0;
                            badBatchSerializedSize = 0;
                        }
                        found = true;
                        result.emplace_back(1, MaxBlobSize, slice, std::move(blob), ColumnSaver);
                    }
                }
                if (badStartPosition) {
                    auto badSlice = Data->Slice(*badStartPosition, badBatchRecordsCount);
                    TBatchSerializationStat stats(badBatchSerializedSize, badBatchRecordsCount, Max<ui32>());
                    result.emplace_back(
                        std::max<ui32>(stats.PredictOptimalSplitFactor(badBatchRecordsCount, MaxBlobSize).value_or(1), badBatchCount) + 1,
                        MaxBlobSize, badSlice, ColumnSaver);
                }
                ++SplitFactor;
            }
            if (found) {
                return result;
            }
        }
        AFL_VERIFY(false);
        return {};
    }
};

std::vector<TSaverSplittedChunk> TSimpleSplitter::Split(const std::shared_ptr<arrow::RecordBatch>& data, const ui32 maxBlobSize) const {
    AFL_VERIFY(data->num_rows());
    TSplitChunk baseChunk(
        Stats ? Stats->PredictOptimalSplitFactor(data->num_rows(), maxBlobSize).value_or(1) : 1, maxBlobSize, data, ColumnSaver);
    std::vector<TSplitChunk> chunks = { baseChunk };
    for (auto it = chunks.begin(); it != chunks.end();) {
        AFL_VERIFY(chunks.size() < 100);
        if (!!it->GetResult()) {
            ++it;
            continue;
        }
        std::vector<TSplitChunk> splitted = it->Split();
        if (splitted.size() == 1) {
            *it = splitted.front();
        } else {
            it = chunks.insert(it, splitted.begin(), splitted.end());
            chunks.erase(it + splitted.size());
        }
    }
    std::vector<TSaverSplittedChunk> result;
    for (auto&& i : chunks) {
        AFL_VERIFY(i.GetResult());
        result.emplace_back(*i.GetResult());
    }
    return result;
}

std::vector<TSaverSplittedChunk> TSimpleSplitter::SplitByRecordsCount(
    const std::shared_ptr<arrow::RecordBatch>& data, const std::vector<ui32>& recordsCount) const {
    std::vector<TSaverSplittedChunk> result;
    ui64 position = 0;
    for (auto&& i : recordsCount) {
        auto subData = data->Slice(position, i);
        result.emplace_back(subData, ColumnSaver.Apply(subData));
        position += i;
    }
    Y_ABORT_UNLESS(position == (ui64)data->num_rows());
    return result;
}

std::vector<TSaverSplittedChunk> TSimpleSplitter::SplitBySizes(
    std::shared_ptr<arrow::RecordBatch> data, const TString& dataSerialization, const std::vector<ui64>& splitPartSizesExt) const {
    return SplitByRecordsCount(data, TSimilarPacker::SizesToRecordsCount(data->num_rows(), dataSerialization, splitPartSizesExt));
}

}   // namespace NKikimr::NArrow::NSplitter
