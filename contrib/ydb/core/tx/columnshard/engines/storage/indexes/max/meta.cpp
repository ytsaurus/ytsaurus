#include "meta.h"

#include <contrib/ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <contrib/ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <contrib/ydb/core/tx/program/program.h>

#include <contrib/ydb/library/formats/arrow/scalar/serialization.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NMax {

std::vector<std::shared_ptr<IPortionDataChunk>> TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const {
    std::shared_ptr<arrow::Scalar> result;
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    {
        TChunkedColumnReader cReader = *reader.begin();
        for (reader.Start(); cReader.IsCorrect(); cReader.ReadNextChunk()) {
            auto currentScalar = cReader.GetCurrentChunk()->GetMaxScalar();
            AFL_VERIFY(currentScalar);
            if (!result || NArrow::ScalarCompare(*result, *currentScalar) == -1) {
                result = currentScalar;
            }
        }
    }
    const TString indexData = NArrow::NScalar::TSerializer::SerializePayloadToString(result).DetachResult();
    return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
}

std::shared_ptr<arrow::Scalar> TIndexMeta::GetMaxScalarVerified(
    const std::vector<TString>& data, const std::shared_ptr<arrow::DataType>& dataType) const {
    AFL_VERIFY(data.size());
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& d : data) {
        std::shared_ptr<arrow::Scalar> current = NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(d, dataType).DetachResult();
        if (!result || NArrow::ScalarCompare(*result, *current) == -1) {
            result = current;
        }
    }
    return result;
}

NJson::TJsonValue TIndexMeta::DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const {
    auto scalar = GetMaxScalarVerified({ data }, indexInfo.GetColumnFeaturesVerified(GetColumnId()).GetArrowField()->type());
    return scalar->ToString();
}

}   // namespace NKikimr::NOlap::NIndexes::NMax
