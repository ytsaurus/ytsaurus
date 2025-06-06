#include "meta.h"

#include <contrib/ydb/core/formats/arrow/hash/calcer.h>
#include <contrib/ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <contrib/ydb/core/tx/program/program.h>
#include <contrib/ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <contrib/ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

std::vector<std::shared_ptr<IPortionDataChunk>> TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const {
    auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());

    for (auto& colReader : reader) {
        for (colReader.Start(); colReader.IsCorrect(); colReader.ReadNextChunk()) {
            auto cArray = colReader.GetCurrentChunk()->GetChunkedArray();
            for (auto&& array : cArray->chunks()) {
                NArrow::SwitchType(array->type_id(), [&](const auto& type) {
                    using TWrap = std::decay_t<decltype(type)>;
                    using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

                    const TArray& arrTyped = static_cast<const TArray&>(*array);
                    if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                        for (int64_t i = 0; i < arrTyped.length(); ++i) {
                            auto cell = TCell::Make(arrTyped.Value(i));
                            sketch->Count(cell.Data(), cell.Size());
                        }
                        return true;
                    }
                    if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                        for (int64_t i = 0; i < arrTyped.length(); ++i) {
                            auto view = arrTyped.GetView(i);
                            sketch->Count(view.data(), view.size());
                        }
                        return true;
                    }
                    AFL_VERIFY(false)("message", "Unsupported arrow type for building an index");
                    return false;
                });
            }
        }
    }

    TString indexData(sketch->AsStringBuf());
    return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
}

}   // namespace NKikimr::NOlap::NIndexes::NCountMinSketch
