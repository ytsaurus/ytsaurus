#pragma once
#include <contrib/ydb/library/formats/arrow/simple_arrays_cache.h>
#include <contrib/ydb/core/tx/columnshard/counters/splitter.h>
#include <contrib/ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <contrib/ydb/core/tx/columnshard/engines/changes/compaction/common/result.h>
#include <contrib/ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <contrib/ydb/core/tx/columnshard/splitter/chunk_meta.h>
#include <contrib/ydb/core/tx/columnshard/splitter/chunks.h>

namespace NKikimr::NOlap::NCompaction {

class TColumnPortion: public TColumnPortionResult {
private:
    using TBase = TColumnPortionResult;
    std::unique_ptr<arrow::ArrayBuilder> Builder;
    std::shared_ptr<arrow::DataType> Type;
    const TColumnMergeContext& Context;
    YDB_READONLY(ui64, CurrentChunkRawSize, 0);
    double PredictedPackedBytes = 0;
    const TSimpleColumnInfo ColumnInfo;
    ui64 PackedSize = 0;

public:
    TColumnPortion(const TColumnMergeContext& context)
        : TBase(context.GetColumnId())
        , Context(context)
        , ColumnInfo(Context.GetIndexInfo().GetColumnFeaturesVerified(context.GetColumnId())) {
        Builder = Context.MakeBuilder();
        Type = Builder->type();
    }

    bool FlushBuffer();

    ui32 AppendSlice(const std::shared_ptr<arrow::Array>& a, const ui32 startIndex, const ui32 length);
};

}   // namespace NKikimr::NOlap::NCompaction
