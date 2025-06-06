#pragma once
#include <contrib/ydb/core/tx/columnshard/counters/indexation.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <contrib/ydb/library/formats/arrow/splitter/stats.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NCompaction {

class TColumnMergeContext {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    ISnapshotSchema::TPtr SchemaInfo;
    YDB_ACCESSOR_DEF(TColumnSaver, Saver);
    YDB_READONLY_DEF(std::shared_ptr<TColumnLoader>, Loader);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, ResultField);
    YDB_READONLY(ui64, ChunkPackedBytesLimit, 7 * 1024 * 1024);
    YDB_READONLY(ui64, ExpectedBlobPackedBytes, 4 * 1024 * 1024);
    YDB_READONLY(ui64, ChunkRawBytesLimit, 50 * 1024 * 1024);
    YDB_READONLY(ui64, StorePackedChunkSizeLimit, 512 * 1024);
    YDB_READONLY(bool, UseWholeChunksOptimization, true);

    std::optional<NArrow::NSplitter::TColumnSerializationStat> ColumnStat;

    const TIndexInfo& IndexInfo;

public:
    std::shared_ptr<arrow::Scalar> GetDefaultValue() const {
        return Loader->GetDefaultValue();
    }

    ISnapshotSchema::TPtr GetSchemaInfo() const {
        return SchemaInfo;
    }

    const std::optional<NArrow::NSplitter::TColumnSerializationStat>& GetColumnStat() const {
        return ColumnStat;
    }

    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder() const {
        return NArrow::MakeBuilder(ResultField);
    }

    const TIndexInfo& GetIndexInfo() const {
        return IndexInfo;
    }

    TColumnMergeContext(const ui32 columnId, const ISnapshotSchema::TPtr& schema, const ui32 chunkRawBytesLimit,
        const std::optional<NArrow::NSplitter::TColumnSerializationStat>& columnStat)
        : ColumnId(columnId)
        , SchemaInfo(schema)
        , Saver(schema->GetColumnSaver(columnId))
        , Loader(schema->GetColumnLoaderOptional(columnId))
        , ResultField(schema->GetIndexInfo().GetColumnFieldVerified(columnId))
        , ChunkRawBytesLimit(chunkRawBytesLimit)
        , UseWholeChunksOptimization(!schema->GetIndexInfo().GetReplaceKey()->GetFieldByName(ResultField->name()))
        , ColumnStat(columnStat)
        , IndexInfo(schema->GetIndexInfo()) {
        Y_ABORT_UNLESS(ChunkRawBytesLimit);
    }
};
}   // namespace NKikimr::NOlap::NCompaction
