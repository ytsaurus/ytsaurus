#include "columnar_statistics.h"

#include <parquet/statistics.h>
#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NArrow {

using namespace NTableClient;

namespace {
    template <typename TypedStatistics, typename Mapper>
    void AddTypedStatistics(
        NTableClient::TColumnarStatistics* columnarStatistics,
        int fieldIndex,
        std::shared_ptr<parquet::Statistics> statistics,
        Mapper mapper)
    {
        auto typedStatistics = std::dynamic_pointer_cast<TypedStatistics>(statistics);
        YT_VERIFY(typedStatistics);
        auto min = mapper(typedStatistics->min());
        auto& fieldMin = columnarStatistics->ColumnMinValues[fieldIndex];
        if (fieldMin.Type() == EValueType::Null || min < fieldMin) {
            fieldMin = min;
        }
        auto max = mapper(typedStatistics->max());
        auto& fieldMax = columnarStatistics->ColumnMaxValues[fieldIndex];
        if (fieldMax.Type() == EValueType::Null || max > fieldMax) {
            fieldMax = max;
        }
    }
} // namespace

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    parquet::FileMetaData& parquetFileMeta)
{
    auto columnarStatistics = NTableClient::TColumnarStatistics::MakeEmpty(
        parquetFileMeta.schema()->group_node()->field_count(),
        /*hasValueStatistics*/ true,
        /*hasLargeStatistics*/ false);
    columnarStatistics.ChunkRowCount = parquetFileMeta.num_rows();
    for (int rowGroupIndex = 0; rowGroupIndex < parquetFileMeta.num_row_groups(); ++rowGroupIndex) {
        auto rowGroupMeta = parquetFileMeta.RowGroup(rowGroupIndex);
        for (int columnIndex = 0; columnIndex < rowGroupMeta->num_columns(); ++columnIndex) {
            auto columnChunk = rowGroupMeta->ColumnChunk(columnIndex);
            auto columnRoot = parquetFileMeta.schema()->GetColumnRoot(columnIndex);
            auto fieldIndex = parquetFileMeta.schema()->group_node()->FieldIndex(*columnRoot);

            columnarStatistics.ColumnDataWeights[fieldIndex] += columnChunk->total_uncompressed_size();

            // The min/max are only usable if the column is present in a root schema.
            // This will be false for the fields of complex/composite types.
            if (&*parquetFileMeta.schema()->Column(columnIndex)->schema_node() == columnRoot) {
                if (auto statistics = columnChunk->statistics()) {
                    columnarStatistics.ColumnNonNullValueCounts[fieldIndex] += rowGroupMeta->num_rows() - statistics->null_count();

                    if (statistics->HasMinMax()) {
                        switch (columnChunk->type()) {
                            case parquet::Type::BOOLEAN:
                                AddTypedStatistics<parquet::BoolStatistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedBooleanValue(v); });
                                break;
                            case parquet::Type::INT32:
                                AddTypedStatistics<parquet::Int32Statistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedInt64Value(v); });
                                break;
                            case parquet::Type::INT64:
                                AddTypedStatistics<parquet::Int64Statistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedInt64Value(v); });
                                break;
                            case parquet::Type::FLOAT:
                                AddTypedStatistics<parquet::FloatStatistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedDoubleValue(v); });
                                break;
                            case parquet::Type::DOUBLE:
                                AddTypedStatistics<parquet::DoubleStatistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedDoubleValue(v); });
                                break;
                            case parquet::Type::BYTE_ARRAY:
                                AddTypedStatistics<parquet::ByteArrayStatistics>(
                                    &columnarStatistics, fieldIndex, statistics,
                                    [](const parquet::ByteArray& v) {
                                        return MakeUnversionedStringValue(TStringBuf(reinterpret_cast<const char*>(v.ptr), v.len));
                                    });
                                break;
                            case parquet::Type::INT96:
                            case parquet::Type::FIXED_LEN_BYTE_ARRAY:
                            case parquet::Type::UNDEFINED:
                                break;
                        }
                    }
                }
            }
        }
    }
    return columnarStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
