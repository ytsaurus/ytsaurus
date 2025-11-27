#include "columnar_statistics.h"

#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/library/formats/arrow_parser.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/statistics.h>

namespace NYT::NArrow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TColumnarStatisticsValueConsumer
    : public NTableClient::IValueConsumer
{
public:
    TColumnarStatisticsValueConsumer(NTableClient::TTableSchemaPtr schema)
        : Schema_(std::move(schema))
        , NameTable_(NTableClient::TNameTable::FromSchema(*Schema_))
        , Statistics_(TColumnarStatistics::MakeEmpty(Schema_->GetColumnCount()))
    { }

    const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const NTableClient::TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    { }

    void OnValue(const NTableClient::TUnversionedValue& value) override
    {
        Builder_.AddValue(value);
    }

    void OnEndRow() override
    {
        Statistics_.Update({Builder_.FinishRow()});
    }

    TColumnarStatistics GetStatistics() const
    {
        return Statistics_;
    }

private:
    const NTableClient::TTableSchemaPtr Schema_;
    const NTableClient::TNameTablePtr NameTable_;

    NTableClient::TUnversionedOwningRowBuilder Builder_;
    TColumnarStatistics Statistics_;
};

template <typename TypedStatistics, typename Mapper>
void AddTypedStatistics(
    NTableClient::TColumnarStatistics* columnarStatistics,
    int fieldIndex,
    std::shared_ptr<parquet20::Statistics> statistics,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    const std::shared_ptr<arrow20::RecordBatch>& batch)
{
    TColumnarStatisticsValueConsumer consumer(NArrow::CreateYTTableSchemaFromArrowSchema(batch->schema()));
    PARQUET_THROW_NOT_OK(NFormats::DecodeRecordBatch(batch, &consumer));
    return consumer.GetStatistics();
}

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    parquet20::FileMetaData& parquetFileMeta)
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
                            case parquet20::Type::BOOLEAN:
                                AddTypedStatistics<parquet20::BoolStatistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedBooleanValue(v); });
                                break;
                            case parquet20::Type::INT32:
                                AddTypedStatistics<parquet20::Int32Statistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedInt64Value(v); });
                                break;
                            case parquet20::Type::INT64:
                                AddTypedStatistics<parquet20::Int64Statistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedInt64Value(v); });
                                break;
                            case parquet20::Type::FLOAT:
                                AddTypedStatistics<parquet20::FloatStatistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedDoubleValue(v); });
                                break;
                            case parquet20::Type::DOUBLE:
                                AddTypedStatistics<parquet20::DoubleStatistics>(&columnarStatistics, fieldIndex, statistics, [](auto v) { return MakeUnversionedDoubleValue(v); });
                                break;
                            case parquet20::Type::BYTE_ARRAY:
                                AddTypedStatistics<parquet20::ByteArrayStatistics>(
                                    &columnarStatistics, fieldIndex, statistics,
                                    [](const parquet20::ByteArray& v) {
                                        return MakeUnversionedStringValue(TStringBuf(reinterpret_cast<const char*>(v.ptr), v.len));
                                    });
                                break;
                            case parquet20::Type::INT96:
                            case parquet20::Type::FIXED_LEN_BYTE_ARRAY:
                            case parquet20::Type::UNDEFINED:
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
