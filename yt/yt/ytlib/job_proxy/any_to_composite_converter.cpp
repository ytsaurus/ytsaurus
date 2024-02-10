#include "any_to_composite_converter.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NJobProxy {

using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TAnyToCompositeConverter::TAnyToCompositeConverter(
    ISchemalessFormatWriterPtr underlyingWriter,
    std::vector<TTableSchemaPtr>& schemas,
    const TNameTablePtr& nameTable)
    : UnderlyingWriter_(std::move(underlyingWriter))
    , TableIndexId_(nameTable->GetIdOrRegisterName(TableIndexColumnName))
    , RowBuffer_(New<TRowBuffer>())
{
    for (const auto& schema : schemas) {
        auto& isComposite = TableIndexToIsComposite_.emplace_back();
        for (const auto& column : schema->Columns()) {
            if (IsV3Composite(column.LogicalType())) {
                auto index = nameTable->GetIdOrRegisterName(column.Name());
                isComposite.resize(index + 1, false);
                isComposite[index] = true;
            }
        }
    }
}

TFuture<void> TAnyToCompositeConverter::GetReadyEvent()
{
    return UnderlyingWriter_->GetReadyEvent();
}

bool TAnyToCompositeConverter::Write(TRange<TUnversionedRow> rows)
{
    return UnderlyingWriter_->Write(ConvertAnyToComposite(rows));
}

bool TAnyToCompositeConverter::WriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch)
{
    return UnderlyingWriter_->Write(ConvertAnyToComposite(rowBatch->MaterializeRows()));
}

TBlob TAnyToCompositeConverter::GetContext() const
{
    return UnderlyingWriter_->GetContext();
}

i64 TAnyToCompositeConverter::GetWrittenSize() const
{
    return UnderlyingWriter_->GetWrittenSize();
}

TFuture<void> TAnyToCompositeConverter::Close()
{
    return UnderlyingWriter_->Close();
}

static bool DoesValueNeedConversion(
    const std::vector<bool>& isComposite,
    const NTableClient::TUnversionedValue value)
{
    return value.Type == NTableClient::EValueType::Any &&
        value.Id < isComposite.size() &&
        isComposite[value.Id];
}

TUnversionedRow TAnyToCompositeConverter::ConvertAnyToComposite(TUnversionedRow row)
{
    auto tableIndex = -1;
    for (auto i = static_cast<int>(row.GetCount()) - 1; i >= 0; --i) {
        if (row[i].Id == TableIndexId_) {
            tableIndex = row[i].Data.Int64;
            break;
        }
    }
    YT_VERIFY(0 <= tableIndex && tableIndex < std::ssize(TableIndexToIsComposite_));
    const auto& isComposite = TableIndexToIsComposite_[tableIndex];

    auto firstValueToConvertIndex = -1;
    for (auto i = 0; i < static_cast<int>(row.GetCount()); ++i) {
        if (DoesValueNeedConversion(isComposite, row[i])) {
            firstValueToConvertIndex = i;
            break;
        }
    }
    if (firstValueToConvertIndex == -1) {
        return row;
    }

    auto convertedRow = RowBuffer_->CaptureRow(row, /*captureValues*/ false);
    for (auto i = firstValueToConvertIndex; i < static_cast<int>(convertedRow.GetCount()); ++i) {
        auto& value = convertedRow[i];
        if (DoesValueNeedConversion(isComposite, value)) {
            value.Type = EValueType::Composite;
        }
    }
    return convertedRow;
}

TRange<TUnversionedRow> TAnyToCompositeConverter::ConvertAnyToComposite(TRange<TUnversionedRow> rows)
{
    RowBuffer_->Clear();
    ConvertedRows_.clear();
    for (auto row : rows) {
        ConvertedRows_.push_back(ConvertAnyToComposite(row));
    }
    return ConvertedRows_;
}

TFuture<void> TAnyToCompositeConverter::Flush()
{
    return UnderlyingWriter_->Flush();
}

i64 TAnyToCompositeConverter::GetEncodedRowBatchCount() const
{
    return UnderlyingWriter_->GetEncodedRowBatchCount();
}

i64 TAnyToCompositeConverter::GetEncodedColumnarBatchCount() const
{
    return UnderlyingWriter_->GetEncodedColumnarBatchCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
