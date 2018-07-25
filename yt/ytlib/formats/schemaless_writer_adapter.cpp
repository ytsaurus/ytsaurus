#include "schemaless_writer_adapter.h"
#include "config.h"

#include <yt/client/table_client/name_table.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NFormats {

using namespace NTableClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const i64 ContextBufferSize = static_cast<i64>(128 * 7) * 1024;
static const i64 ContextBufferCapacity = static_cast<i64>(1024) * 1024;

////////////////////////////////////////////////////////////////////////////////

TSchemalessFormatWriterBase::TSchemalessFormatWriterBase(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
    : NameTable_(nameTable)
    , Output_(output)
    , EnableContextSaving_(enableContextSaving)
    , ControlAttributesConfig_(controlAttributesConfig)
    , KeyColumnCount_(keyColumnCount)
    , NameTableReader_(std::make_unique<TNameTableReader>(NameTable_))
{
    CurrentBuffer_.Reserve(ContextBufferCapacity);

    EnableRowControlAttributes_ = ControlAttributesConfig_->EnableTableIndex ||
        ControlAttributesConfig_->EnableRangeIndex ||
        ControlAttributesConfig_->EnableRowIndex;

    try {
        RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
    } catch (const std::exception& ex) {
        Error_ = TError("Failed to add system columns to name table for a format writer") << ex;
    }
}

TFuture<void> TSchemalessFormatWriterBase::GetReadyEvent()
{
    return MakeFuture(Error_);
}

TFuture<void> TSchemalessFormatWriterBase::Close()
{
    try {
        DoFlushBuffer();
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
    }

    return MakeFuture(Error_);
}

const TTableSchema& TSchemalessFormatWriterBase::GetSchema() const
{
    Y_UNREACHABLE();
}

const TNameTablePtr& TSchemalessFormatWriterBase::GetNameTable() const
{
    return NameTable_;
}

TBlobOutput* TSchemalessFormatWriterBase::GetOutputStream()
{
    return &CurrentBuffer_;
}

TBlob TSchemalessFormatWriterBase::GetContext() const
{
    TBlob result;
    result.Append(PreviousBuffer_);
    result.Append(TRef::FromBlob(CurrentBuffer_.Blob()));
    return result;
}

void TSchemalessFormatWriterBase::TryFlushBuffer(bool force)
{
    if (CurrentBuffer_.Size() > ContextBufferSize || (!EnableContextSaving_ && force)) {
        DoFlushBuffer();
    }
}

i64 TSchemalessFormatWriterBase::GetWrittenSize() const
{
    return WrittenSize_;
}

void TSchemalessFormatWriterBase::FlushWriter()
{ }

void TSchemalessFormatWriterBase::DoFlushBuffer()
{
    FlushWriter();

    if (CurrentBuffer_.Size() == 0) {
        return;
    }

    WrittenSize_ += CurrentBuffer_.Size();

    PreviousBuffer_ = CurrentBuffer_.Flush();
    WaitFor(Output_->Write(PreviousBuffer_))
        .ThrowOnError();

    CurrentBuffer_.Reserve(ContextBufferCapacity);
}

bool TSchemalessFormatWriterBase::Write(const TRange<TUnversionedRow> &rows)
{
    if (!Error_.IsOK()) {
        return false;
    }

    try {
        DoWrite(rows);
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

bool TSchemalessFormatWriterBase::CheckKeySwitch(TUnversionedRow row, bool isLastRow)
{
    if (!ControlAttributesConfig_->EnableKeySwitch) {
        return false;
    }

    bool needKeySwitch = false;
    try {
        needKeySwitch = CurrentKey_ && CompareRows(row, CurrentKey_, KeyColumnCount_);
        CurrentKey_ = row;
    } catch (const std::exception& ex) {
        // COMPAT(psushin): composite values are not comparable anymore.
        THROW_ERROR_EXCEPTION("Cannot inject key switch into output stream") << ex;
    }

    if (isLastRow && CurrentKey_) {
        // After processing last row we create a copy of CurrentKey.
        LastKey_ = GetKeyPrefix(CurrentKey_, KeyColumnCount_);
        CurrentKey_ = LastKey_;
    }

    return needKeySwitch;
}

bool TSchemalessFormatWriterBase::IsSystemColumnId(int id) const
{
    return IsTableIndexColumnId(id) ||
        IsRangeIndexColumnId(id) ||
        IsRowIndexColumnId(id);
}

bool TSchemalessFormatWriterBase::IsTableIndexColumnId(int id) const
{
    return id == TableIndexId_;
}

bool TSchemalessFormatWriterBase::IsRowIndexColumnId(int id) const
{
    return id == RowIndexId_;
}

bool TSchemalessFormatWriterBase::IsRangeIndexColumnId(int id) const
{
    return id == RangeIndexId_;
}

int TSchemalessFormatWriterBase::GetRangeIndexColumnId() const
{
    return RangeIndexId_;
}

int TSchemalessFormatWriterBase::GetRowIndexColumnId() const
{
    return RowIndexId_;
}

int TSchemalessFormatWriterBase::GetTableIndexColumnId() const
{
    return TableIndexId_;
}

void TSchemalessFormatWriterBase::WriteControlAttributes(TUnversionedRow row)
{
    if (!EnableRowControlAttributes_) {
        return;
    }

    ++RowIndex_;

    TNullable<i64> tableIndex;
    TNullable<i64> rangeIndex;
    TNullable<i64> rowIndex;

    for (auto* it = row.Begin(); it != row.End(); ++it) {
        if (it->Id == TableIndexId_) {
            tableIndex = it->Data.Int64;
        } else if (it->Id == RowIndexId_) {
            rowIndex = it->Data.Int64;
        } else if (it->Id == RangeIndexId_) {
            rangeIndex = it->Data.Int64;
        }
    }

    bool needRowIndex = false;
    if (tableIndex && *tableIndex != TableIndex_) {
        if (ControlAttributesConfig_->EnableTableIndex)
            WriteTableIndex(*tableIndex);
        TableIndex_ = *tableIndex;
        needRowIndex = true;
    }

    if (rangeIndex && *rangeIndex != RangeIndex_) {
        if (ControlAttributesConfig_->EnableRangeIndex)
            WriteRangeIndex(*rangeIndex);
        RangeIndex_ = *rangeIndex;
        needRowIndex = true;
    }

    if (rowIndex) {
        needRowIndex = needRowIndex || (*rowIndex != RowIndex_);
        RowIndex_ = *rowIndex;
        if (ControlAttributesConfig_->EnableRowIndex && needRowIndex) {
            WriteRowIndex(*rowIndex);
        }
    }
}

void TSchemalessFormatWriterBase::WriteTableIndex(i64 tableIndex)
{ }

void TSchemalessFormatWriterBase::WriteRangeIndex(i64 rangeIndex)
{ }

void TSchemalessFormatWriterBase::WriteRowIndex(i64 rowIndex)
{ }

bool TSchemalessFormatWriterBase::HasError() const
{
    return !Error_.IsOK();
}

void TSchemalessFormatWriterBase::RegisterError(const TError& error)
{
    Error_ = error;
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterAdapter::TSchemalessWriterAdapter(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
    : TSchemalessFormatWriterBase(
        nameTable,
        std::move(output),
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount)
{ }

// CreateConsumerForFormat may throw an exception if there is no consumer for the given format,
// so we set Consumer_ inside Init function rather than inside the constructor.
void TSchemalessWriterAdapter::Init(const TFormat& format)
{
    // This is generic code for those formats, that support skipping nulls.
    // See #TYsonFormatConfig and #TJsonFormatConfig.
    SkipNullValues_ = format.Attributes().Get("skip_null_values", false);
    Consumer_ = CreateConsumerForFormat(format, EDataType::Tabular, GetOutputStream());
}

void TSchemalessWriterAdapter::DoWrite(const TRange<TUnversionedRow>& rows)
{
    int count = static_cast<int>(rows.Size());
    for (int index = 0; index < count; ++index) {
        if (CheckKeySwitch(rows[index], index + 1 == count /* isLastRow */)) {
            WriteControlAttribute(EControlAttribute::KeySwitch, true);
        }

        ConsumeRow(rows[index]);
        TryFlushBuffer(false);
    }

    TryFlushBuffer(true);
}

void TSchemalessWriterAdapter::FlushWriter()
{
    Consumer_->Flush();
}

template <class T>
void TSchemalessWriterAdapter::WriteControlAttribute(
    EControlAttribute controlAttribute,
    T value)
{
    BuildYsonListFragmentFluently(Consumer_.get())
        .Item()
        .BeginAttributes()
            .Item(FormatEnum(controlAttribute)).Value(value)
        .EndAttributes()
        .Entity();
}

void TSchemalessWriterAdapter::WriteTableIndex(i64 tableIndex)
{
    WriteControlAttribute(EControlAttribute::TableIndex, tableIndex);
}

void TSchemalessWriterAdapter::WriteRowIndex(i64 rowIndex)
{
    WriteControlAttribute(EControlAttribute::RowIndex, rowIndex);
}

void TSchemalessWriterAdapter::WriteRangeIndex(i64 rangeIndex)
{
    WriteControlAttribute(EControlAttribute::RangeIndex, rangeIndex);
}

void TSchemalessWriterAdapter::ConsumeRow(TUnversionedRow row)
{
    WriteControlAttributes(row);

    Consumer_->OnListItem();
    Consumer_->OnBeginMap();
    for (auto* it = row.Begin(); it != row.End(); ++it) {
        auto& value = *it;

        if (IsSystemColumnId(value.Id)) {
            continue;
        }

        if (value.Type == EValueType::Null && SkipNullValues_) {
            continue;
        }

        Consumer_->OnKeyedItem(NameTableReader_->GetName(value.Id));
        WriteYsonValue(Consumer_.get(), value);
    }
    Consumer_->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

void WriteYsonValue(IYsonConsumer* writer, const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Int64:
            writer->OnInt64Scalar(value.Data.Int64);
            break;
        case EValueType::Uint64:
            writer->OnUint64Scalar(value.Data.Uint64);
            break;
        case EValueType::Double:
            writer->OnDoubleScalar(value.Data.Double);
            break;
        case EValueType::Boolean:
            writer->OnBooleanScalar(value.Data.Boolean);
            break;
        case EValueType::String:
            writer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
            break;
        case EValueType::Null:
            writer->OnEntity();
            break;
        case EValueType::Any:
            writer->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
            break;
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
