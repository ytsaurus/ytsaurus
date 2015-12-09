#include "schemaless_writer_adapter.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/common.h>
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

const i64 ContextBufferSize = (i64) 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TSchemalessFormatWriterBase::TSchemalessFormatWriterBase(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    bool enableKeySwitch,
    int keyColumnCount)
    : KeyColumnCount_(keyColumnCount)
    , NameTable_(nameTable)
    , EnableContextSaving_(enableContextSaving)
    , EnableKeySwitch_(enableKeySwitch)
    , Output_(CreateSyncAdapter(output))
{
    CurrentBuffer_.Reserve(ContextBufferSize);

    if (EnableContextSaving_) {
        PreviousBuffer_.Reserve(ContextBufferSize);
    }
}

TFuture<void> TSchemalessFormatWriterBase::Open()
{
    return VoidFuture;
}

TFuture<void> TSchemalessFormatWriterBase::GetReadyEvent()
{
    return MakeFuture(Error_);
}

TFuture<void> TSchemalessFormatWriterBase::Close()
{
    try {
        DoFlushBuffer();
        Output_->Finish();
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
    }

    return MakeFuture(Error_);
}

bool TSchemalessFormatWriterBase::IsSorted() const 
{
    return false;
}

TNameTablePtr TSchemalessFormatWriterBase::GetNameTable() const
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
    result.Append(TRef::FromBlob(PreviousBuffer_.Blob()));
    result.Append(TRef::FromBlob(CurrentBuffer_.Blob()));
    return result;
}

void TSchemalessFormatWriterBase::TryFlushBuffer(bool force)
{
    if ((CurrentBuffer_.Size() > ContextBufferSize) || (!EnableContextSaving_ && force)) {
        DoFlushBuffer();
    }
}

void TSchemalessFormatWriterBase::DoFlushBuffer()
{
    if (CurrentBuffer_.Size() == 0) {
        return;
    }

    const auto& buffer = CurrentBuffer_.Blob();
    Output_->Write(buffer.Begin(), buffer.Size());

    if (EnableContextSaving_) {
        std::swap(PreviousBuffer_, CurrentBuffer_);
    }
    CurrentBuffer_.Clear();
}

bool TSchemalessFormatWriterBase::Write(const std::vector<TUnversionedRow> &rows)
{
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
    if (!EnableKeySwitch_) {
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
        CurrentKey_ = LastKey_.Get();
    }

    return needKeySwitch;
}


////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterAdapter::TSchemalessWriterAdapter(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    bool enableKeySwitch,
    int keyColumnCount)
    : TSchemalessFormatWriterBase(
        nameTable, 
        std::move(output), 
        enableContextSaving, 
        enableKeySwitch, 
        keyColumnCount)
{ }

// CreateConsumerForFormat may throw an exception if there is no consumer for the given format,
// so we set Consumer_ inside Init function rather than inside the constructor.
void TSchemalessWriterAdapter::Init(const TFormat& format)
{
    Consumer_ = CreateConsumerForFormat(format, EDataType::Tabular, GetOutputStream());
}

void TSchemalessWriterAdapter::DoWrite(const std::vector<TUnversionedRow>& rows)
{
    for (int index = 0; index < static_cast<int>(rows.size()); ++index) {
        if (CheckKeySwitch(rows[index], index + 1 == rows.size() /* isLastRow */)) {
            WriteControlAttribute(EControlAttribute::KeySwitch, true);
        }

        ConsumeRow(rows[index]);
        TryFlushBuffer(false);
    }

    TryFlushBuffer(true);
}

void TSchemalessWriterAdapter::WriteTableIndex(i32 tableIndex)
{
    WriteControlAttribute(EControlAttribute::TableIndex, tableIndex);
}

void TSchemalessWriterAdapter::WriteRangeIndex(i32 rangeIndex)
{
    WriteControlAttribute(EControlAttribute::RangeIndex, rangeIndex);
}

void TSchemalessWriterAdapter::WriteRowIndex(i64 rowIndex)
{
    WriteControlAttribute(EControlAttribute::RowIndex, rowIndex);
}

template <class T>
void TSchemalessWriterAdapter::WriteControlAttribute(
    EControlAttribute controlAttribute,
    T value)
{
    BuildYsonListFluently(Consumer_.get())
        .Item()
        .BeginAttributes()
            .Item(FormatEnum(controlAttribute)).Value(value)
        .EndAttributes()
        .Entity();
}

void TSchemalessWriterAdapter::ConsumeRow(const TUnversionedRow& row)
{
    auto nameTable = GetNameTable();
    Consumer_->OnListItem();
    Consumer_->OnBeginMap();
    for (auto* it = row.Begin(); it != row.End(); ++it) {
        auto& value = *it;

        Consumer_->OnKeyedItem(nameTable->GetName(value.Id));
        switch (value.Type) {
            case EValueType::Int64:
                Consumer_->OnInt64Scalar(value.Data.Int64);
                break;
            case EValueType::Uint64:
                Consumer_->OnUint64Scalar(value.Data.Uint64);
                break;
            case EValueType::Double:
                Consumer_->OnDoubleScalar(value.Data.Double);
                break;
            case EValueType::Boolean:
                Consumer_->OnBooleanScalar(value.Data.Boolean);
                break;
            case EValueType::String:
                Consumer_->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                break;
            case EValueType::Null:
                Consumer_->OnEntity();
                break;
            case EValueType::Any:
                Consumer_->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                break;
            default:
                YUNREACHABLE();
        }
    }
    Consumer_->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
