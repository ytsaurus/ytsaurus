#include "stdafx.h"
#include "dsv_writer.h"

#include <ytlib/table_client/name_table.h>

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDsvWriterBase::TDsvWriterBase(
    TDsvFormatConfigPtr config)
    : Config_(config)
    , Table_(config, true)
{
    YCHECK(Config_);
}

void TDsvWriterBase::EscapeAndWrite(const TStringBuf& string, bool inKey, TOutputStream* stream)
{
    if (Config_->EnableEscaping) {
        WriteEscaped(
            stream,
            string,
            inKey ? Table_.KeyStops : Table_.ValueStops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        stream->Write(string);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessDsvWriter::TSchemalessDsvWriter(
    TNameTablePtr nameTable, 
    bool enableContextSaving,
    IAsyncOutputStreamPtr output,
    TDsvFormatConfigPtr config)
    : TSchemalessFormatWriterBase(nameTable, enableContextSaving, std::move(output))
    , TDsvWriterBase(config)
{ }

void TSchemalessDsvWriter::DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows)
{
    auto* output = GetOutputStream();
    for (const auto& row : rows) {
        bool firstValue = true;

        if (Config_->LinePrefix) {
            output->Write(Config_->LinePrefix.Get());
            firstValue = false;
        }

        for (const auto* value = row.Begin(); value != row.End(); ++value) {
            if (value->Type == EValueType::Null) {
                continue;
            }

            if (!firstValue) {
                output->Write(Config_->FieldSeparator);
            }

            WriteValue(*value);
            firstValue = false;
        }

        FinalizeRow(firstValue);
        TryFlushBuffer();
    }

    TryFlushBuffer();
}

void TSchemalessDsvWriter::FinalizeRow(bool firstValue)
{
    auto* output = GetOutputStream();
    if (Config_->EnableTableIndex) {
        if (!firstValue) {
            output->Write(Config_->FieldSeparator);
        }
        
        EscapeAndWrite(Config_->TableIndexColumn, true, output);
        output->Write(Config_->KeyValueSeparator);
        output->Write(::ToString(TableIndex_));
    }

    output->Write(Config_->RecordSeparator);
}

void TSchemalessDsvWriter::WriteValue(const TUnversionedValue& value) 
{
    auto nameTable = GetNameTable();
    auto* output = GetOutputStream();
    EscapeAndWrite(nameTable->GetName(value.Id), true, output);
    output->Write(Config_->KeyValueSeparator);

    switch (value.Type) {
        case EValueType::Int64:
            output->Write(::ToString(value.Data.Int64));
            break;

        case EValueType::Uint64:
            output->Write(::ToString(value.Data.Uint64));
            break;

        case EValueType::Double:
            output->Write(::ToString(value.Data.Double));
            break;

        case EValueType::Boolean:
            output->Write(FormatBool(value.Data.Boolean));
            break;

        case EValueType::String:
            EscapeAndWrite(TStringBuf(value.Data.String, value.Length), false, output);
            break;

        case EValueType::Any:
            THROW_ERROR_EXCEPTION("Values of type \"any\" are not supported by dsv format (Value: %v)", value);

        default:
            YUNREACHABLE();
    }
}

void TSchemalessDsvWriter::WriteTableIndex(int tableIndex)
{
    TableIndex_ = tableIndex;
}

void TSchemalessDsvWriter::WriteRangeIndex(i32 rangeIndex)
{
    THROW_ERROR_EXCEPTION("Range indexes are not supported by dsv format");
}

void TSchemalessDsvWriter::WriteRowIndex(i64 rowIndex)
{
    THROW_ERROR_EXCEPTION("Row indexes are not supported by dsv format");
}

////////////////////////////////////////////////////////////////////////////////

TDsvNodeConsumer::TDsvNodeConsumer(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvWriterBase(config)
    , AllowBeginList_(true)
    , AllowBeginMap_(true)
    , BeforeFirstMapItem_(true)
    , BeforeFirstListItem_(true)
    , Stream_(stream)
{ }

void TDsvNodeConsumer::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, false, Stream_);
}

void TDsvNodeConsumer::OnInt64Scalar(i64 value)
{
    Stream_->Write(::ToString(value));
}

void TDsvNodeConsumer::OnUint64Scalar(ui64 value)
{
    Stream_->Write(::ToString(value));
}

void TDsvNodeConsumer::OnDoubleScalar(double value)
{
    Stream_->Write(::ToString(value));
}

void TDsvNodeConsumer::OnBooleanScalar(bool value)
{
    Stream_->Write(FormatBool(value));
}

void TDsvNodeConsumer::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by DSV");
}

void TDsvNodeConsumer::OnBeginList()
{
    if (AllowBeginList_) {
        AllowBeginList_ = false;
    } else {
        THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
    }
}

void TDsvNodeConsumer::OnListItem()
{
    AllowBeginMap_ = true;
    if (BeforeFirstListItem_) {
        BeforeFirstListItem_ = false;
    } else {
        // Not first item.
        Stream_->Write(Config_->RecordSeparator);
    }
}

void TDsvNodeConsumer::OnEndList()
{
    Stream_->Write(Config_->RecordSeparator);
}

void TDsvNodeConsumer::OnBeginMap()
{
    if (AllowBeginMap_) {
        AllowBeginList_ = false;
        AllowBeginMap_ = false;
        BeforeFirstMapItem_ = true;
    } else {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
    }
}

void TDsvNodeConsumer::OnKeyedItem(const TStringBuf& key)
{
    YASSERT(!AllowBeginMap_);
    YASSERT(!AllowBeginList_);

    if (BeforeFirstMapItem_) {
        BeforeFirstMapItem_ = false;
    } else {
        Stream_->Write(Config_->FieldSeparator);
    }

    EscapeAndWrite(key, true, Stream_);
    Stream_->Write(Config_->KeyValueSeparator);
}

void TDsvNodeConsumer::OnEndMap()
{
    YASSERT(!AllowBeginMap_);
    YASSERT(!AllowBeginList_);
}

void TDsvNodeConsumer::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
}

void TDsvNodeConsumer::OnEndAttributes()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
