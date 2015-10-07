#include "stdafx.h"
#include "yamr_writer.h"

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TYamrConsumer::TYamrConsumer(TOutputStream* stream, TYamrFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , Table(
        Config->FieldSeparator,
        Config->RecordSeparator,
        Config->EnableEscaping, // Enable key escaping
        Config->EnableEscaping, // Enable value escaping
        Config->EscapingSymbol,
        true)
    , State(EState::None)
{
    
    YCHECK(Config);
    YCHECK(Stream);
}

TYamrConsumer::~TYamrConsumer()
{ }

void TYamrConsumer::OnInt64Scalar(i64 value)
{
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(::ToString(value));
        OnStringScalar(StringStorage_.back());
        return;
    }
    
    YASSERT(State == EState::ExpectAttributeValue);

    switch (ControlAttribute) {
    case EControlAttribute::TableIndex:
        if (!Config->EnableTableIndex) {
            // Silently ignore table switches.
            break;
        }

        if (Config->Lenval) {
            WritePod(*Stream, static_cast<ui32>(-1));
            WritePod(*Stream, static_cast<ui32>(value));
        } else {
            Stream->Write(ToString(value));
            Stream->Write(Config->RecordSeparator);
        }
        break;

    case EControlAttribute::RangeIndex:
        if (!Config->Lenval) {
            THROW_ERROR_EXCEPTION("Range indexes are not supported in text YAMR format");
        }
        WritePod(*Stream, static_cast<ui32>(-3));
        WritePod(*Stream, static_cast<ui32>(value));
        break;

    case EControlAttribute::RowIndex:
        if (!Config->Lenval) {
             THROW_ERROR_EXCEPTION("Row indexes are not supported in text YAMR format");
        }
        WritePod(*Stream, static_cast<ui32>(-4));
        WritePod(*Stream, static_cast<ui64>(value));
        break;

    default:
        YUNREACHABLE();
    }

    State = EState::ExpectEndAttributes;
}

void TYamrConsumer::OnUint64Scalar(ui64 value)
{
    YASSERT(State == EState::ExpectValue || State == EState::ExpectAttributeValue);
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(::ToString(value));
        OnStringScalar(StringStorage_.back());
        return;
    }
    THROW_ERROR_EXCEPTION("Uint64 attributes are not supported by YAMR");
}

void TYamrConsumer::OnDoubleScalar(double value)
{
    YASSERT(State == EState::ExpectValue || State == EState::ExpectAttributeValue);
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(::ToString(value));
        OnStringScalar(StringStorage_.back());
        return;
    }
    THROW_ERROR_EXCEPTION("Double attributes are not supported by YAMR");
}

void TYamrConsumer::OnBooleanScalar(bool value)
{
    YASSERT(State == EState::ExpectValue || State == EState::ExpectAttributeValue);
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(Stroka(FormatBool(value)));
        OnStringScalar(StringStorage_.back());
        return;
    }

    YASSERT(State == EState::ExpectAttributeValue);

    switch (ControlAttribute) {
    case EControlAttribute::KeySwitch:
        if (!Config->Lenval) {
            THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMR format");
        }
        WritePod(*Stream, static_cast<ui32>(-2));
        break;

    default:
        THROW_ERROR_EXCEPTION("Unknown boolean control attribute received");
    }

    State = EState::ExpectEndAttributes;
}

void TYamrConsumer::OnStringScalar(const TStringBuf& value)
{
    YCHECK(State != EState::ExpectAttributeValue);
    YASSERT(State == EState::ExpectValue);

    switch (ValueType) {
        case EValueType::ExpectKey:
            Key = value;
            break;

        case EValueType::ExpectSubkey:
            Subkey = value;
            break;

        case EValueType::ExpectValue:
            Value = value;
            break;

        case EValueType::ExpectUnknown:
            //Ignore unknows columns.
            break;

        default:
            YUNREACHABLE();
    }

    State = EState::ExpectColumnName;
}

void TYamrConsumer::OnEntity()
{
    if (State == EState::ExpectValue) {
        // Ignore nulls.
        State = EState::ExpectColumnName;
    } else {
        YCHECK(State == EState::ExpectEntity);
        State = EState::None;
    }
}

void TYamrConsumer::OnBeginList()
{
    YASSERT(State == EState::ExpectValue);
    THROW_ERROR_EXCEPTION("Lists are not supported by YAMR");
}

void TYamrConsumer::OnListItem()
{
    YASSERT(State == EState::None);
}

void TYamrConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TYamrConsumer::OnBeginMap()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by YAMR");
    }
    YASSERT(State == EState::None);
    State = EState::ExpectColumnName;

    Key = Null;
    Subkey = Null;
    Value = Null;
}

void TYamrConsumer::OnKeyedItem(const TStringBuf& key)
{
    switch (State) {
    case EState::ExpectColumnName:
        if (key == Config->Key) {
            ValueType = EValueType::ExpectKey;
        } else if (key == Config->Subkey) {
            ValueType = EValueType::ExpectSubkey;
        } else if (key == Config->Value) {
            ValueType = EValueType::ExpectValue;
        } else {
            ValueType = EValueType::ExpectUnknown;
        }

        State = EState::ExpectValue;
        break;

    case EState::ExpectAttributeName:
        ControlAttribute = ParseEnum<EControlAttribute>(ToString(key));
        State = EState::ExpectAttributeValue;
        break;

    case EState::None:
    case EState::ExpectValue:
    case EState::ExpectAttributeValue:
    case EState::ExpectEntity:
    case EState::ExpectEndAttributes:
    default:
        YUNREACHABLE();
    }
}

void TYamrConsumer::OnEndMap()
{
    YASSERT(State == EState::ExpectColumnName);
    State = EState::None;

    WriteRow();
}

void TYamrConsumer::OnBeginAttributes()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by YAMR");
    }

    YASSERT(State == EState::None);
    State = EState::ExpectAttributeName;
}

void TYamrConsumer::OnEndAttributes()
{
    YASSERT(State == EState::ExpectEndAttributes);
    State = EState::ExpectEntity;
}

void TYamrConsumer::WriteRow()
{
    if (!Key) {
        THROW_ERROR_EXCEPTION("Missing column %Qv in YAMR record",
            Config->Key);
    }

    if (!Value) {
        THROW_ERROR_EXCEPTION("Missing column %Qv in YAMR record",
            Config->Value);
    }

    TStringBuf key = *Key;
    TStringBuf subkey = Subkey ? *Subkey : "";
    TStringBuf value = *Value;

    if (!Config->Lenval) {
        EscapeAndWrite(key, true);
        Stream->Write(Config->FieldSeparator);
        if (Config->HasSubkey) {
            EscapeAndWrite(subkey, true);
            Stream->Write(Config->FieldSeparator);
        }
        EscapeAndWrite(value, false);
        Stream->Write(Config->RecordSeparator);
    } else {
        WriteInLenvalMode(key);
        if (Config->HasSubkey) {
            WriteInLenvalMode(subkey);
        }
        WriteInLenvalMode(value);
    }

    StringStorage_.clear();
}

void TYamrConsumer::WriteInLenvalMode(const TStringBuf& value)
{
    WritePod(*Stream, static_cast<ui32>(value.size()));
    Stream->Write(value);
}

void TYamrConsumer::EscapeAndWrite(const TStringBuf& value, bool inKey)
{
    if (Config->EnableEscaping) {
        WriteEscaped(
            Stream,
            value,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol);
    } else {
        Stream->Write(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessYamrWriter::TSchemalessYamrWriter(
    TNameTablePtr nameTable, 
    bool enableContextSaving,
    IAsyncOutputStreamPtr output,
    TYamrFormatConfigPtr config)
    : TSchemalessFormatWriterBase(nameTable, enableContextSaving, std::move(output))
    , Config_(config)
    , Table_(
        config->FieldSeparator,
        config->RecordSeparator,
        config->EnableEscaping, // Enable key escaping
        config->EnableEscaping, // Enable value escaping
        config->EscapingSymbol,
        true)
{ }

void TSchemalessYamrWriter::EscapeAndWrite(const TStringBuf& value, bool inKey)
{
    auto* stream = GetOutputStream();
    if (Config_->EnableEscaping) {
        WriteEscaped(
            stream,
            value,
            inKey ? Table_.KeyStops : Table_.ValueStops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        stream->Write(value);
    }
}

void TSchemalessYamrWriter::WriteInLenvalMode(const TStringBuf& value)
{
    auto* stream = GetOutputStream();
    WritePod(*stream, static_cast<ui32>(value.size()));
    stream->Write(value);
}

void TSchemalessYamrWriter::DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows)
{
    auto* stream = GetOutputStream();
  
    int maxNumberOfValuesPerRow = Config_->HasSubkey ? 3 : 2;

    for (const auto& row : rows) {
        if (row.GetCount() < 2) {
            THROW_ERROR_EXCEPTION("Row should consist of at least 2 values, found %v values instead", row.GetCount());
        }
        if (row.GetCount() > maxNumberOfValuesPerRow) {
            THROW_ERROR_EXCEPTION("Row should consist of at most %v values, found %v values instead", maxNumberOfValuesPerRow, row.GetCount()); 
        }
        
        int columnIndex = 0;

        TStringBuf key, subkey, value;

        if (row[columnIndex].Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Key (column #%v) should be of type String", columnIndex);
        }
        key = row[columnIndex].Data.String;
        columnIndex++;

        if (row.GetCount() == 3) {
            // If subkey has type Null, we consider it being equal to "".
            if (row[columnIndex].Type == EValueType::Null) {
                subkey = "";
            } else if (row[columnIndex].Type == EValueType::String) {
                subkey = row[columnIndex].Data.String;
            } else {
                THROW_ERROR_EXCEPTION("Subkey (column #%v) should be of type Null or String", columnIndex); 
            }
            columnIndex++;
        } else {
            subkey = "";
        }
        
        if (row[columnIndex].Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Value (column #%v) should be of type String", columnIndex);
        }
        value = row[columnIndex].Data.String;
        columnIndex++;
        
        if (!Config_->Lenval) {
            EscapeAndWrite(key, true);
            stream->Write(Config_->FieldSeparator);
            if (Config_->HasSubkey) {
                EscapeAndWrite(subkey, true);
                stream->Write(Config_->FieldSeparator);
            }
            EscapeAndWrite(value, false);
            stream->Write(Config_->RecordSeparator);
        } else {
            WriteInLenvalMode(key);
            if (Config_->HasSubkey) {
                WriteInLenvalMode(subkey);
            }
            WriteInLenvalMode(value);
        }

        TryFlushBuffer();
    }

    TryFlushBuffer();
}

void TSchemalessYamrWriter::WriteTableIndex(int tableIndex)
{
    auto* stream = GetOutputStream();
    
    if (!Config_->EnableTableIndex) {
        // Silently ignore table switches.
        return;
    }

    if (Config_->Lenval) {
        WritePod(*stream, static_cast<ui32>(-1));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    } else {
        stream->Write(ToString(tableIndex));
        stream->Write(Config_->RecordSeparator);
    }
}

void TSchemalessYamrWriter::WriteRangeIndex(i32 rangeIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMR format");
    }
    WritePod(*stream, static_cast<ui32>(-3));
    WritePod(*stream, static_cast<ui32>(rangeIndex));
}

void TSchemalessYamrWriter::WriteRowIndex(i64 rowIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
         THROW_ERROR_EXCEPTION("Row indices are not supported in text YAMR format");
    }
    WritePod(*stream, static_cast<ui32>(-4));
    WritePod(*stream, static_cast<ui64>(rowIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
