#include "yamred_dsv_writer.h"

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NTableClient;

// ToDo(psushin): consider extracting common base for TYamrWriter & TYamredDsvWriter
// Take a look at OnBeginAttributes, OnEndAttributes, EscapeAndWrite etc.

////////////////////////////////////////////////////////////////////////////////

TYamredDsvWriter::TYamredDsvWriter(TOutputStream* stream, TYamredDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , RowCount(-1)
    , State(EState::None)
    , Table(config)
{
    YCHECK(Stream);
    YCHECK(Config);

    FOREACH (const auto& name, Config->KeyColumnNames) {
        YCHECK(KeyColumnNames.insert(name));
        YCHECK(KeyFields.insert(std::make_pair(name, TColumnValue())).second);
    }
    FOREACH (const auto& name, Config->SubkeyColumnNames) {
        YCHECK(SubkeyColumnNames.insert(name));
        YCHECK(SubkeyFields.insert(std::make_pair(name, TColumnValue())).second);
    }
}

TYamredDsvWriter::~TYamredDsvWriter()
{ }

void TYamredDsvWriter::OnIntegerScalar(i64 value)
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Integer values are not supported by YAMRed DSV");
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

        default:
            YUNREACHABLE();
    }

    State = EState::ExpectEndAttributes;
}

void TYamredDsvWriter::OnDoubleScalar(double value)
{
    THROW_ERROR_EXCEPTION("Double values are not supported by YAMRed DSV");
}

void TYamredDsvWriter::OnStringScalar(const TStringBuf& value)
{
    YCHECK(State != EState::ExpectAttributeValue);
    YASSERT(State == EState::ExpectValue);
    State = EState::ExpectColumnName;

    // Compare size before search for optimization.
    // It is not safe in case of repeated keys. Be careful!
    if (KeyCount < KeyColumnNames.size()) {
        auto it = KeyFields.find(ColumnName);
        if (it != KeyFields.end()) {
            it->second.Value = value;
            it->second.RowIndex = RowCount;
            ++KeyCount;
            IncreaseLength(&KeyLength, CalculateLength(value, false));
            return;
        }
    }

    if (SubkeyCount < SubkeyColumnNames.size()) {
        auto it = SubkeyFields.find(ColumnName);
        if (it != SubkeyFields.end()) {
            it->second.Value = value;
            it->second.RowIndex = RowCount;
            ++SubkeyCount;
            IncreaseLength(&SubkeyLength, CalculateLength(value, false));
            return;
        }
    }

    ValueFields.push_back(ColumnName);
    ValueFields.push_back(value);
    IncreaseLength(&ValueLength, CalculateLength(ColumnName, true) + CalculateLength(value, false) + 1);
}

void TYamredDsvWriter::OnEntity()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Entities are not supported by YAMRed DSV");
    }
    YASSERT(State == EState::ExpectEntity);
    State = EState::None;
}

void TYamredDsvWriter::OnBeginList()
{
    YASSERT(State == EState::ExpectValue);
    THROW_ERROR_EXCEPTION("Lists are not supported by YAMRed DSV");
}

void TYamredDsvWriter::OnListItem()
{
    YASSERT(State == EState::None);
}

void TYamredDsvWriter::OnEndList()
{
    YUNREACHABLE();
}

void TYamredDsvWriter::OnBeginMap()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by YAMRed DSV");
    }

    YASSERT(State == EState::None);
    State = EState::ExpectColumnName;

    KeyCount = 0;
    SubkeyCount = 0;

    KeyLength = 0;
    SubkeyLength = 0;
    ValueLength = 0;

    ValueFields.clear();
    ++RowCount;
}

void TYamredDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    switch (State) {
    case EState::ExpectColumnName:
        ColumnName = key;
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

void TYamredDsvWriter::OnEndMap()
{
    WriteRow();
    State = EState::None;
}

void TYamredDsvWriter::OnBeginAttributes()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by YAMRed DSV");
    }

    YASSERT(State == EState::None);
    State = EState::ExpectAttributeName;
}

void TYamredDsvWriter::OnEndAttributes()
{
    YASSERT(State == EState::ExpectEndAttributes);
    State = EState::ExpectEntity;
}

void TYamredDsvWriter::WriteRow()
{
    if (Config->Lenval) {
        WritePod(*Stream, KeyLength);
        WriteYamrKey(Config->KeyColumnNames, KeyFields, KeyCount);

        if (Config->HasSubkey) {
            WritePod(*Stream, SubkeyLength);
            WriteYamrKey(Config->SubkeyColumnNames, SubkeyFields, SubkeyCount);
        }

        WritePod(*Stream, ValueLength);
        WriteYamrValue();
    } else {
        WriteYamrKey(Config->KeyColumnNames, KeyFields, KeyCount);
        Stream->Write(Config->FieldSeparator);

        if (Config->HasSubkey) {
            WriteYamrKey(Config->SubkeyColumnNames, SubkeyFields, SubkeyCount);
            Stream->Write(Config->FieldSeparator);
        }

        WriteYamrValue();
        Stream->Write(Config->RecordSeparator);
    }
}

void TYamredDsvWriter::WriteYamrKey(
    const std::vector<Stroka>& columnNames,
    const TDictionary& fieldValues,
    i32 fieldCount)
{
    if (fieldCount < columnNames.size()) {
        FOREACH (const auto& column, fieldValues) {
            if (column.second.RowIndex != RowCount) {
                THROW_ERROR_EXCEPTION("Missing column %s in YAMRed DSV",
                    ~Stroka(column.first).Quote());
            }
        }
        YUNREACHABLE();
    }

    auto nameIt = columnNames.begin();
    while (nameIt != columnNames.end()) {
        auto it = fieldValues.find(*nameIt);
        YASSERT(it != fieldValues.end());

        EscapeAndWrite(it->second.Value, false);
        ++nameIt;
        if (nameIt != columnNames.end()) {
            Stream->Write(Config->YamrKeysSeparator);
        }
    }
}

void TYamredDsvWriter::WriteYamrValue()
{
    YASSERT(ValueFields.size() % 2 == 0);

    auto it = ValueFields.begin();
    while (it !=  ValueFields.end()) {
        // Write key.
        EscapeAndWrite(*it, true);
        ++it;

        Stream->Write(Config->KeyValueSeparator);

        // Write value.
        EscapeAndWrite(*it, false);
        ++it;

        if (it != ValueFields.end()) {
            Stream->Write(Config->FieldSeparator);
        }
    }
}

void TYamredDsvWriter::EscapeAndWrite(const TStringBuf& string, bool inKey)
{
    if (Config->EnableEscaping) {
        WriteEscaped(
            Stream,
            string,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol);
    } else {
        Stream->Write(string);
    }
}

void TYamredDsvWriter::IncreaseLength(ui32* length, ui32 delta)
{
    if (*length > 0) {
        *length  += 1;
    }
    *length += delta;
}

ui32 TYamredDsvWriter::CalculateLength(const TStringBuf& string, bool inKey)
{
    return Config->EnableEscaping 
        ?  CalculateEscapedLength(
            string,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol)
        : string.length();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

