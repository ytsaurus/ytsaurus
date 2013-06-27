#include "yamred_dsv_writer.h"

#include <yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamredDsvWriter::TYamredDsvWriter(TOutputStream* stream, TYamredDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , State(EState::None)
    , IsValueEmpty(true)
    , AllowBeginMap(true)
    , ExpectTableIndex(false)
    , Table(config)
{
    FOREACH (const auto& name, Config->KeyColumnNames) {
        KeyColumnNames.insert(name);
        KeyFields[name] = TStringBuf();
    }
    FOREACH (const auto& name, Config->SubkeyColumnNames) {
        SubkeyColumnNames.insert(name);
        SubkeyFields[name] = TStringBuf();
    }
}

TYamredDsvWriter::~TYamredDsvWriter()
{ }

void TYamredDsvWriter::OnIntegerScalar(i64 value)
{
    if (ExpectTableIndex) {
        YASSERT(value < std::numeric_limits<i64>::max());
        WritePod(*Stream, static_cast<i16>(value));
        ExpectTableIndex = false;
    } else {
        RememberValue(ToString(value));
    }
}

void TYamredDsvWriter::OnDoubleScalar(double value)
{
    RememberValue(ToString(value));
}

void TYamredDsvWriter::OnStringScalar(const TStringBuf& value)
{
    RememberValue(value);
}

void TYamredDsvWriter::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by YAMRed DSV");
}

void TYamredDsvWriter::OnBeginList()
{
    THROW_ERROR_EXCEPTION("Lists are not supported by YAMRed DSV");
}

void TYamredDsvWriter::OnListItem()
{ }

void TYamredDsvWriter::OnEndList()
{
    YUNREACHABLE();
}

void TYamredDsvWriter::OnBeginMap()
{
    if (!AllowBeginMap) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by YAMRed DSV");
    }
    AllowBeginMap = false;
    IsValueEmpty = true;

    KeyCount = 0;
    FOREACH (auto& elem, KeyFields) {
        elem.second = TStringBuf();
    }

    SubkeyCount = 0;
    FOREACH (auto& elem, SubkeyFields) {
        elem.second = TStringBuf();
    }

    ValueBuffer.Clear();
}

void TYamredDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (State != EState::None) {
        // TODO(babenko): improve diagnostics
        THROW_ERROR_EXCEPTION("Missing value in YAMRed DSV");
    }

    if (key == "table_index") {
        ExpectTableIndex = true;
    }
    else {
        Key = key;
        State = EState::ExpectingValue;
    }

}

void TYamredDsvWriter::OnEndMap()
{
    AllowBeginMap = true;
    WriteRow();
}

void TYamredDsvWriter::OnBeginAttributes()
{
    if (!Config->EnableTableIndex) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by YAMRed DSV");
    }
}

void TYamredDsvWriter::OnEndAttributes()
{
    if (!Config->EnableTableIndex) {
        YUNREACHABLE();
    }
}

void TYamredDsvWriter::RememberValue(const TStringBuf& value)
{
    if (State != EState::ExpectingValue) {
        THROW_ERROR_EXCEPTION("Missing key in YAMRed DSV");
    }
    // Compare size before search for optimization.
    // It is not safe in case of repeated keys. Be careful!
    if (KeyCount != KeyColumnNames.size() && KeyColumnNames.count(Key))
    {
        KeyCount += 1;
        KeyFields[Key] = value;
    } else if ( SubkeyCount != SubkeyColumnNames.size() && SubkeyColumnNames.count(Key)) {
        SubkeyCount += 1;
        SubkeyFields[Key] = value;
    } else {
        if (IsValueEmpty) {
            IsValueEmpty = false;
        } else {
            ValueBuffer.Write(Config->FieldSeparator);
        }

        EscapeAndWrite(&ValueBuffer, Key, true);
        ValueBuffer.Write(Config->KeyValueSeparator);
        EscapeAndWrite(&ValueBuffer, value, false);
    }
    State = EState::None;
}

void TYamredDsvWriter::WriteRow()
{
    WriteYamrField(Config->KeyColumnNames, KeyFields, KeyCount);
    if (Config->HasSubkey) {
        WriteYamrField(Config->SubkeyColumnNames, SubkeyFields, SubkeyCount);
    }
    if (Config->Lenval) {
        WritePod(*Stream, static_cast<i32>(ValueBuffer.GetSize()));
        Stream->Write(ValueBuffer.Begin(), ValueBuffer.GetSize());
    }
    else {
        Stream->Write(ValueBuffer.Begin(), ValueBuffer.GetSize());
        Stream->Write(Config->RecordSeparator);
    }
}

void TYamredDsvWriter::WriteYamrField(
    const std::vector<Stroka>& columnNames,
    const Dictionary& fieldValues,
    i32 fieldCount)
{
    if (fieldCount != columnNames.size()) {
        THROW_ERROR_EXCEPTION("Missing column in YAMRed DSV: actual %s, expected %d", fieldCount, columnNames.size());
    }

    if (Config->Lenval) {
        if (columnNames.size() == 0) {
            WritePod(*Stream, 0);
        }
        else {
            i32 length = (columnNames.size() - 1);
            for (int i = 0; i < columnNames.size(); ++i) {
                length += fieldValues.find(columnNames[i])->second.size();
            }
            WritePod(*Stream, length);

            for (int i = 0; i < columnNames.size(); ++i) {
                Stream->Write(fieldValues.find(columnNames[i])->second);
                if (i + 1 != columnNames.size()) {
                    Stream->Write(Config->YamrKeysSeparator);
                }
            }
        }
    }
    else {
        for (int i = 0; i < columnNames.size(); ++i) {
            EscapeAndWrite(Stream, fieldValues.find(columnNames[i])->second, false);
            if (i + 1 != columnNames.size()) {
                Stream->Write(Config->YamrKeysSeparator);
            }
        }
        Stream->Write(Config->FieldSeparator);
    }
}

void TYamredDsvWriter::EscapeAndWrite(TOutputStream* outputStream, const TStringBuf& string, bool inKey)
{
    if (Config->EnableEscaping) {
        WriteEscaped(
            outputStream,
            string,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol);
    } else {
        outputStream->Write(string);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

