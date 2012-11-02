#include "yamred_dsv_writer.h"

#include <ytree/yson_format.h>

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
{
    FOREACH (const auto& val, Config->KeyColumnNames) {
        KeyColumnNames.insert(val);
    }
    FOREACH (const auto& val, Config->SubkeyColumnNames) {
        SubkeyColumnNames.insert(val);
    }
}

TYamredDsvWriter::~TYamredDsvWriter()
{ }

void TYamredDsvWriter::OnIntegerScalar(i64 value)
{
    RememberValue(ToString(value));
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

    KeyFields.clear();
    SubkeyFields.clear();
    ValueBuffer.Clear();
}

void TYamredDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (State != EState::None) {
        // TODO(babenko): improve diagnostics
        THROW_ERROR_EXCEPTION("Missing value in YAMRed DSV");
    }
    Key = key;
    State = EState::ExpectingValue;
}

void TYamredDsvWriter::OnEndMap()
{
    AllowBeginMap = true;
    WriteRow();
}

void TYamredDsvWriter::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Attributes are not supported by YAMRed DSV");
}

void TYamredDsvWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

void TYamredDsvWriter::RememberValue(const TStringBuf& value)
{
    if (State != EState::ExpectingValue) {
        THROW_ERROR_EXCEPTION("Missing key in YAMRed DSV");
    }
    // Compare size before search for optimization.
    // It is not safe in case of repeated keys. Be careful!
    if (KeyFields.size() != KeyColumnNames.size() &&
        KeyColumnNames.count(Key))
    {
        YASSERT(KeyFields.count(Key) == 0);
        KeyFields[Key] = value;
    }
    else if (
        SubkeyFields.size() != SubkeyColumnNames.size() &&
        SubkeyColumnNames.count(Key))
    {
        YASSERT(SubkeyFields.count(Key) == 0);
        SubkeyFields[Key] = value;
    }
    else {
        if (IsValueEmpty) {
            IsValueEmpty = false;
        }
        else {
            ValueBuffer.Write(Config->FieldSeparator);
        }
        ValueBuffer.Write(Key);
        ValueBuffer.Write(Config->KeyValueSeparator);
        ValueBuffer.Write(value);
    }
    State = EState::None;
}
    
void TYamredDsvWriter::WriteRow()
{
    WriteYamrField(Config->KeyColumnNames, KeyFields);
    if (Config->HasSubkey) {
        WriteYamrField(Config->SubkeyColumnNames, SubkeyFields);
    }
    Stream->Write(ValueBuffer.Begin(), ValueBuffer.GetSize());
    Stream->Write(Config->RecordSeparator);
}

void TYamredDsvWriter::WriteYamrField(
    const std::vector<Stroka>& columnNames,
    const std::map<Stroka, Stroka>& fieldValues)
{
    for (int i = 0; i < columnNames.size(); ++i) {
        auto it = fieldValues.find(columnNames[i]);
        if (it == fieldValues.end()) {
            THROW_ERROR_EXCEPTION("Missing required column in YAMRed DSV: %s", ~columnNames[i]);
        }
        Stream->Write(it->second);
        if (i + 1 != columnNames.size()) {
            Stream->Write(Config->YamrKeysSeparator);
        }
    }
    Stream->Write(Config->FieldSeparator);
}


////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT

