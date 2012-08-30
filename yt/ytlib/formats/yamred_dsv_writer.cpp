#include "yamred_dsv_writer.h"

#include <ytree/yson_format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamredDsvWriter::TYamredDsvWriter(TOutputStream* stream, TYamredDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config ? config : New<TYamredDsvFormatConfig>())
    , State(EState::None)
    , AllowBeginMap(true)
    , IsValueEmpty(true)
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
    ythrow yexception() << "Entities are not supported by Yamred Dsv";
}

void TYamredDsvWriter::OnBeginList()
{
    ythrow yexception() << "Lists are not supported by Yamred Dsv";
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
        ythrow yexception() << "Embedded maps are not supported by Yamred Dsv";
    }
    AllowBeginMap = false;

    KeyFields.clear();
    SubkeyFields.clear();
    ValueBuffer.Clear();
}

void TYamredDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (State != EState::None) {
        ythrow yexception() << "Pass key without value is forbidden";
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
    ythrow yexception() << "Attributes are not supported by Yamred Dsv";
}

void TYamredDsvWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

void TYamredDsvWriter::RememberValue(const TStringBuf& value)
{
    if (State != EState::ExpectingValue) {
        ythrow yexception() << "Pass value without key is forbidden";
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
            ythrow yexception() << "There is no required column with name " << columnNames[i];
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

