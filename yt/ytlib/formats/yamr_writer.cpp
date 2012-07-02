#include "stdafx.h"
#include "yamr_writer.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TYamrWriter::TYamrWriter(TOutputStream* stream, TYamrFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , AllowBeginMap(true)
    , State(EState::None)
{
    if (!Config) {
        Config = New<TYamrFormatConfig>();
    }
}

TYamrWriter::~TYamrWriter()
{ }

void TYamrWriter::OnIntegerScalar(i64 value)
{
    RememberItem(ToString(value));
}

void TYamrWriter::OnDoubleScalar(double value)
{
    RememberItem(ToString(value));
}

void TYamrWriter::OnStringScalar(const TStringBuf& value)
{
    RememberItem(Stroka(value));
}

void TYamrWriter::OnEntity()
{
    ythrow yexception() << "Entities are not supported by Yamr";
}

void TYamrWriter::OnBeginList()
{
    ythrow yexception() << "Lists are not supported by Yamr";
}

void TYamrWriter::OnListItem()
{ }

void TYamrWriter::OnEndList()
{
    YUNREACHABLE();
}

void TYamrWriter::OnBeginMap()
{
    if (!AllowBeginMap) {
        ythrow yexception() << "Embedded maps are not supported by Yamr";
    }
    AllowBeginMap = false;
    Key.clear();
    Subkey.clear();
    Value.clear();
}

void TYamrWriter::OnKeyedItem(const TStringBuf& key)
{
    if (key == Config->Key) {
        State = EState::ExpectingKey;
    } else if (Config->HasSubkey && key == Config->Subkey) {
        State = EState::ExpectingSubkey;
    } else if (key == Config->Value) {
        State = EState::ExpectingValue;
    }
}

void TYamrWriter::OnEndMap()
{
    AllowBeginMap = true;
    WriteRow();
}

void TYamrWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported by Yamr";
}

void TYamrWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

void TYamrWriter::RememberItem(const Stroka& item)
{
    switch (State) {
        case EState::None:
            return;
        case EState::ExpectingKey:
            Key = item;
            break;
        case EState::ExpectingSubkey:
            Subkey = item;
            break;
        case EState::ExpectingValue:
            Value = item;
            break;
        default:
            YUNREACHABLE();
    }
    State = EState::None;
}

void TYamrWriter::WriteRow()
{
    if (!Config->Lenval) {
        Stream->Write(Key);
        Stream->Write(Config->FieldSeparator);
        if (Config->HasSubkey) {
            Stream->Write(Subkey);
            Stream->Write(Config->FieldSeparator);
        }
        Stream->Write(Value);
        Stream->Write(Config->RowSeparator);
    } else {
        WriteInLenvalMode(Key);
        if (Config->HasSubkey) {
            WriteInLenvalMode(Subkey);
        }
        WriteInLenvalMode(Value);
    }
}

void TYamrWriter::WriteInLenvalMode(const Stroka& value)
{
    WritePod(*Stream, static_cast<i32>(value.size()));
    Stream->Write(value);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
