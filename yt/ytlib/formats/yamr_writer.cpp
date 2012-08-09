#include "stdafx.h"
#include "yamr_writer.h"

#include <ytree/yson_format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

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

// TODO(panin): maybe get rid of this copy-paste from TDsvWriter
void TYamrWriter::OnRaw(const TStringBuf& yson, EYsonType type)
{
    // On raw is called only for values in table

    if (type != EYsonType::Node) {
        YUNIMPLEMENTED();
    }

    Lexer.Reset();
    Lexer.Read(yson);
    Lexer.Finish();

    YCHECK(Lexer.GetState() == TLexer::EState::Terminal);
    auto token = Lexer.GetToken();
    switch(token.GetType()) {
        case ETokenType::String:
            OnStringScalar(token.GetStringValue());
            break;

        case ETokenType::Integer:
            OnIntegerScalar(token.GetIntegerValue());
            break;

        case ETokenType::Double:
            OnDoubleScalar(token.GetDoubleValue());
            break;

        case EntityToken:
            ythrow yexception() << "Enitites are not supported as values in table";
            break;

        case BeginListToken:
            ythrow yexception() << "Lists are not supported as values in table";
            break;

        case BeginMapToken:
            ythrow yexception() << "Maps are not supported as values in table";
            break;

        case BeginAttributesToken:
            ythrow yexception() << "Attributes are not supported as values in table";
            break;

        default:
            YUNREACHABLE();
    }
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
        Stream->Write(Config->RecordSeparator);
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
