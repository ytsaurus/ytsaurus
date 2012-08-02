#include "stdafx.h"
#include "dsv_writer.h"

#include <ytree/yson_format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDsvWriter::TDsvWriter(
    TOutputStream* stream,
    EYsonType type,
    TDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , FirstLine(true)
    , FirstItem(true)
    , AllowBeginMap(true)
    , Type(type)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    if (Type == EYsonType::Node) {
        AllowBeginList = true;
    } else {
        AllowBeginList = false;
    }

    memset(IsKeyStopSymbol, 0, sizeof(IsKeyStopSymbol));
    IsKeyStopSymbol[Config->RecordSeparator] = true;
    IsKeyStopSymbol[Config->FieldSeparator] = true;
    IsKeyStopSymbol[Config->KeyValueSeparator] = true;
    IsKeyStopSymbol[Config->EscapingSymbol] = true;
    IsKeyStopSymbol['\0'] = true;

    // Note:: KeyValueSeparator is not escaped
    memset(IsValueStopSymbol, 0, sizeof(IsValueStopSymbol));
    IsValueStopSymbol[Config->RecordSeparator] = true;
    IsValueStopSymbol[Config->FieldSeparator] = true;
    IsValueStopSymbol[Config->EscapingSymbol] = true;
    IsValueStopSymbol['\0'] = true;

    // Init escaping table
    for (int i = 0; i < 256; ++i) {
        EscapingTable[i] = i;
    }
    EscapingTable['\0'] = '0';
    EscapingTable['\n'] = 'n';
    EscapingTable['\t'] = 't';
}

TDsvWriter::~TDsvWriter()
{ }

void TDsvWriter::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, IsValueStopSymbol);
}

void TDsvWriter::OnIntegerScalar(i64 value)
{
    Stream->Write(ToString(value));
}

void TDsvWriter::OnDoubleScalar(double value)
{
    Stream->Write(ToString(value));
}

void TDsvWriter::OnEntity()
{
    ythrow yexception() << "Entities are not supported by DSV";
}

void TDsvWriter::OnBeginList()
{
    if (!AllowBeginList) {
        ythrow yexception() << "Embedded lists are not supported by DSV";
    }
    AllowBeginList = false;
}

void TDsvWriter::OnListItem()
{
    if (Config->LinePrefix) {
        Stream->Write(Config->LinePrefix.Get());
    }

    if (Type == EYsonType::Node && !FirstLine) {
        Stream->Write(Config->RecordSeparator);
    }

    FirstLine = false;
}

void TDsvWriter::OnEndList()
{
    if (Type == EYsonType::Node) {
        Stream->Write(Config->RecordSeparator);
    }
}

void TDsvWriter::OnBeginMap()
{
    if (!AllowBeginMap) {
        ythrow yexception() << "Embedded maps are not supported by DSV";
    }
    AllowBeginMap = false;
    AllowBeginList = false;

    FirstItem = true;
}

void TDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (!FirstItem || Config->LinePrefix) {
        Stream->Write(Config->FieldSeparator);
    }

    EscapeAndWrite(key, IsKeyStopSymbol);
    Stream->Write(Config->KeyValueSeparator);

    FirstItem = false;
}

void TDsvWriter::OnEndMap()
{
    AllowBeginMap = true;
    if (Type == EYsonType::ListFragment) {
        Stream->Write(Config->RecordSeparator);
    }
}

void TDsvWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported by DSV";
}

void TDsvWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

void TDsvWriter::OnRaw(const TStringBuf& yson, EYsonType type)
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

void TDsvWriter::EscapeAndWrite(const TStringBuf& key, const bool* IsStopSymbol)
{
    if (Config->EnableEscaping) {
        auto current = key.begin();
        auto end = key.end();
        while (current != end) {
            auto next = FindNextEscapedSymbol(current, end, IsStopSymbol);
            Stream->Write(current, next - current);
            if (next != end) {
                Stream->Write(Config->EscapingSymbol);
                Stream->Write(EscapingTable[static_cast<ui8>(*next)]);
                ++next;
            }
            current = next;
        }
    } else {
        Stream->Write(key);
    }
}

const char* TDsvWriter::FindNextEscapedSymbol(
    const char* begin,
    const char* end,
    const bool* IsStopSymbol)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsStopSymbol[static_cast<ui8>(*current)]) {
            return current;
        }
    }
    return end;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
