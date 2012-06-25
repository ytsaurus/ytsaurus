#include "stdafx.h"
#include "dsv_writer.h"

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

    memset(IsStopSymbol, 0, sizeof(IsStopSymbol));
    IsStopSymbol[Config->EscapingSymbol] = true;
    IsStopSymbol[Config->KeyValueSeparator] = true;
    IsStopSymbol[Config->FieldSeparator] = true;
    IsStopSymbol[Config->RecordSeparator] = true;
}

TDsvWriter::~TDsvWriter()
{ }

void TDsvWriter::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value);
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
    ythrow yexception() << "Entities are not supported by Dsv";
}

void TDsvWriter::OnBeginList()
{
    if (!AllowBeginList) {
        ythrow yexception() << "Embedded lists are not supported by Dsv";
    }
    AllowBeginList = false;
}

void TDsvWriter::OnListItem()
{
    if (!FirstLine) {
        Stream->Write(Config->RecordSeparator);
    }
    if (Config->LinePrefix) {
        Stream->Write(Config->LinePrefix.Get());
    }
    FirstLine = false;
}

void TDsvWriter::OnEndList()
{ }

void TDsvWriter::OnBeginMap()
{
    if (!AllowBeginMap) {
        ythrow yexception() << "Embedded maps are not supported by Dsv";
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

    EscapeAndWrite(key);
    Stream->Write(Config->KeyValueSeparator);

    FirstItem = false;
}

void TDsvWriter::OnEndMap()
{
    AllowBeginMap = true;
}

void TDsvWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported by Dsv";
}

void TDsvWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

void TDsvWriter::EscapeAndWrite(const TStringBuf& key)
{
    if (Config->EnableEscaping) {
        auto current = key.begin();
        auto end = key.end();
        while (current != end) {
            auto next = FindNextEscapedSymbol(current, end);
            Stream->Write(current, next - current);
            if (next != end) {
                Stream->Write(Config->EscapingSymbol);
                Stream->Write(*next);
                ++next;
            }
            current = next;
        }
    } else {
        Stream->Write(key);
    }
}

const char* TDsvWriter::FindNextEscapedSymbol(const char* begin, const char* end)
{
    auto current = begin;
    for ( ; current != end; ++current) {
        if (IsStopSymbol[*current]) {
            return current;
        }
    }
    return end;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
