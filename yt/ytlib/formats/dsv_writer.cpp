#include "stdafx.h"
#include "dsv_writer.h"
#include "dsv_symbols.h"

#include <ytlib/misc/error.h>

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
    , InsideFirstLine(true)
    , InsideFirstItem(true)
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
    InitDsvSymbols(Config);
}

TDsvWriter::~TDsvWriter()
{ }

void TDsvWriter::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, IsValueStopSymbol);
}

void TDsvWriter::OnIntegerScalar(i64 value)
{
    Stream->Write(::ToString(value));
}

void TDsvWriter::OnDoubleScalar(double value)
{
    Stream->Write(::ToString(value));
}

void TDsvWriter::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by DSV");
}

void TDsvWriter::OnBeginList()
{
    if (!AllowBeginList) {
        THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
    }
    AllowBeginList = false;
}

void TDsvWriter::OnListItem()
{
    if (Config->LinePrefix) {
        Stream->Write(Config->LinePrefix.Get());
    }

    if (Type == EYsonType::Node && !InsideFirstLine) {
        Stream->Write(Config->RecordSeparator);
    }

    InsideFirstLine = false;
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
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
    }
    AllowBeginMap = false;
    AllowBeginList = false;

    InsideFirstItem = true;
}

void TDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (!InsideFirstItem || Config->LinePrefix) {
        Stream->Write(Config->FieldSeparator);
    }

    EscapeAndWrite(key, IsKeyStopSymbol);
    Stream->Write(Config->KeyValueSeparator);

    InsideFirstItem = false;
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
    THROW_ERROR_EXCEPTION("Attributes are not supported by DSV");
}

void TDsvWriter::OnEndAttributes()
{
    YUNREACHABLE();
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
