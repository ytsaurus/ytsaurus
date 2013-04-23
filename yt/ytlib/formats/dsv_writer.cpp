#include "stdafx.h"
#include "dsv_writer.h"

#include <ytlib/misc/error.h>

#include <yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDsvWriter::TDsvWriter(
    TOutputStream* stream,
    EYsonType type,
    TDsvFormatConfigPtr config)
    : Stream(stream)
    , Type(type)
    , Config(config)
    , Table(config)
    , InsideFirstLine(true)
    , InsideFirstItem(true)
    , InsideAttributes(false)
    , AllowBeginList(type == EYsonType::Node)
    , AllowBeginMap(true)
    , AllowAttributes(true)
{
    YCHECK(Stream);
    YCHECK(Config);
}

TDsvWriter::~TDsvWriter()
{ }

void TDsvWriter::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, false);
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
    if (!AllowBeginList || InsideAttributes) {
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
    InsideFirstItem = true;
}

void TDsvWriter::OnEndList()
{
    if (Type == EYsonType::Node) {
        Stream->Write(Config->RecordSeparator);
    }
}

void TDsvWriter::OnBeginMap()
{
    if (!AllowBeginMap || InsideAttributes) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
    }
    AllowBeginMap = false;
    AllowBeginList = false;
    AllowAttributes = false;
}

void TDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (!InsideFirstItem || Config->LinePrefix) {
        Stream->Write(Config->FieldSeparator);
    }

    if (InsideAttributes && Config->WithAttributes) {
        Stream->Write(Config->AttributesPrefix);
    }

    EscapeAndWrite(key, true);
    Stream->Write(Config->KeyValueSeparator);

    InsideFirstItem = false;
}

void TDsvWriter::OnEndMap()
{
    AllowBeginMap = true;
    AllowAttributes = true;
    if (Type == EYsonType::ListFragment) {
        Stream->Write(Config->RecordSeparator);
    }
}

void TDsvWriter::OnBeginAttributes()
{
    if (!Config->WithAttributes || !AllowAttributes) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by DSV");
    }
    InsideAttributes = true;
}

void TDsvWriter::OnEndAttributes()
{
    if (!Config->WithAttributes) {
        YUNREACHABLE();
    }
    InsideAttributes = false;
}

void TDsvWriter::EscapeAndWrite(const TStringBuf& string, bool inKey)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
