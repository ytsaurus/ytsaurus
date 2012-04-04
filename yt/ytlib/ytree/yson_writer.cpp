#include "stdafx.h"
#include "yson_writer.h"
#include "yson_format.h"

#include <ytlib/misc/serialize.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////
    
TYsonWriter::TYsonWriter(TOutputStream* stream, EYsonFormat format)
    : Stream(stream)
    , IsFirstItem(true)
    , IsEmptyEntity(false)
    , Indent(0)
    , Format(format)
{
    YASSERT(stream);
}

void TYsonWriter::WriteIndent()
{
    for (int i = 0; i < IndentSize * Indent; ++i) {
        Stream->Write(' ');
    }
}

void TYsonWriter::WriteStringScalar(const Stroka& value)
{
    if (Format == EYsonFormat::Binary) {
        Stream->Write(StringMarker);
        WriteVarInt32(Stream, static_cast<i32>(value.length()));
        Stream->Write(value.begin(), value.length());
    } else {
        Stream->Write('"');
        Stream->Write(EscapeC(value));
        Stream->Write('"');
    }
}

void TYsonWriter::WriteMapItem(const Stroka& name)
{
    CollectionItem(ItemSeparator);
    WriteStringScalar(name);
    if (Format == EYsonFormat::Pretty) {
        Stream->Write(' ');
    }
    Stream->Write(KeyValueSeparator);
    if (Format == EYsonFormat::Pretty) {
        Stream->Write(' ');
    }
    IsFirstItem = false;
}

void TYsonWriter::BeginCollection(char openBracket)
{
    Stream->Write(openBracket);
    IsFirstItem = true;
}

void TYsonWriter::CollectionItem(char separator)
{
    if (IsFirstItem) {
        if (Format == EYsonFormat::Pretty) {
            Stream->Write('\n');
            ++Indent;
        }
    } else {
        Stream->Write(separator);
        if (Format == EYsonFormat::Pretty) {
            Stream->Write('\n');
        }
    }
    if (Format == EYsonFormat::Pretty) {
        WriteIndent();
    }
    IsFirstItem = false;
}

void TYsonWriter::EndCollection(char closeBracket)
{
    if (Format == EYsonFormat::Pretty && !IsFirstItem) {
        Stream->Write('\n');
        --Indent;
        WriteIndent();
    }
    Stream->Write(closeBracket);
    IsFirstItem = false;
}

void TYsonWriter::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    WriteStringScalar(value);
    if (Format == EYsonFormat::Pretty && hasAttributes) {
        Stream->Write(' ');
    }
}

void TYsonWriter::OnIntegerScalar(i64 value, bool hasAttributes)
{
    if (Format == EYsonFormat::Binary) {
        Stream->Write(IntegerMarker);
        WriteVarInt64(Stream, value);
    } else {
        Stream->Write(ToString(value));
    }
    if (Format == EYsonFormat::Pretty && hasAttributes) {
        Stream->Write(' ');
    }
}

void TYsonWriter::OnDoubleScalar(double value, bool hasAttributes)
{
    if (Format == EYsonFormat::Binary) {
        Stream->Write(DoubleMarker);
        Stream->Write(&value, sizeof(double));
    } else {
        Stream->Write(ToString(value));
    }
    if (Format == EYsonFormat::Pretty && hasAttributes) {
        Stream->Write(' ');
    }
}

void TYsonWriter::OnEntity(bool hasAttributes)
{
    if (!hasAttributes) {
        Stream->Write(BeginAttributesSymbol);
        Stream->Write(EndAttributesSymbol);
    }
}

void TYsonWriter::OnBeginList()
{
    BeginCollection(BeginListSymbol);
}

void TYsonWriter::OnListItem()
{
    CollectionItem(ItemSeparator);
}

void TYsonWriter::OnEndList(bool hasAttributes)
{
    EndCollection(EndListSymbol);
    if (Format == EYsonFormat::Pretty && hasAttributes) {
        Stream->Write(' ');
    }
}

void TYsonWriter::OnBeginMap()
{
    BeginCollection(BeginMapSymbol);
}

void TYsonWriter::OnMapItem(const Stroka& name)
{
    WriteMapItem(name);
}

void TYsonWriter::OnEndMap(bool hasAttributes)
{
    EndCollection(EndMapSymbol);
    if (Format == EYsonFormat::Pretty && hasAttributes) {
        Stream->Write(' ');
    }
}

void TYsonWriter::OnBeginAttributes()
{
    BeginCollection(BeginAttributesSymbol);
}

void TYsonWriter::OnAttributesItem(const Stroka& name)
{
    WriteMapItem(name);
}

void TYsonWriter::OnEndAttributes()
{
    EndCollection(EndAttributesSymbol);
}

void TYsonWriter::OnRaw(const TYson& yson)
{
    Stream->Write(yson);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
