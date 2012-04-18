#include "stdafx.h"
#include "yson_writer.h"
#include "yson_format.h"

#include <ytlib/misc/serialize.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////
    
// Copied from <util/string/escape.cpp>
namespace {

static inline char HexDigit(char value) {
    YASSERT(value < 16);
    if (value < 10)
        return '0' + value;
    else
        return 'A' + value - 10;
}

static inline char OctDigit(char value) {
    YASSERT(value < 8);
    return '0' + value;
}

static inline bool IsPrintable(char c) {
    return c >= 32 && c <= 126;
}

static inline bool IsHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
}

static inline bool IsOctDigit(char c) {
    return  c >= '0' && c <= '7';
}

static const size_t ESCAPE_C_BUFFER_SIZE = 4;

static inline size_t EscapeC(unsigned char c, char next, char r[ESCAPE_C_BUFFER_SIZE]) {
    // (1) Printable characters go as-is, except backslash and double quote.
    // (2) Characters \r, \n, \t and \0 ... \7 replaced by their simple escape characters (if possible).
    // (3) Otherwise, character is encoded using hexadecimal escape sequence (if possible), or octal.
    if (c == '\"') {
        r[0] = '\\';
        r[1] = '\"';
        return 2;
    } else if (c == '\\') {
        r[0] = '\\';
        r[1] = '\\';
        return 2;
    } else if (IsPrintable(c)) {
        r[0] = c;
        return 1;
    } else if (c == '\r') {
        r[0] = '\\';
        r[1] = 'r';
        return 2;
    } else if (c == '\n') {
        r[0] = '\\';
        r[1] = 'n';
        return 2;
    } else if (c == '\t') {
        r[0] = '\\';
        r[1] = 't';
        return 2;
   } else if (c < 8 && !IsOctDigit(next)) {
        r[0] = '\\';
        r[1] = OctDigit(c);
        return 2;
    } else if (!IsHexDigit(next)) {
        r[0] = '\\';
        r[1] = 'x';
        r[2] = HexDigit((c & 0xF0) >> 4);
        r[3] = HexDigit((c & 0x0F) >> 0);
        return 4;
    } else {
        r[0] = '\\';
        r[1] = OctDigit((c & 0700) >> 6);
        r[2] = OctDigit((c & 0070) >> 3);
        r[3] = OctDigit((c & 0007) >> 0);
        return 4;
    }
}

void EscapeC(const char* str, size_t len, TOutputStream& output) {
    char buffer[ESCAPE_C_BUFFER_SIZE];

    size_t i, j;
    for (i = 0, j = 0; i < len; ++i) {
        size_t rlen = EscapeC(str[i], (i + 1 < len ? str[i + 1] : 0), buffer);

        if (rlen > 1) {
            output.Write(str + j, i - j);
            j = i + 1;
            output.Write(buffer, rlen);
        }
    }

    if (j > 0) {
        output.Write(str + j, len - j);
    } else {
        output.Write(str, len);
    }
}

}

////////////////////////////////////////////////////////////////////////////////

TYsonWriter::TYsonWriter(
    TOutputStream* stream,
    EYsonFormat format,
    bool formatRaw)
    : Stream(stream)
    , IsFirstItem(true)
    , IsEmptyEntity(false)
    , Indent(0)
    , Format(format)
    , FormatRaw(formatRaw)
{
    YASSERT(stream);
}

void TYsonWriter::WriteIndent()
{
    for (int i = 0; i < IndentSize * Indent; ++i) {
        Stream->Write(' ');
    }
}

void TYsonWriter::WriteStringScalar(const TStringBuf& value)
{
    if (Format == EYsonFormat::Binary) {
        Stream->Write(StringMarker);
        WriteVarInt32(Stream, static_cast<i32>(value.length()));
        Stream->Write(value.begin(), value.length());
    } else {
        Stream->Write('"');
        EscapeC(value.data(), value.length(), *Stream);
        Stream->Write('"');
    }
}

void TYsonWriter::WriteMapItem(const TStringBuf& name)
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

void TYsonWriter::OnStringScalar(const TStringBuf& value)
{
    WriteStringScalar(value);
}

void TYsonWriter::OnIntegerScalar(i64 value)
{
    if (Format == EYsonFormat::Binary) {
        Stream->Write(IntegerMarker);
        WriteVarInt64(Stream, value);
    } else {
        Stream->Write(ToString(value));
    }
}

void TYsonWriter::OnDoubleScalar(double value)
{
    if (Format == EYsonFormat::Binary) {
        Stream->Write(DoubleMarker);
        Stream->Write(&value, sizeof(double));
    } else {
        Stream->Write(ToString(value));
    }
}

void TYsonWriter::OnEntity()
{
    Stream->Write('#');
}

void TYsonWriter::OnBeginList()
{
    BeginCollection(BeginListSymbol);
}

void TYsonWriter::OnListItem()
{
    CollectionItem(ItemSeparator);
}

void TYsonWriter::OnEndList()
{
    EndCollection(EndListSymbol);
}

void TYsonWriter::OnBeginMap()
{
    BeginCollection(BeginMapSymbol);
}

void TYsonWriter::OnKeyedItem(const TStringBuf& type)
{
    WriteMapItem(type);
}

void TYsonWriter::OnEndMap()
{
    EndCollection(EndMapSymbol);
}

void TYsonWriter::OnBeginAttributes()
{
    BeginCollection(BeginAttributesSymbol);
}

void TYsonWriter::OnEndAttributes()
{
    EndCollection(EndAttributesSymbol);
    if (Format == EYsonFormat::Pretty) {
        Stream->Write(' ');
    }
}

void TYsonWriter::OnRaw(const TStringBuf& yson, EYsonType type)
{
    if (FormatRaw) {
        TYsonConsumerBase::OnRaw(yson, type);
    } else {
        Stream->Write(yson);
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonFragmentWriter::TYsonFragmentWriter(
    TOutputStream* stream,
    EYsonFormat format,
    bool formatRaw)
    : TYsonWriter(stream, format, formatRaw)
    , NestedCount(0)
{
    // this is a hack for not making an indent before first tokens (in pretty mode)
    Indent = -1;
}

void TYsonFragmentWriter::BeginCollection(char openBracket)
{
    ++NestedCount;
    TYsonWriter::BeginCollection(openBracket);
}

void TYsonFragmentWriter::CollectionItem(char separator)
{
    if (IsFirstItem) {
        if (Format == EYsonFormat::Pretty) {
            if (NestedCount != 0) {
                Stream->Write('\n');
            }
            ++Indent;
        }
    } else {
        Stream->Write(separator);
        if (Format == EYsonFormat::Pretty ||
           (Format == EYsonFormat::Text && NestedCount == 0))
        {
            Stream->Write('\n');
        }
    }
    if (Format == EYsonFormat::Pretty) {
        WriteIndent();
    }
    IsFirstItem = false;
}

void TYsonFragmentWriter::EndCollection(char closeBracket)
{
    TYsonWriter::EndCollection(closeBracket);
    --NestedCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
