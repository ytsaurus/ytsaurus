#include "common.h"

#include "yson_writer.h"
#include "yson_format.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////
    
TYsonWriter::TYsonWriter(TOutputStream* stream, bool isBinary)
    : Stream(stream)
    , IsFirstItem(false)
    , IsEmptyEntity(false)
    , Indent(0)
    , IsBinary(isBinary)
{ }

void TYsonWriter::WriteIndent()
{
    for (int i = 0; i < IndentSize * Indent; ++i) {
        Stream->Write(' ');
    }
}

void TYsonWriter::SetEmptyEntity()
{
    IsEmptyEntity = true;
}

void TYsonWriter::ResetEmptyEntity()
{
    IsEmptyEntity = false;
}

void TYsonWriter::FlushEmptyEntity()
{
    if (IsEmptyEntity) {
        Stream->Write("<>");
        IsEmptyEntity = false;
    }
}

void TYsonWriter::BeginCollection(char openBracket)
{
    Stream->Write(openBracket);
    IsFirstItem = true;
}

void TYsonWriter::CollectionItem(char separator)
{
    if (IsFirstItem) {
        Stream->Write('\n');
        ++Indent;
    } else {
        FlushEmptyEntity();
        Stream->Write(separator);
        Stream->Write('\n');
    }
    if (!IsBinary) {
        WriteIndent();
    }
    IsFirstItem = false;
}

void TYsonWriter::EndCollection(char closeBracket)
{
    FlushEmptyEntity();
    if (!IsFirstItem) {
        Stream->Write('\n');
        --Indent;
        if (!IsBinary) {
            WriteIndent();
        }
    }
    Stream->Write(closeBracket);
    IsFirstItem = false;
}


void TYsonWriter::StringScalar(const Stroka& value)
{
    if (IsBinary) {
        Stream->Write(StringMarker);
        Stream->Write(static_cast<i32>(value.size()));
        Stream->Write(value);
    } else {
        // TODO: escaping
        Stream->Write('"');
        Stream->Write(value);
        Stream->Write('"');
    }
}

void TYsonWriter::Int64Scalar(i64 value)
{
    if (IsBinary) {
        Stream->Write(Int64Marker);
        Stream->Write(value);
    } else {
        Stream->Write(ToString(value));
    }
}

void TYsonWriter::DoubleScalar(double value)
{
    if (IsBinary) {
        Stream->Write(DoubleMarker);
        Stream->Write(value);
    } else {
        Stream->Write(ToString(value));
    }
}

void TYsonWriter::EntityScalar()
{
    SetEmptyEntity();
}

void TYsonWriter::BeginList()
{
    BeginCollection('[');
}

void TYsonWriter::ListItem(int index)
{
    UNUSED(index);
    CollectionItem(ListItemSeparator);
}

void TYsonWriter::EndList()
{
    EndCollection(']');
}

void TYsonWriter::BeginMap()
{
    BeginCollection('{');
}

void TYsonWriter::MapItem(const Stroka& name)
{
    CollectionItem(MapItemSeparator);
    // TODO: escaping
    Stream->Write(name);
    Stream->Write(' ');
    Stream->Write(KeyValueSeparator);
    Stream->Write(' ');
}

void TYsonWriter::EndMap()
{
    EndCollection('}');
}


void TYsonWriter::BeginAttributes()
{
    if (IsEmptyEntity) {
        ResetEmptyEntity();
    } else {
        Stream->Write(' ');
    }
    BeginCollection('<');
}

void TYsonWriter::AttributesItem(const Stroka& name)
{
    CollectionItem(MapItemSeparator);
    // TODO: escaping
    Stream->Write(name);
    Stream->Write(' ');
    Stream->Write(KeyValueSeparator);
    Stream->Write(' ');
    IsFirstItem = false;
}

void TYsonWriter::EndAttributes()
{
    EndCollection('>');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
