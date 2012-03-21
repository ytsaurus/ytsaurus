#include "stdafx.h"
#include "yson_row_consumer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TRowConsumer::TRowConsumer(ISyncWriter* writer)
    : Writer(writer)
    , RowIndex(0)
    , InsideRow(false)
{ }

void TRowConsumer::OnStringScalar(const Stroka& value, bool hasAttributes)
{
    CheckNoAttributes(hasAttributes);
    CheckInsideRow();

    Writer->Write(Column, TValue(value));
}

void TRowConsumer::OnInt64Scalar(i64 value, bool hasAttributes)
{
    CheckNoAttributes(hasAttributes);
    CheckInsideRow();

    ythrow yexception() << Sprintf("Table value cannot be an integer (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnDoubleScalar(double value, bool hasAttributes)
{
    CheckNoAttributes(hasAttributes);
    CheckInsideRow();

    ythrow yexception() << Sprintf("Table value cannot be a double (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);

    ythrow yexception() << Sprintf("Table value cannot be an entity (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnBeginList()
{
    ythrow yexception() << Sprintf("Table value cannot be a list (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnListItem()
{
    YUNREACHABLE();
}

void TRowConsumer::OnEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TRowConsumer::OnBeginMap()
{
    if (InsideRow) {
        ythrow yexception() << Sprintf("Table value cannot be a map (RowIndex: %d)", RowIndex);
    }
    InsideRow = true;
}

void TRowConsumer::OnMapItem(const Stroka& name)
{
    YASSERT(InsideRow);
    Column = name;
}

void TRowConsumer::OnEndMap(bool hasAttributes)
{
    CheckNoAttributes(hasAttributes);
    YASSERT(InsideRow);
    Writer->EndRow();
    InsideRow = false;
    ++RowIndex;
}

void TRowConsumer::OnBeginAttributes() 
{
    YUNREACHABLE();
}

void TRowConsumer::OnAttributesItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TRowConsumer::OnEndAttributes()
{
    YUNREACHABLE();
}

void TRowConsumer::CheckNoAttributes(bool hasAttributes)
{
    if (hasAttributes) {
        ythrow yexception() << Sprintf("Table value cannot have attributes (RowIndex: %d)", RowIndex);
    }
}

void TRowConsumer::CheckInsideRow()
{
    if (!InsideRow) {
        ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", RowIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
