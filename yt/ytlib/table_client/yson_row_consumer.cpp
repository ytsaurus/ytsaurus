#include "stdafx.h"
#include "yson_row_consumer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TRowConsumer::TRowConsumer(ISyncTableWriter* writer)
    : Writer(writer)
    , RowIndex(0)
    , InsideRow(false)
{ }

void TRowConsumer::OnStringScalar(const TStringBuf& value)
{
    CheckInsideRow();

    Writer->Write(Column, TValue(value));
}

void TRowConsumer::OnIntegerScalar(i64 value)
{
    CheckInsideRow();

    ythrow yexception() << Sprintf("Table value cannot be an integer (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnDoubleScalar(double value)
{
    CheckInsideRow();

    ythrow yexception() << Sprintf("Table value cannot be a double (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnEntity()
{
    ythrow yexception() << Sprintf("Table value cannot be an entity (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnBeginList()
{
    ythrow yexception() << Sprintf("Table value cannot be a list (RowIndex: %d)", RowIndex);
}

void TRowConsumer::OnListItem()
{
    // Represents separator between rows, do nothing.
}

void TRowConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TRowConsumer::OnBeginMap()
{
    if (InsideRow) {
        ythrow yexception() << Sprintf("Table value cannot be a map (RowIndex: %d)", RowIndex);
    }
    InsideRow = true;
}

void TRowConsumer::OnMapItem(const TStringBuf& name)
{
    YASSERT(InsideRow);
    Column.assign(name);
}

void TRowConsumer::OnEndMap()
{
    YASSERT(InsideRow);
    Writer->EndRow();
    InsideRow = false;
    ++RowIndex;
}

void TRowConsumer::OnBeginAttributes() 
{
    YUNREACHABLE();
}

void TRowConsumer::OnAttributesItem(const TStringBuf& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TRowConsumer::OnEndAttributes()
{
    YUNREACHABLE();
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
