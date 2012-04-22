#include "stdafx.h"

#include "sync_writer.h"
#include "table_consumer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(const ISyncWriterPtr& writer)
    : Writer(writer)
    , KeyColumns(writer->GetKeyColumns())
    , InsideRow(false)
    , ValueOffset(0)
    , ValueConsumer(&RowBuffer)
    , CurrentKey(KeyColumns ? KeyColumns->size() : 0)
{ }

void TTableConsumer::OnMyStringScalar(const TStringBuf& value)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: "PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyIntegerScalar(i64 value)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: "PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyDoubleScalar(double value)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: "PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyEntity()
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: "PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyBeginList()
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: "PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyListItem()
{
    YASSERT(!InsideRow);
    // Separator between rows, do nothing.
}

void TTableConsumer::OnMyBeginMap()
{
    YASSERT(!InsideRow);
    InsideRow = true;
}

void TTableConsumer::OnMyMapItem(const TStringBuf& name)
{
    YASSERT(InsideRow);

    {
        auto offset = RowBuffer.GetSize();
        RowBuffer.Write(name);
        CurrentColumn = TStringBuf(
            RowBuffer.Begin() + offset,
            RowBuffer.GetSize() - offset);
    }

    if (!UsedColumns.insert(CurrentColumn).second) {
        ythrow yexception() << Sprintf("Invalid row format, duplicate column name (RowIndex: "PRId64", Column: %s)", 
            Writer->GetRowCount(),
            ~CurrentColumn.ToString());
    }

    ValueOffset = RowBuffer.GetSize();

    if (KeyColumns.IsInitialized()) {
        int keyIndex = -1;
        for(int i = 0; i < KeyColumns->size(); ++i) {
            if (CurrentColumn == KeyColumns->at(i)) {
                keyIndex = i;
                break;
            }
        }

        if (keyIndex > 0)
            ValueConsumer.OnNewValue(&CurrentKey, keyIndex);
    }

    ForwardNode(&ValueConsumer, BIND(
        &TTableConsumer::OnValueEnded, 
        this));
}

void TTableConsumer::OnColumn()
{ }

void TTableConsumer::OnValueEnded()
{
    TStringBuf value(
        RowBuffer.Begin() + ValueOffset, 
        RowBuffer.GetSize() - ValueOffset);

    Row.push_back(std::make_pair(CurrentColumn, value));
}

void TTableConsumer::OnMyEndMap()
{
    YASSERT(InsideRow);

    if (KeyColumns) {
        if (TKey::Compare(Writer->GetLastKey(), CurrentKey) > 0) {
            ythrow yexception() << Sprintf("Invalid sorting order (RowIndex: "PRId64", PreviousKey: %s, CurrentKey: %s)", 
                Writer->GetRowCount(),
                ~(Writer->GetLastKey().ToString()),
                ~CurrentKey.ToString());
        }
    }

    Writer->WriteRow(Row, CurrentKey);

    CurrentKey.Reset();
    UsedColumns.clear();
    Row.clear();
    RowBuffer.Clear();
    ValueOffset = 0;

    InsideRow = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
