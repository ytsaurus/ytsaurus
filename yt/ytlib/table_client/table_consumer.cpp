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
    , ValueConsumer(&RowBuffer)
    , CurrentKey(KeyColumns ? KeyColumns->size() : 0)
{ }

void TTableConsumer::OnMyStringScalar(const TStringBuf& value)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %"PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyIntegerScalar(i64 value)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %"PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyDoubleScalar(double value)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %"PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyEntity()
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %"PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyBeginList()
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %"PRId64")", Writer->GetRowCount());
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

void TTableConsumer::OnMyKeyedItem(const TStringBuf& name)
{
    YASSERT(InsideRow);

    RowBuffer.Write(name);
    Offsets.push_back(RowBuffer.GetSize());

    if (!UsedColumns.insert(ToString(name)).second) {
        ythrow yexception() << Sprintf(
            "Invalid row format, duplicate column name (RowIndex: %"PRId64", Column: %s)", 
            Writer->GetRowCount(),
            ~ToString(name));
    }

    if (KeyColumns.IsInitialized()) {
        int keyIndex = -1;
        for(int i = 0; i < KeyColumns->size(); ++i) {
            if (name == KeyColumns->at(i)) {
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
    Offsets.push_back(RowBuffer.GetSize());
}

void TTableConsumer::OnMyEndMap()
{
    YASSERT(InsideRow);

    if (KeyColumns) {
        if (TKey::Compare(Writer->GetLastKey(), CurrentKey) > 0) {
            ythrow yexception() << Sprintf(
                "Invalid sorting order (RowIndex: %"PRId64", PreviousKey: %s, CurrentKey: %s)", 
                Writer->GetRowCount(),
                ~(Writer->GetLastKey().ToString()),
                ~CurrentKey.ToString());
        }
    }

    int i = 0;
    auto begin = 0;
    while (i < Offsets.size()) {
        auto end = Offsets[i];
        TStringBuf name(RowBuffer.Begin() + begin, end - begin);

        ++i;
        begin = end;
        end = Offsets[i];
        TStringBuf value(RowBuffer.Begin() + begin, end - begin);
        Row.push_back(std::make_pair(name, value));

        ++i;
    }

    Writer->WriteRow(Row, CurrentKey);

    CurrentKey.Reset();
    UsedColumns.clear();
    Offsets.clear();
    Row.clear();
    RowBuffer.Clear();

    InsideRow = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
