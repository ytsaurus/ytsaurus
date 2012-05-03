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

    auto offset = RowBuffer.GetSize();

    RowBuffer.Write(name);
    Offsets.push_back(RowBuffer.GetSize());

    TBlobRange column(RowBuffer.GetBlob(), offset, name.size());
    if (!UsedColumns.insert(column).second) {
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

    TRow row;

    int i = 0;
    auto begin = 0;
    auto end = 0;
    while (i < Offsets.size()) {
        begin = end;
        end = Offsets[i];
        TStringBuf name(RowBuffer.Begin() + begin, end - begin);
        ++i;

        begin = end;
        end = Offsets[i];
        TStringBuf value(RowBuffer.Begin() + begin, end - begin);
        ++i;

        row.push_back(std::make_pair(name, value));
    }

    Writer->WriteRow(row, CurrentKey);

    CurrentKey.Reset();
    UsedColumns.clear();
    Offsets.clear();
    RowBuffer.Clear();

    InsideRow = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
