#include "stdafx.h"

#include "sync_writer.h"
#include "table_consumer.h"
#include "key.h"

#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(const ISyncWriterPtr& writer)
    : Writer(writer)
    , KeyColumns(writer->GetKeyColumns())
    , InsideRow(false)
    , ValueConsumer(&RowBuffer)
    , CurrentKey(KeyColumns ? KeyColumns->size() : 0)
    , OnValueFinished_(BIND(&TTableConsumer::OnValueFinished, this))
{
    if (KeyColumns) {
        for (int index = 0; index < static_cast<int>(KeyColumns.Get().size()); ++index) {
            KeyColumnToIndex[KeyColumns.Get()[index]] = index;
        }
    }
}

void TTableConsumer::OnMyStringScalar(const TStringBuf& value)
{
    YASSERT(!InsideRow);
    ThrowMapExpected();
}

void TTableConsumer::OnMyIntegerScalar(i64 value)
{
    YASSERT(!InsideRow);
    ThrowMapExpected();
}

void TTableConsumer::OnMyDoubleScalar(double value)
{
    YASSERT(!InsideRow);
    ThrowMapExpected();
}

void TTableConsumer::OnMyEntity()
{
    YASSERT(!InsideRow);
    ThrowMapExpected();
}

void TTableConsumer::OnMyBeginList()
{
    YASSERT(!InsideRow);
    ThrowMapExpected();
}

void TTableConsumer::ThrowMapExpected()
{
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %"PRId64")", Writer->GetRowCount());
}

void TTableConsumer::OnMyListItem()
{
    YASSERT(!InsideRow);
    // Row separator, do nothing.
}

void TTableConsumer::OnMyBeginMap()
{
    YASSERT(!InsideRow);
    InsideRow = true;
}

void TTableConsumer::OnMyKeyedItem(const TStringBuf& name)
{
    YASSERT(InsideRow);

    TBlobRange column(RowBuffer.GetBlob(), RowBuffer.GetSize(), name.size());
    RowBuffer.Write(name);
    Offsets.push_back(RowBuffer.GetSize());

    if (!UsedColumns.insert(column).second) {
        ythrow yexception() << Sprintf(
            "Duplicate column name %s (RowIndex: %"PRId64")", 
            ~ToString(name).Quote(),
            Writer->GetRowCount());
    }

    ForwardNode(&ValueConsumer, OnValueFinished_);
}

void TTableConsumer::OnValueFinished()
{
    Offsets.push_back(RowBuffer.GetSize());
}

void TTableConsumer::OnMyEndMap()
{
    YASSERT(InsideRow);

    TLexer lexer;
    TKey<TFakeStrbufStore> key;
    TRow row;

    int index = 0;
    int begin = 0;
    int end = 0;
    while (index < Offsets.size()) {
        begin = end;
        end = Offsets[index++];
        TStringBuf name(RowBuffer.Begin() + begin, end - begin);

        begin = end;
        end = Offsets[index++];
        TStringBuf value(RowBuffer.Begin() + begin, end - begin);

        auto it = KeyColumnToIndex.find(name);
        if (it != KeyColumnToIndex.end()) {
            key.SetKeyPart(it->second, value, lexer);
        }

        row.push_back(std::make_pair(name, value));
    }


    if (KeyColumns) {
        if (CompareKeys(Writer->GetLastKey(), key) > 0) {
            ythrow yexception() << Sprintf(
                "Table data is not sorted (RowIndex: %"PRId64", PreviousKey: %s, CurrentKey: %s)", 
                Writer->GetRowCount(),
                ~Writer->GetLastKey().ToString(),
                ~CurrentKey.ToString());
        }
    }

    Writer->WriteRow(row, key);

    CurrentKey.Reset();
    UsedColumns.clear();
    Offsets.clear();
    RowBuffer.Clear();
    InsideRow = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
