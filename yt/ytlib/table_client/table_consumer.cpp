#include "stdafx.h"
#include "sync_writer.h"
#include "table_consumer.h"
#include "key.h"
#include "config.h"

namespace NYT {
namespace NTableClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(const TTableConsumerConfigPtr& config, const ISyncWriterPtr& writer)
    : Config(config)
    , Writer(writer)
    , KeyColumns(writer->GetKeyColumns())
    , InsideRow(false)
    , ValueConsumer(&RowBuffer)
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

    Offsets.push_back(RowBuffer.GetSize());
    RowBuffer.Write(name);
    Offsets.push_back(RowBuffer.GetSize());

    Forward(&ValueConsumer);
}

void TTableConsumer::OnMyEndMap()
{
    YASSERT(InsideRow);
    YASSERT(Offsets.size() % 2 == 0);

    TNonOwningKey key(KeyColumns.IsInitialized() ? KeyColumns->size() : 0);
    TRow row;
    row.reserve(Offsets.size() / 2);

    {
        // Process records in backwards order to use last value for duplicate columns.
        UsedColumns.clear();
        int index = Offsets.size();
        int begin = RowBuffer.GetSize();
        while (index > 0) {
            int end = begin;
            begin = Offsets[--index];
            TStringBuf value(RowBuffer.Begin() + begin, end - begin);

            end = begin;
            begin = Offsets[--index];
            TStringBuf name(RowBuffer.Begin() + begin, end - begin);

            if (!UsedColumns.insert(name).second) {
                if (Config->Strict) {
                    ythrow yexception() << Sprintf(
                        "Duplicate column name %s (RowIndex: %"PRId64")", 
                        ~ToString(name).Quote(),
                        Writer->GetRowCount());
                } else {
                    // Ignore duplicate columns.
                    continue;
                }
            }

            auto it = KeyColumnToIndex.find(name);
            if (it != KeyColumnToIndex.end()) {
                key.SetKeyPart(it->second, value, Lexer);
            }

            row.push_back(std::make_pair(name, value));
        }
    }

    if (KeyColumns) {
        if (CompareKeys(Writer->GetLastKey(), key) > 0) {
            ythrow yexception() << Sprintf(
                "Table data is not sorted (RowIndex: %"PRId64", PreviousKey: %s, CurrentKey: %s)", 
                Writer->GetRowCount(),
                ~ToString(Writer->GetLastKey()),
                ~ToString(key));
        }
    }

    Writer->WriteRow(row, key);

    Offsets.clear();
    RowBuffer.Clear();
    InsideRow = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
