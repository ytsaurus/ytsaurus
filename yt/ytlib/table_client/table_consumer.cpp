#include "stdafx.h"

#include "sync_writer.h"
#include "table_consumer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*
TValidatingWriter::TValidatingWriter(
    const TSchema& schema, 
    IAsyncWriter* writer)
    : Writer(writer)
    , Schema(schema)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YASSERT(writer);
    {
        int columnIndex = 0;
        FOREACH(auto& keyColumn, Schema.KeyColumns()) {
            Attributes.add_key_columns(keyColumn);

            auto res = ColumnIndexes.insert(MakePair(keyColumn, columnIndex));
            YASSERT(res.Second());
            ++columnIndex;
        }

        FOREACH(auto& channel, Schema.GetChannels()) {
            FOREACH(auto& column, channel.GetColumns()) {
                auto res = ColumnIndexes.insert(MakePair(column, columnIndex));
                if (res.Second()) {
                    ++columnIndex;
                }
            }
        }

        IsColumnUsed.resize(columnIndex, false);
    }

    CurrentKey.resize(Schema.KeyColumns().size());

    // Fill protobuf chunk meta.
    FOREACH(auto channel, Schema.GetChannels()) {
        *Attributes.add_chunk_channels()->mutable_channel() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel, ColumnIndexes));
    }
    Attributes.set_is_sorted(false);
}

TAsyncError TValidatingWriter::AsyncOpen()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    return Writer->AsyncOpen(Attributes);
}

void TValidatingWriter::Write(const TColumn& column, TValue value)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    int columnIndex = TChannelWriter::UnknownIndex;
    auto it = ColumnIndexes.find(column);

    if (it == ColumnIndexes.end()) {
        auto res = UsedRangeColumns.insert(column);
        if (!res.Second()) {
            ythrow yexception() << Sprintf(
                "Column \"%s\" already used in the current row.", 
                ~column);
        }
    } else {
        columnIndex = it->Second();
        if (IsColumnUsed[columnIndex]) {
            ythrow yexception() << Sprintf(
                "Column \"%s\" already used in the current row.", 
                ~column);
        } else {
            IsColumnUsed[columnIndex] = true;
        }

        if (columnIndex < Schema.KeyColumns().size()) {
            CurrentKey[columnIndex] = value.ToString();
        }
    }

    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->Write(columnIndex, column, value);
    }
}

TAsyncError TValidatingWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    for (int columnIndex = 0; columnIndex < Schema.KeyColumns().size(); ++columnIndex) {
        if (!IsColumnUsed[columnIndex]) {
            FOREACH(auto& channelWriter, ChannelWriters) {
                channelWriter->Write(columnIndex, Schema.KeyColumns()[columnIndex], TStringBuf());
            }
        }
    }

    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->EndRow();
    }

    for (int i = 0; i < IsColumnUsed.size(); ++i)
        IsColumnUsed[i] = false;
    UsedRangeColumns.clear();

    TKey currentKey(Schema.KeyColumns().size());
    currentKey.swap(CurrentKey);

    return Writer->AsyncEndRow(currentKey, ChannelWriters);
}

TAsyncError TValidatingWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YASSERT(UsedRangeColumns.empty());
    for (int i = 0; i < IsColumnUsed.size(); ++i)
        YASSERT(!IsColumnUsed[i]);

    return Writer->AsyncClose(ChannelWriters);
}

*/
////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(const ISyncWriterPtr& writer)
    : Writer(writer)
    , KeyColumns(writer->GetKeyColumns())
    , InsideRow(false)
    , ValueOffset(0)
    , ValueConsumer(&RowBuffer)
    , CurrentKey(KeyColumns ? KeyColumns->size() : 0)
    , PreviousKey(KeyColumns ? KeyColumns->size() : 0)
{ }

void TTableConsumer::OnMyStringScalar(const TStringBuf& value, bool hasAttributes)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", Writer->GetRowCount());
}

void TTableConsumer::OnMyIntegerScalar(i64 value, bool hasAttributes)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", Writer->GetRowCount());
}

void TTableConsumer::OnMyDoubleScalar(double value, bool hasAttributes)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", Writer->GetRowCount());
}

void TTableConsumer::OnMyEntity(bool hasAttributes)
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", Writer->GetRowCount());
}

void TTableConsumer::OnMyBeginList()
{
    YASSERT(!InsideRow);
    ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", Writer->GetRowCount());
}

void TTableConsumer::OnMyListItem()
{
    YASSERT(!InsideRow);
    // Represents separator between rows, do nothing.
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
        ythrow yexception() << Sprintf("Invalid row format, duplicate column name (RowIndex: %d, Column: %s)", 
            Writer->GetRowCount(),
            ~CurrentColumn);
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

void TTableConsumer::OnMyEndMap(bool hasAttributes)
{
    CheckNoAttributes(hasAttributes);
    YASSERT(InsideRow);

    Writer->WriteRow(Row, CurrentKey);

    if (KeyColumns) {
        if (CurrentKey < PreviousKey) {
            ythrow yexception() << Sprintf("Invalid sorting order (RowIndex: %d, PreviousKey: %s, CurrentKey: %s)", 
                Writer->GetRowCount(),
                ~PreviousKey.ToString(),
                ~CurrentKey.ToString());
        }

        PreviousKey.Swap(CurrentKey);
        CurrentKey.Reset();
    }

    UsedColumns.clear();
    Row.clear();
    RowBuffer.Clear();
    ValueOffset = 0;

    InsideRow = false;
}

void TTableConsumer::CheckNoAttributes(bool hasAttributes)
{
    if (hasAttributes) {
        ythrow yexception() << Sprintf("Table value cannot have attributes (Writer->GetRowCount(): %d)", Writer->GetRowCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
