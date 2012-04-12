#include "stdafx.h"

#include "validating_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TValidatingWriter::TValidatingWriter(
    const TSchema& schema, 
    IAsyncBlockWriter* writer)
    : Writer(writer)
    , Schema(schema)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YASSERT(writer);
    {
        int columnIndex = 0;
        FOREACH (auto& keyColumn, Schema.KeyColumns()) {
            Attributes.add_key_columns(keyColumn);

            auto res = ColumnIndexes.insert(MakePair(keyColumn, columnIndex));
            YASSERT(res.Second());
            ++columnIndex;
        }

        FOREACH (auto& channel, Schema.GetChannels()) {
            FOREACH (auto& column, channel.GetColumns()) {
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
    FOREACH (auto channel, Schema.GetChannels()) {
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

    FOREACH (auto& channelWriter, ChannelWriters) {
        channelWriter->Write(columnIndex, column, value);
    }
}

TAsyncError TValidatingWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    for (int columnIndex = 0; columnIndex < Schema.KeyColumns().size(); ++columnIndex) {
        if (!IsColumnUsed[columnIndex]) {
            FOREACH (auto& channelWriter, ChannelWriters) {
                channelWriter->Write(columnIndex, Schema.KeyColumns()[columnIndex], TStringBuf());
            }
        }
    }

    FOREACH (auto& channelWriter, ChannelWriters) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
