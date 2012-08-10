#include "stdafx.h"
#include "table_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"
#include "size_limits.h"

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/encoding_writer.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/chunk_client/private.h>
#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableWriterLogger;

static const int RangeColumnIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TTableChunkWriter::TTableChunkWriter(
    TChunkWriterConfigPtr config,
    NChunkClient::IAsyncWriterPtr chunkWriter,
    const std::vector<TChannel>& channels,
    const TNullable<TKeyColumns>& keyColumns)
    : TChunkWriterBase(config, chunkWriter, keyColumns)
    , Channels(channels)
    , IsOpen(false)
    , SamplesSize(0)
    , IndexSize(0)
    , BasicMetaSize(0)
{
    YASSERT(config);
    YASSERT(chunkWriter);

    MiscExt.set_codec_id(Config->CodecId);

    // Init trash channel.
    auto trashChannel = TChannel::CreateUniversal();
    FOREACH (const auto& channel, Channels) {
        trashChannel -= channel;
    }
    Channels.push_back(trashChannel);

    FOREACH (const auto& channel, Channels) {
        *ChannelsExt.add_items()->mutable_channel() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel.GetColumns().size()));
    }

    BasicMetaSize = ChannelsExt.ByteSize() + MiscExt.ByteSize();

    if (KeyColumns) {
        MiscExt.set_sorted(true);
        CurrentKey.ClearAndResize(KeyColumns->size());
        LastKey.ClearAndResize(KeyColumns->size());

        for (int keyIndex = 0; keyIndex < KeyColumns->size(); ++keyIndex) {
            const auto& column = KeyColumns->at(keyIndex);
            auto& columnInfo = ColumnMap[column];
            columnInfo.KeyColumnIndex = keyIndex;

            SelectChannels(column, columnInfo);
        }
    } else {
        MiscExt.set_sorted(false);
    }
}

void TTableChunkWriter::SelectChannels(const TStringBuf& name, TColumnInfo& columnInfo)
{
    for (int channelIndex = 0; channelIndex < Channels.size(); ++channelIndex) {
        const auto& channel = Channels[channelIndex];

        for (int columnIndex = 0; columnIndex < channel.GetColumns().size(); ++columnIndex) {
            const auto& fixed = channel.GetColumns()[columnIndex];
            if (fixed == name) {
                columnInfo.Channels.push_back(TChannelColumn(ChannelWriters[channelIndex], columnIndex));
            }
        }

        if (channel.ContainsInRanges(name)) {
            columnInfo.Channels.push_back(TChannelColumn(ChannelWriters[channelIndex], RangeColumnIndex));
        }
    }
}

TAsyncError TTableChunkWriter::AsyncOpen()
{
    // No thread affinity check here:
    // TChunkSequenceWriter may call it from different threads.
    YASSERT(!IsOpen);
    YASSERT(!State.IsClosed());

    IsOpen = true;
    return State.GetOperationError();
}

void TTableChunkWriter::FinalizeRow(const TRow& row)
{
    FOREACH (const auto& writer, ChannelWriters) {
        writer->EndRow();
    }

    if (SamplesSize < Config->SampleRate * DataWeight * EncodingWriter->GetCompressionRatio()) {
        EmitSample(row);
    }

    RowCount += 1;

    CurrentSize = EncodingWriter->GetCompressedSize();
    for (int channelIndex = 0; channelIndex < static_cast<int>(ChannelWriters.size()); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];
        CurrentSize += channel->GetCurrentSize();
        if (channel->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
            PrepareBlock(channelIndex);
        }
    }
}

auto TTableChunkWriter::GetColumnInfo(const TStringBuf& name) ->TColumnInfo& 
{
    auto it = ColumnMap.find(name);
    if (it == ColumnMap.end()) {
        ColumnNames.push_back(name.ToString());
        auto& columnInfo = ColumnMap[ColumnNames.back()];
        SelectChannels(name, columnInfo);
        return columnInfo;
    }
    return it->second;
}

void TTableChunkWriter::WriteValue(const std::pair<TStringBuf, TStringBuf>& value, const TColumnInfo& columnInfo)
{
    FOREACH (auto& channel, columnInfo.Channels) {
        if (channel.ColumnIndex == RangeColumnIndex) {
            channel.Writer->WriteRange(value.first, value.second);
        } else {
            channel.Writer->WriteFixed(channel.ColumnIndex, value.second);
        }
    }

    DataWeight += value.first.size();
    DataWeight += value.second.size();
    ValueCount += 1;
}

bool TTableChunkWriter::TryWriteRow(const TRow& row)
{
    YASSERT(IsOpen);
    YASSERT(!State.IsClosed());

    if (!State.IsActive() || !EncodingWriter->IsReady())
        return false;

    DataWeight += 1;
    FOREACH (const auto& pair, row) {
        //ToDo: check column name length.
        //ToDo: check column map size.

        auto& columnInfo = GetColumnInfo(pair.first);

        if (columnInfo.LastRow == RowCount) {
            if (Config->AllowDuplicateColumnNames) {
                // Ignore second and subsequent values with the same column name.
                continue;
            }
            State.Fail(TError(Sprintf("Duplicate column name %s", ~Stroka(pair.first).Quote())));
            return false;
        }

        columnInfo.LastRow = RowCount;

        WriteValue(pair, columnInfo);

        if (columnInfo.KeyColumnIndex >= 0) {
            CurrentKey.SetKeyPart(columnInfo.KeyColumnIndex, pair.second, Lexer);
        }
    }

    FinalizeRow(row);

    if (KeyColumns) {
        if (CompareKeys(LastKey, CurrentKey) > 0) {
            State.Fail(TError(Sprintf(
                "Sort order violation (PreviousKey: %s, CurrentKey: %s)", 
                ~ToString(LastKey),
                ~ToString(CurrentKey))));
            return false;
        }

        LastKey = CurrentKey;
        ProcessKey();
    }

    return true;
}

// We beleive that 
//  1. row doesn't contain duplicate column names.
//  2. data is sorted
// All checks are disabled.

bool TTableChunkWriter::TryWriteRowUnsafe(const TRow& row, const TNonOwningKey& key)
{
    if (TryWriteRowUnsafe(row)) {
        LastKey = key;
        ProcessKey();

        return true;
    }

    return false;
}

bool TTableChunkWriter::TryWriteRowUnsafe(const TRow& row)
{
    YASSERT(IsOpen);
    YASSERT(!State.IsClosed());

    if (!State.IsActive() || !EncodingWriter->IsReady())
        return false;

    DataWeight += 1;
    FOREACH (const auto& pair, row) {
        auto& columnInfo = GetColumnInfo(pair.first);
        WriteValue(pair, columnInfo);
    }

    FinalizeRow(row);

    return true;
}

void TTableChunkWriter::ProcessKey()
{
    if (RowCount == 1) {
        *BoundaryKeysExt.mutable_start() = LastKey.ToProto();
    }

    if (IndexSize < Config->IndexRate * DataWeight * EncodingWriter->GetCompressionRatio()) {
        EmitIndexEntry();
    }
}

void TTableChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = ChannelWriters[channelIndex];

    auto* blockInfo = ChannelsExt.mutable_items(channelIndex)->add_blocks();
    blockInfo->set_row_count(channel->GetCurrentRowCount());
    blockInfo->set_block_index(CurrentBlockIndex);

    ++CurrentBlockIndex;

    int size = 0;
    auto blockParts(channel->FlushBlock());
    FOREACH (auto& part, blockParts) {
        size += part.Size();
    }
    blockInfo->set_block_size(size);

    EncodingWriter->WriteBlock(MoveRV(blockParts));
}

TTableChunkWriter::~TTableChunkWriter()
{ }

i64 TTableChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

const TOwningKey& TTableChunkWriter::GetLastKey() const 
{
    return LastKey;
}

void TTableChunkWriter::SetLastKey(const TOwningKey& key)
{
    LastKey = key;
}

i64 TTableChunkWriter::GetRowCount() const
{
    return RowCount;
}

TAsyncError TTableChunkWriter::AsyncClose()
{
    YASSERT(IsOpen);
    YASSERT(!State.IsClosed());

    LOG_DEBUG("Closing writer (KeyColumnCount: %d)", static_cast<int>(ColumnNames.size()));

    State.StartOperation();

    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];

        if (channel->GetCurrentRowCount()) {
            PrepareBlock(channelIndex);
        }
    }

    EncodingWriter->AsyncFlush().Subscribe(BIND(
        &TTableChunkWriter::OnFinalBlocksWritten,
        MakeWeak(this)).Via(WriterThread->GetInvoker()));

    return State.GetOperationError();
}

void TTableChunkWriter::OnFinalBlocksWritten(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    CurrentSize = EncodingWriter->GetCompressedSize();

    SetProtoExtension(Meta.mutable_extensions(), SamplesExt);

    if (KeyColumns) {
        *BoundaryKeysExt.mutable_end() = LastKey.ToProto();

        const auto lastIndexRow = --IndexExt.items().end();
        if (RowCount > lastIndexRow->row_index() + 1) {
            auto* item = IndexExt.add_items();
            *item->mutable_key() = LastKey.ToProto();
            item->set_row_index(RowCount - 1);
        }

        SetProtoExtension(Meta.mutable_extensions(), IndexExt);
        SetProtoExtension(Meta.mutable_extensions(), BoundaryKeysExt);
        {
            NProto::TKeyColumnsExt keyColumnsExt;
            ToProto(keyColumnsExt.mutable_values(), KeyColumns.Get());
            SetProtoExtension(Meta.mutable_extensions(), keyColumnsExt);
        }
    }

    FinalizeWriter();
}

void TTableChunkWriter::EmitIndexEntry()
{
    auto* item = IndexExt.add_items();
    *item->mutable_key() = LastKey.ToProto();
    // RowCount is already increased
    item->set_row_index(RowCount - 1);
    IndexSize += LastKey.GetSize();
}

void TTableChunkWriter::EmitSample(const TRow& row)
{
    auto item = SamplesExt.add_items();

    std::map<TStringBuf, TStringBuf> sortedRow(row.begin(), row.end());
    FOREACH (const auto& pair, sortedRow) {
        auto* part = item->add_parts();
        part->set_column(pair.first.begin(), pair.first.size());
        // sizeof(i32) for type field.
        SamplesSize += sizeof(i32);

        Lexer.Reset();
        YCHECK(Lexer.Read(pair.second));
        YASSERT(Lexer.GetState() == TLexer::EState::Terminal);
        auto& token = Lexer.GetToken();
        switch (token.GetType()) {
            case ETokenType::Integer:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateValue(
                    token.GetIntegerValue()).ToProto();
                SamplesSize += sizeof(i64);
                break;

            case ETokenType::String: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(EKeyPartType::String);
                auto partSize = std::min(token.GetStringValue().size(), MaxKeySize);
                keyPart->set_str_value(token.GetStringValue().begin(), partSize);
                SamplesSize += partSize;
                break;
            }

            case ETokenType::Double:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateValue(
                    token.GetDoubleValue()).ToProto();
                SamplesSize += sizeof(double);
                break;

            default:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateSentinel(EKeyPartType::Composite).ToProto();
                break;
        }
    }
}

NChunkHolder::NProto::TChunkMeta TTableChunkWriter::GetMasterMeta() const
{
    YASSERT(State.IsClosed());

    NChunkHolder::NProto::TChunkMeta meta;
    meta.set_type(EChunkType::Table);
    SetProtoExtension(meta.mutable_extensions(), MiscExt);
    if (KeyColumns) {
        SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt);
    }

    return meta;
}

NChunkHolder::NProto::TChunkMeta TTableChunkWriter::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

i64 TTableChunkWriter::GetMetaSize() const
{
    return BasicMetaSize + SamplesSize + IndexSize + (CurrentBlockIndex + 1) * sizeof(NProto::TBlockInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
