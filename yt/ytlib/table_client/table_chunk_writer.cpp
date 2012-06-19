#include "stdafx.h"
#include "table_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"
#include "size_limits.h"

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/chunk_client/async_writer.h>
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

////////////////////////////////////////////////////////////////////////////////

TTableChunkWriter::TTableChunkWriter(
    TChunkWriterConfigPtr config,
    NChunkClient::IAsyncWriterPtr chunkWriter,
    const std::vector<TChannel>& channels,
    const TNullable<TKeyColumns>& keyColumns)
    : TChunkWriterBase(chunkWriter, config)
    , Channels(channels)
    , KeyColumns(keyColumns)
    , IsOpen(false)
    , CurrentSize(0)
    , SamplesSize(0)
    , IndexSize(0)
    , BasicMetaSize(0)
{
    YASSERT(config);
    YASSERT(chunkWriter);

    MiscExt.set_row_count(0);
    MiscExt.set_value_count(0);
    MiscExt.set_codec_id(Config->CodecId);

    {
        int columnIndex = 0;

        if (KeyColumns) {
            MiscExt.set_sorted(true);
            FOREACH (const auto& column, KeyColumns.Get()) {
                if (ColumnIndexes.insert(MakePair(column, columnIndex)).second) {
                    ++columnIndex;
                }
            }
        } else {
            MiscExt.set_sorted(false);
        }

        auto trashChannel = TChannel::CreateUniversal();

        FOREACH (const auto& channel, Channels) {
            trashChannel -= channel;
            FOREACH (const auto& column, channel.GetColumns()) {
                if (ColumnIndexes.insert(MakePair(column, columnIndex)).second) {
                    ++columnIndex;
                }
            }
        }

        Channels.push_back(trashChannel);
    }

    // Fill protobuf chunk meta.
    FOREACH (const auto& channel, Channels) {
        *ChannelsExt.add_items()->mutable_channel() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel, ColumnIndexes));
    }

    BasicMetaSize = ChannelsExt.ByteSize() + MiscExt.ByteSize();
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

bool TTableChunkWriter::TryWriteRow(TRow& row, const TNonOwningKey& key)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!State.IsClosed());

    if (!PendingSemaphore.IsReady())
        return false;

    if (!State.IsActive())
        return false;

    i64 rowDataWeight = 1;
    FOREACH (const auto& pair, row) {
        auto it = ColumnIndexes.find(pair.first);
        auto columnIndex = it == ColumnIndexes.end() 
            ? TChannelWriter::UnknownIndex 
            : it->second;

        rowDataWeight += pair.first.size();
        rowDataWeight += pair.second.size();

        MiscExt.set_value_count(MiscExt.value_count() + 1);

        FOREACH (const auto& writer, ChannelWriters) {
            writer->Write(columnIndex, pair.first, pair.second);
        }
    }

    FOREACH (const auto& writer, ChannelWriters) {
        writer->EndRow();
    }

    CurrentSize = SentSize;
    MiscExt.set_row_count(MiscExt.row_count() + 1);

    DataWeight += rowDataWeight;
    if (SamplesSize < Config->SampleRate * DataWeight * CompressionRatio) {
        EmitSample(row);
    }

    if (KeyColumns) {
        LastKey = key;

        if (MiscExt.row_count() == 1) {
            *BoundaryKeysExt.mutable_left() = key.ToProto();
        }

        if (IndexSize < Config->IndexRate * DataWeight * CompressionRatio) {
            EmitIndexEntry();
        }
    }

    for (int channelIndex = 0; channelIndex < static_cast<int>(ChannelWriters.size()); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];
        CurrentSize += channel->GetCurrentSize();
        if (channel->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
            PrepareBlock(channelIndex);
        }
    }

    return true;
}

void TTableChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    PendingSemaphore.Acquire();
    auto channel = ChannelWriters[channelIndex];

    auto* blockInfo = ChannelsExt.mutable_items(channelIndex)->add_blocks();
    blockInfo->set_row_count(channel->GetCurrentRowCount());

    auto block = channel->FlushBlock();
    WriterThread->GetInvoker()->Invoke(BIND(
        &TTableChunkWriter::CompressAndWriteBlock, 
        MakeWeak(this),
        block, 
        blockInfo));
}

TTableChunkWriter::~TTableChunkWriter()
{ }

i64 TTableChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

const TKey<TBlobOutput>& TTableChunkWriter::GetLastKey() const 
{
    return LastKey;
}

const TNullable<TKeyColumns>& TTableChunkWriter::GetKeyColumns() const
{
    return KeyColumns;
}

i64 TTableChunkWriter::GetRowCount() const
{
    return MiscExt.row_count();
}

TAsyncError TTableChunkWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!State.IsClosed());

    State.Close();
    State.StartOperation();

    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];

        if (channel->GetCurrentRowCount()) {
            PrepareBlock(channelIndex);
        }
    }

    PendingSemaphore.GetFreeEvent().Subscribe(BIND(
        &TTableChunkWriter::OnFinalBlocksWritten,
        MakeWeak(this)).Via(WriterThread->GetInvoker()));

    return State.GetOperationError();
}

void TTableChunkWriter::OnFinalBlocksWritten()
{
    CurrentSize = SentSize;

    SetProtoExtension(Meta.mutable_extensions(), SamplesExt);

    if (KeyColumns) {
        *BoundaryKeysExt.mutable_right() = LastKey.ToProto();

        const auto lastIndexRow = --IndexExt.items().end();
        if (MiscExt.row_count() > lastIndexRow->row_index() + 1) {
            auto* item = IndexExt.add_items();
            *item->mutable_key() = LastKey.ToProto();
            item->set_row_index(MiscExt.row_count() - 1);
        }

        SetProtoExtension(Meta.mutable_extensions(), IndexExt);
        SetProtoExtension(Meta.mutable_extensions(), BoundaryKeysExt);
        {
            NProto::TKeyColumnsExt keyColumnsExt;
            ToProto(keyColumnsExt.mutable_values(), KeyColumns.Get());
            SetProtoExtension(Meta.mutable_extensions(), keyColumnsExt);
        }
    }

    FinaliseWriter();
}

void TTableChunkWriter::EmitIndexEntry()
{
    auto* item = IndexExt.add_items();
    *item->mutable_key() = LastKey.ToProto();
    item->set_row_index(MiscExt.row_count() - 1);
    IndexSize += LastKey.GetSize();
}

void TTableChunkWriter::EmitSample(TRow& row)
{
    auto item = SamplesExt.add_items();

    std::sort(row.begin(), row.end());
    
    TLexer lexer;
    FOREACH (const auto& pair, row) {
        auto* part = item->add_parts();
        part->set_column(pair.first.begin(), pair.first.size());
        // sizeof(i32) for type field.
        SamplesSize += sizeof(i32);

        lexer.Reset();
        YCHECK(lexer.Read(pair.second));
        YASSERT(lexer.GetState() == TLexer::EState::Terminal);
        auto& token = lexer.GetToken();
        switch (token.GetType()) {
            case ETokenType::Integer:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateValue(
                    token.GetIntegerValue()).ToProto();
                SamplesSize += sizeof(i64);
                break;

            case ETokenType::String: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(EKeyType::String);
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
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateComposite().ToProto();
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

i64 TTableChunkWriter::GetMetaSize() const
{
    return BasicMetaSize + SamplesSize + IndexSize + (CurrentBlockIndex + 1) * sizeof(NProto::TBlockInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
