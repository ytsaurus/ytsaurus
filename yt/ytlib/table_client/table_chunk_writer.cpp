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
    : Config(config)
    , Channels(channels)
    , ChunkWriter(chunkWriter)
    , KeyColumns(keyColumns)
    , IsOpen(false)
    , IsClosed(false)
    , CurrentBlockIndex(0)
    , CurrentSize(0)
    , SentSize(0)
    , UncompressedSize(0)
    , DataWeight(0)
    , SamplesSize(0)
    , IndexSize(0)
    , CompressionRatio(config->EstimatedCompressionRatio)
    , BasicMetaSize(0)
{
    YASSERT(config);
    YASSERT(chunkWriter);

    Codec = GetCodec(ECodecId(Config->CodecId));
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
    YASSERT(!IsClosed);

    IsOpen = true;
    return MakeFuture(TError());
}

TAsyncError TTableChunkWriter::AsyncWriteRow(TRow& row, const TNonOwningKey& key)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

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

    std::vector<TSharedRef> completedBlocks;
    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];
        CurrentSize += channel->GetCurrentSize();

        if (channel->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
            auto block = PrepareBlock(channelIndex);
            completedBlocks.push_back(block);
        } 
    }

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

    return ChunkWriter->AsyncWriteBlocks(completedBlocks);
}

TSharedRef TTableChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = ChannelWriters[channelIndex];

    auto* blockInfo = ChannelsExt.mutable_items(channelIndex)->add_blocks();
    blockInfo->set_block_index(CurrentBlockIndex);
    blockInfo->set_row_count(channel->GetCurrentRowCount());

    auto block = channel->FlushBlock();
    UncompressedSize += block.Size();

    auto data = Codec->Compress(block);

    SentSize += data.Size();

    CompressionRatio = SentSize / double(DataWeight + 1);

    ++CurrentBlockIndex;

    return data;
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
    YASSERT(!IsClosed);

    IsClosed = true;

    std::vector<TSharedRef> finalBlocks;
    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];

        if (channel->GetCurrentRowCount()) {
            auto block = PrepareBlock(channelIndex);
            finalBlocks.push_back(block);
        }
    }

    CurrentSize = SentSize;

    SetProtoExtension(Meta.mutable_extensions(), SamplesExt);
    SetProtoExtension(Meta.mutable_extensions(), ChannelsExt);

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

    Meta.set_type(EChunkType::Table);
    {
        MiscExt.set_uncompressed_data_size(UncompressedSize);
        MiscExt.set_compressed_data_size(SentSize);
        MiscExt.set_data_weight(DataWeight);
        MiscExt.set_meta_size(Meta.ByteSize());
        SetProtoExtension(Meta.mutable_extensions(), MiscExt);
    }

    return ChunkWriter
        ->AsyncWriteBlocks(finalBlocks)
        .Apply(BIND(&TTableChunkWriter::OnFinalBlocksWritten, MakeStrong(this)));
}

TAsyncError TTableChunkWriter::OnFinalBlocksWritten(TError error)
{
    if (!error.IsOK()) {
        return MakeFuture(error);
    }

    return ChunkWriter->AsyncClose(Meta);
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
        YVERIFY(lexer.Read(pair.second));
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
    YASSERT(IsClosed);

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
