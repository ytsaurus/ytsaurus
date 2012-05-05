#include "stdafx.h"
#include "chunk_writer.h"

#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

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

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriter::TChunkWriter(
    const TChunkWriterConfigPtr& config,
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
    , LastKey()
    , SamplesSize(0)
    , IndexSize(0)
    , DataOffset(0)
{
    YASSERT(chunkWriter);
    Codec = GetCodec(ECodecId(Config->CodecId));
    ProtoMisc.set_codec_id(Config->CodecId);

    {
        int columnIndex = 0;

        if (KeyColumns) {
            ProtoMisc.set_sorted(true);

            FOREACH(auto& column, KeyColumns.Get()) {
                auto res = ColumnIndexes.insert(MakePair(column, columnIndex));
                if (res.second)
                    ++columnIndex;
            }
        } else {
            ProtoMisc.set_sorted(false);
        }

        auto trashChannel = TChannel::CreateUniversal();

        FOREACH(auto& channel, Channels) {
            trashChannel -= channel;
            FOREACH(auto& column, channel.GetColumns()) {
                auto res = ColumnIndexes.insert(MakePair(column, columnIndex));
                if (res.second) {
                    ++columnIndex;
                }
            }
        }

        Channels.push_back(trashChannel);
    }

    // Fill protobuf chunk meta.
    FOREACH(auto channel, Channels) {
        *ProtoChannels.add_items()->mutable_channel() = channel.ToProto();
        ChannelWriters.push_back(New<TChannelWriter>(channel, ColumnIndexes));
    }
}

TAsyncError TChunkWriter::AsyncOpen()
{
    // No thread affinity check here - 
    // TChunkSequenceWriter may call it from different threads.
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    IsOpen = true;
    return MakeFuture(TError());
}

TAsyncError TChunkWriter::AsyncWriteRow(TRow& row, TKey& key)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    FOREACH(const auto& pair, row) {
        auto it = ColumnIndexes.find(pair.first);
        auto columnIndex = it == ColumnIndexes.end() 
            ? TChannelWriter::UnknownIndex 
            : it->second;

        FOREACH(auto& channelWriter, ChannelWriters) {
            channelWriter->Write(columnIndex, pair.first, pair.second);
        }
    }

    FOREACH(auto& channelWriter, ChannelWriters) {
        channelWriter->EndRow();
    }

    CurrentSize = SentSize;
    ProtoMisc.set_row_count(ProtoMisc.row_count() + 1);

    std::vector<TSharedRef> completedBlocks;
    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];
        CurrentSize += channel->GetCurrentSize();

        if (channel->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
            auto block = PrepareBlock(channelIndex);
            completedBlocks.push_back(block);
        } 
    }

    LastKey.Swap(key);

    if (SamplesSize < Config->SampleRate * CurrentSize) {
        *ProtoSamples.add_items() = MakeSample(row);
    }

    if (KeyColumns) {
        if (ProtoMisc.row_count() == 1) {
            *ProtoBoundaryKeys.mutable_left() = key.ToProto();
        }

        if (IndexSize < Config->IndexRate * CurrentSize) {
            auto* indexRow = ProtoIndex.add_index_rows();
            *indexRow->mutable_key() = key.ToProto();
            indexRow->set_row_index(ProtoMisc.row_count() - 1);
            IndexSize += key.GetSize();
        }
    }

    return ChunkWriter->AsyncWriteBlocks(completedBlocks);
}

TSharedRef TChunkWriter::PrepareBlock(int channelIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = ChannelWriters[channelIndex];

    auto* blockInfo = ProtoChannels.mutable_items(channelIndex)->add_blocks();
    blockInfo->set_block_index(CurrentBlockIndex);
    blockInfo->set_row_count(channel->GetCurrentRowCount());

    auto block = channel->FlushBlock();
    UncompressedSize += block.Size();

    auto data = Codec->Compress(block);

    SentSize += data.Size();
    ++CurrentBlockIndex;

    return data;
}

TChunkWriter::~TChunkWriter()
{ }

i64 TChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

TKey& TChunkWriter::GetLastKey()
{
    return LastKey;
}

const TNullable<TKeyColumns>& TChunkWriter::GetKeyColumns() const
{
    return KeyColumns;
}

i64 TChunkWriter::GetRowCount() const
{
    return ProtoMisc.row_count();
}

TAsyncError TChunkWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    IsClosed = true;

    std::vector<TSharedRef> completedBlocks;
    for (int channelIndex = 0; channelIndex < ChannelWriters.size(); ++channelIndex) {
        auto& channel = ChannelWriters[channelIndex];

        if (channel->GetCurrentRowCount()) {
            auto block = PrepareBlock(channelIndex);
            completedBlocks.push_back(block);
        }
    }

    CurrentSize = SentSize;

    NChunkHolder::NProto::TChunkMeta chunkMeta;
    chunkMeta.set_type(EChunkType::Table);

    ProtoMisc.set_uncompressed_size(UncompressedSize);

    SetProtoExtension(chunkMeta.mutable_extensions(), ProtoMisc);
    SetProtoExtension(chunkMeta.mutable_extensions(), ProtoSamples);
    SetProtoExtension(chunkMeta.mutable_extensions(), ProtoChannels);

    if (KeyColumns) {
        *ProtoBoundaryKeys.mutable_right() = LastKey.ToProto();

        const auto lastIndexRow = --ProtoIndex.index_rows().end();
        if (ProtoMisc.row_count() > lastIndexRow->row_index() + 1) {
            auto* indexRow = ProtoIndex.add_index_rows();
            *indexRow->mutable_key() = LastKey.ToProto();
            indexRow->set_row_index(ProtoMisc.row_count() - 1);
        }

        SetProtoExtension(chunkMeta.mutable_extensions(), ProtoIndex);
        SetProtoExtension(chunkMeta.mutable_extensions(), ProtoBoundaryKeys);

        NProto::TKeyColumns protoKeyColumns;
        ToProto(protoKeyColumns.mutable_values(), KeyColumns.Get());
        SetProtoExtension(chunkMeta.mutable_extensions(), protoKeyColumns);
    }

    return ChunkWriter->AsyncClose(MoveRV(completedBlocks), chunkMeta);
}

NProto::TSample TChunkWriter::MakeSample(TRow& row)
{
    std::sort(row.begin(), row.end());

    TLexer lexer;

    NProto::TSample sample;
    FOREACH(const auto& pair, row) {
        auto* part = sample.add_parts();
        part->set_column(pair.first.begin(), pair.first.size());

        lexer.Reset();
        YVERIFY(lexer.Read(pair.second));
        YASSERT(lexer.GetState() == TLexer::EState::Terminal);
        auto& token = lexer.GetToken();
        switch (token.GetType()) {
        case ETokenType::Integer:
            *(part->mutable_key_part()) = TKeyPart(token.GetIntegerValue()).ToProto();
            // sizeof(int) for type field.
            SamplesSize += sizeof(i64) + sizeof(int);
            break;

        case ETokenType::String: {
            auto *keyPart = part->mutable_key_part();
            keyPart->set_type(EKeyType::String);
            auto length = std::min(
                token.GetStringValue().size(), 
                static_cast<size_t>(Config->MaxSampleSize));
            keyPart->set_str_value(token.GetStringValue().begin(), length);
            // sizeof(int) for type field.
            SamplesSize += length + sizeof(int);
            break;
        }

        case ETokenType::Double:
            *(part->mutable_key_part()) = TKeyPart(token.GetDoubleValue()).ToProto();
            SamplesSize += sizeof(double) + sizeof(int);
            break;

        default:
            *(part->mutable_key_part()) = TKeyPart::CreateComposite().ToProto();
            // sizeof(int) for type field.
            SamplesSize += sizeof(int);
            break;

        }
    }

    sample.set_row_index(ProtoMisc.row_count() - 1);
    sample.set_data_offset();

    return sample;
}

NChunkHolder::NProto::TChunkMeta TChunkWriter::GetMasterMeta() const
{
    YASSERT(IsClosed);
    NChunkHolder::NProto::TChunkMeta meta;
    meta.set_type(EChunkType::Table);
    SetProtoExtension(meta.mutable_extensions(), ProtoMisc);
    if (KeyColumns) {
        SetProtoExtension(meta.mutable_extensions(), ProtoBoundaryKeys);
    }

    return meta;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
