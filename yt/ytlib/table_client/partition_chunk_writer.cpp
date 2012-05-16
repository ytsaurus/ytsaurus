#include "stdafx.h"

#include "partition_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

#include <ytlib/ytree/lexer.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool operator < (const TNonOwningKey& key, const TOwningKey& partitionKey)
{
    return CompareKeys(key, partitionKey) < 0;
}

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriter::TPartitionChunkWriter(
    TChunkWriterConfigPtr config,
    NChunkClient::IAsyncWriterPtr chunkWriter,
    const std::vector<TChannel>& channels,
    const TKeyColumns& keyColumns,
    const std::vector<NProto::TKey>& partitionKeys)
    : Config(config)
    , Channel(TChannel::CreateUniversal())
    , ChunkWriter(chunkWriter)
    , IsClosed(false)
    , KeyColumnCount(keyColumns.size())
    , CurrentBlockIndex(0)
    , SentSize(0)
    , CurrentSize(0)
    , UncompressedSize(0)
    , DataSize(0)
    , CompressionRatio(config->EstimatedCompressionRatio)
    , Codec(GetCodec(Config->CodecId))
    , BasicMetaSize(0)
{
    {
        int columnIndex = 0;
        for (; columnIndex < keyColumns.size(); ++columnIndex) {
            Channel.AddColumn(keyColumns[columnIndex]);
            YVERIFY(ColumnIndexes.insert(std::make_pair(
                Channel.GetColumns()[columnIndex], 
                columnIndex)).second);
        }

        // Compose single channel that contains all fixed columns.
        FOREACH(const auto& channel, channels) {
            FOREACH(const auto& column, channel.GetColumns()) {
                bool isKnownColumn = false;
                FOREACH(const auto& existingColumns, Channel.GetColumns()) {
                    if (existingColumns == column) {
                        isKnownColumn = true;
                        break;
                    }
                }

                if (!isKnownColumn) {
                    Channel.AddColumn(column);
                    YVERIFY(ColumnIndexes.insert(std::make_pair(
                        Channel.GetColumns()[columnIndex], 
                        columnIndex)).second);
                    ++columnIndex;
                }
            }
        }
    }

    *ChannelsExt.add_items()->mutable_channel() = Channel.ToProto();

    PartitionKeys.reserve(partitionKeys.size());
    FOREACH(const auto& key, partitionKeys) {
        PartitionKeys.push_back(TOwningKey());
        PartitionKeys.back().FromProto(key);
    }

    for (int i = 0; i <= PartitionKeys.size(); ++i) {
        ChannelWriters.push_back(New<TChannelWriter>(Channel, ColumnIndexes));
        PartitionsExt.add_sizes(0);
    }

    BasicMetaSize = ChannelsExt.ByteSize() + sizeof(i64) * PartitionKeys.size() + 
        sizeof(NChunkHolder::NProto::TMiscExt) + 
        sizeof(NChunkHolder::NProto::TChunkMeta);
}

TPartitionChunkWriter::~TPartitionChunkWriter()
{ }

TAsyncError TPartitionChunkWriter::AsyncWriteRow(const TRow& row)
{
    YASSERT(!IsClosed);

    TNonOwningKey key(KeyColumnCount);
    {
        TLexer lexer;
        FOREACH(const auto& pair, row) {
            auto it = ColumnIndexes.find(pair.first);
            if (it != ColumnIndexes.end() && it->second < KeyColumnCount) {
                key.SetKeyPart(it->second, pair.second, lexer);
            }
        }
    }

    auto partitionIt = std::lower_bound(PartitionKeys.begin(), PartitionKeys.end(), key);
    auto partitionTag = std::distance(PartitionKeys.begin(), partitionIt);
    auto& channelWriter = ChannelWriters[partitionTag];

    i64 rowDataSize = 1;
    FOREACH(const auto& pair, row) {
        auto it = ColumnIndexes.find(pair.first);
        channelWriter->Write(
            it == ColumnIndexes.end() ? TChannelWriter::UnknownIndex : it->second,
            pair.first,
            pair.second);

        rowDataSize += pair.first.size();
        rowDataSize += pair.second.size();
    }

    PartitionsExt.set_sizes(partitionTag, PartitionsExt.sizes(partitionTag) + rowDataSize);
    MiscExt.set_row_count(MiscExt.row_count() + 1);

    DataSize += rowDataSize;

    std::vector<TSharedRef> blocks;
    if (channelWriter->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
        blocks.push_back(PrepareBlock(partitionTag));
    }

    CurrentSize = SentSize + channelWriter->GetCurrentSize();
    return ChunkWriter->AsyncWriteBlocks(blocks);
}

TSharedRef TPartitionChunkWriter::PrepareBlock(int partitionTag)
{
    auto& channelWriter = ChannelWriters[partitionTag];
    auto* blockInfo = ChannelsExt.mutable_items(0)->add_blocks();
    blockInfo->set_block_index(CurrentBlockIndex);
    blockInfo->set_row_count(channelWriter->GetCurrentRowCount());
    blockInfo->set_partition_tag(partitionTag);

    auto block = channelWriter->FlushBlock();
    UncompressedSize += block.Size();

    auto data = Codec->Compress(block);

    SentSize += data.Size();
    CompressionRatio = SentSize / double(DataSize);
    ++CurrentBlockIndex;
    return data;
}

i64 TPartitionChunkWriter::GetMetaSize() const
{
    return BasicMetaSize + CurrentBlockIndex * sizeof(NProto::TBlockInfo);
}

i64 TPartitionChunkWriter::GetCurrentSize() const
{
    return CurrentSize;
}

NChunkHolder::NProto::TChunkMeta TPartitionChunkWriter::GetMasterMeta() const
{
    NChunkHolder::NProto::TChunkMeta meta;
    meta.set_type(EChunkType::Table);
    SetProtoExtension(meta.mutable_extensions(), MiscExt);
    SetProtoExtension(meta.mutable_extensions(), PartitionsExt);

    return meta;
}

TAsyncError TPartitionChunkWriter::AsyncClose()
{
    std::vector<TSharedRef> finalBlocks;
    for (int partitionTag = 0; partitionTag <= PartitionKeys.size(); ++partitionTag) {
        auto& channelWriter = ChannelWriters[partitionTag];
        if (channelWriter->GetCurrentRowCount()) {
            auto block = PrepareBlock(partitionTag);
            finalBlocks.push_back(block);
        }
    }

    SetProtoExtension(Meta.mutable_extensions(), PartitionsExt);
    SetProtoExtension(Meta.mutable_extensions(), ChannelsExt);
    Meta.set_type(EChunkType::Table);
    {
        MiscExt.set_uncompressed_data_size(UncompressedSize);
        MiscExt.set_compressed_data_size(SentSize);
        MiscExt.set_meta_size(Meta.ByteSize());
        SetProtoExtension(Meta.mutable_extensions(), MiscExt);
    }

    return ChunkWriter
        ->AsyncWriteBlocks(finalBlocks)
        .Apply(BIND(&TPartitionChunkWriter::OnFinalBlocksWritten, MakeStrong(this)));
}

TAsyncError TPartitionChunkWriter::OnFinalBlocksWritten(TError error)
{
    if (!error.IsOK()) {
        return MakeFuture(error);
    }

    return ChunkWriter->AsyncClose(Meta);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
