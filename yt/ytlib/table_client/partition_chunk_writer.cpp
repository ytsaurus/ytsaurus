#include "stdafx.h"

#include "partition_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

#include <ytlib/ytree/lexer.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NYTree;

static NLog::TLogger& Logger = TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

bool operator < (const TOwningKey& partitionKey, const TNonOwningKey& key)
{
    return CompareKeys(partitionKey, key) < 0;
}

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriter::TPartitionChunkWriter(
    TChunkWriterConfigPtr config,
    NChunkClient::IAsyncWriterPtr chunkWriter,
    const std::vector<TChannel>& channels,
    const TKeyColumns& keyColumns,
    const std::vector<NProto::TKey>& partitionKeys)
    : TChunkWriterBase(chunkWriter, config)
    , Channel(TChannel::CreateUniversal())
    , KeyColumnCount(keyColumns.size())
    , CurrentSize(0)
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
        FOREACH (const auto& channel, channels) {
            FOREACH (const auto& column, channel.GetColumns()) {
                bool isKnownColumn = false;
                FOREACH (const auto& existingColumns, Channel.GetColumns()) {
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
    FOREACH (const auto& key, partitionKeys) {
        PartitionKeys.push_back(TOwningKey());
        PartitionKeys.back().FromProto(key);
    }

    for (int partitionTag = 0; partitionTag <= PartitionKeys.size(); ++partitionTag) {
        ChannelWriters.push_back(New<TChannelWriter>(Channel, ColumnIndexes));
        auto* partitionAttributes = PartitionsExt.add_partitions();
        partitionAttributes->set_data_weight(0);
        partitionAttributes->set_row_count(0);
    }

    MiscExt.set_value_count(0);

    BasicMetaSize = ChannelsExt.ByteSize() + sizeof(i64) * PartitionKeys.size() + 
        sizeof(NChunkHolder::NProto::TMiscExt) + 
        sizeof(NChunkHolder::NProto::TChunkMeta);
}

TPartitionChunkWriter::~TPartitionChunkWriter()
{ }

bool TPartitionChunkWriter::TryWriteRow(const TRow& row)
{
    YASSERT(!State.IsClosed());

    if (!PendingSemaphore.IsReady())
        return false;

    if (!State.IsActive())
        return false;

    TNonOwningKey key(KeyColumnCount);
    {
        TLexer lexer;
        FOREACH (const auto& pair, row) {
            auto it = ColumnIndexes.find(pair.first);
            if (it != ColumnIndexes.end() && it->second < KeyColumnCount) {
                key.SetKeyPart(it->second, pair.second, lexer);
            }
        }
    }

    auto partitionIt = std::lower_bound(PartitionKeys.begin(), PartitionKeys.end(), key);
    auto partitionTag = std::distance(PartitionKeys.begin(), partitionIt);
    auto& channelWriter = ChannelWriters[partitionTag];

    i64 rowDataWeight = 1;
    FOREACH (const auto& pair, row) {
        auto it = ColumnIndexes.find(pair.first);
        channelWriter->Write(
            it == ColumnIndexes.end() ? TChannelWriter::UnknownIndex : it->second,
            pair.first,
            pair.second);

        rowDataWeight += pair.first.size();
        rowDataWeight += pair.second.size();

        MiscExt.set_value_count(MiscExt.value_count() + 1);
    }
    channelWriter->EndRow();

    // Update partition counters.
    auto* partitionAttributes = PartitionsExt.mutable_partitions(partitionTag);
    partitionAttributes->set_data_weight(partitionAttributes->data_weight() + rowDataWeight);
    partitionAttributes->set_row_count(partitionAttributes->row_count() + 1);

    // Update global counters.
    DataWeight += rowDataWeight;
    MiscExt.set_row_count(MiscExt.row_count() + 1);

    if (channelWriter->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
        PrepareBlock(partitionTag);
    }

    CurrentSize = SentSize + channelWriter->GetCurrentSize();
    return true;
}

void TPartitionChunkWriter::PrepareBlock(int partitionTag)
{
    PendingSemaphore.Acquire();

    auto& channelWriter = ChannelWriters[partitionTag];
    auto* blockInfo = ChannelsExt.mutable_items(0)->add_blocks();
    blockInfo->set_row_count(channelWriter->GetCurrentRowCount());
    blockInfo->set_partition_tag(partitionTag);

    auto block = channelWriter->FlushBlock();
    WriterThread->GetInvoker()->Invoke(BIND(
        &TPartitionChunkWriter::CompressAndWriteBlock, 
        MakeWeak(this),
        block, 
        blockInfo));
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
    YASSERT(!State.IsClosed());

    State.StartOperation();

    for (int partitionTag = 0; partitionTag <= PartitionKeys.size(); ++partitionTag) {
        auto& channelWriter = ChannelWriters[partitionTag];
        if (channelWriter->GetCurrentRowCount() > 0) {
            PrepareBlock(partitionTag);
        }
    }

    PendingSemaphore.GetFreeEvent().Subscribe(BIND(
        &TPartitionChunkWriter::OnFinalBlocksWritten,
        MakeWeak(this)).Via(WriterThread->GetInvoker()));

    return State.GetOperationError();
}

void TPartitionChunkWriter::OnFinalBlocksWritten()
{
    SetProtoExtension(Meta.mutable_extensions(), PartitionsExt);
    FinaliseWriter();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
