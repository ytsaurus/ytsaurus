#include "stdafx.h"
#include "partition_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"
#include "partitioner.h"

#include <core/yson/lexer.h>

#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/encoding_writer.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NVersionedTableClient;
using namespace NChunkClient;
using namespace NYTree;

using NVersionedTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriterFacade::TPartitionChunkWriterFacade(TPartitionChunkWriter* writer)
    : Writer(writer)
{ }

void TPartitionChunkWriterFacade::WriteRow(const TRow& row)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    Writer->WriteRow(row);
}

void TPartitionChunkWriterFacade::WriteRowUnsafe(const TRow& row)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    Writer->WriteRowUnsafe(row);
}

void TPartitionChunkWriterFacade::WriteRowUnsafe(
    const TRow& row,
    const TKey& key)
{
    UNUSED(key);
    WriteRowUnsafe(row);
}

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriter::TPartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IChunkWriterPtr chunkWriter,
    IPartitioner* partitioner)
    : TChunkWriterBase(config, options, chunkWriter)
    , Partitioner(partitioner)
    , Facade(this)
    , BasicMetaSize(0)
    , Pool(TPartitionChunkWriterMemoryPoolTag())
{
    int keyColumnCount = Options->KeyColumns.Get().size();
    PartitionKey = TKey::Allocate(&Pool, keyColumnCount);

    for (int i = 0; i < keyColumnCount; ++i) {
        KeyColumnIndexes[options->KeyColumns.Get()[i]] = i;
    }
    ToProto(ChannelsExt.add_items()->mutable_channel(), TChannel::Universal());

    int upperReserveLimit = TChannelWriter::MaxUpperReserveLimit;
    {
        int averageBufferSize = config->MaxBufferSize / Partitioner->GetPartitionCount() / 2;
        while (upperReserveLimit > averageBufferSize) {
            upperReserveLimit >>= 1;
        }

        YCHECK(upperReserveLimit >= TChannelWriter::MinUpperReserveLimit);
    }

    for (int partitionTag = 0; partitionTag < Partitioner->GetPartitionCount(); ++partitionTag) {
        // Write range column sizes to effectively skip during reading.
        Buffers.push_back(New<TChannelWriter>(partitionTag, 0, true, upperReserveLimit));
        BuffersHeap.push_back(Buffers.back().Get());
        CurrentBufferCapacity += Buffers.back()->GetCapacity();

        auto* partitionAttributes = PartitionsExt.add_partitions();
        partitionAttributes->set_row_count(0);
        partitionAttributes->set_uncompressed_data_size(0);
    }

    YCHECK(Buffers.size() == BuffersHeap.size());

    BasicMetaSize =
        ChannelsExt.ByteSize() +
        sizeof(i64) * Partitioner->GetPartitionCount() +
        sizeof(NChunkClient::NProto::TMiscExt) +
        sizeof(NChunkClient::NProto::TChunkMeta);

    CheckBufferCapacity();
}

TPartitionChunkWriter::~TPartitionChunkWriter()
{ }

TPartitionChunkWriterFacade* TPartitionChunkWriter::GetFacade()
{
    if (State.IsActive() && EncodingWriter->IsReady()) {
        return &Facade;
    }

    return nullptr;
}

void TPartitionChunkWriter::WriteRow(const TRow& row)
{
    // TODO(babenko): check column names
    WriteRowUnsafe(row);
}

void TPartitionChunkWriter::WriteRowUnsafe(const TRow& row)
{
    YASSERT(State.IsActive());

    ResetRowValues(&PartitionKey);

    for (const auto& pair : row) {
        auto it = KeyColumnIndexes.find(pair.first);
        if (it != KeyColumnIndexes.end()) {
            PartitionKey[it->second] = MakeKeyPart(pair.second, Lexer);
        }
    }

    int partitionTag = Partitioner->GetPartitionTag(PartitionKey);
    auto& channelWriter = Buffers[partitionTag];

    i64 rowDataWeight = 1;
    auto capacity = channelWriter->GetCapacity();
    auto channelSize = channelWriter->GetDataSize();

    for (const auto& pair : row) {
        channelWriter->WriteRange(pair.first, pair.second);

        rowDataWeight += pair.first.size();
        rowDataWeight += pair.second.size();
        ValueCount += 1;
    }
    channelWriter->EndRow();

    CurrentBufferCapacity += channelWriter->GetCapacity() - capacity;

    // Update partition counters.
    auto* partitionAttributes = PartitionsExt.mutable_partitions(partitionTag);
    partitionAttributes->set_row_count(partitionAttributes->row_count() + 1);

    // Update global counters.
    DataWeight += rowDataWeight;
    RowCount += 1;

    CurrentUncompressedSize += channelWriter->GetDataSize() - channelSize;
    CurrentSize = static_cast<i64>(EncodingWriter->GetCompressionRatio() * CurrentUncompressedSize);

    AdjustBufferHeap(partitionTag);

    if (channelWriter->GetDataSize() > static_cast<size_t>(Config->BlockSize)) {
        YCHECK(channelWriter->GetHeapIndex() == 0);
        PrepareBlock();
    }

    if (CurrentBufferCapacity > Config->MaxBufferSize) {
        PrepareBlock();
    }
}

void TPartitionChunkWriter::PrepareBlock()
{
    PopBufferHeap();
    auto* channelWriter = BuffersHeap.back();

    auto partitionTag = channelWriter->GetBufferIndex();

    auto* blockInfo = ChannelsExt.mutable_items(0)->add_blocks();
    blockInfo->set_row_count(channelWriter->GetCurrentRowCount());
    blockInfo->set_partition_tag(partitionTag);
    blockInfo->set_block_index(CurrentBlockIndex);

    LOG_DEBUG("Emitting block for partition %v (BlockIndex: %v, RowCount: %v)",
        partitionTag,
        CurrentBlockIndex,
        channelWriter->GetCurrentRowCount());

    ++CurrentBlockIndex;

    i64 size = 0;
    auto blockParts = channelWriter->FlushBlock();
    for (const auto& part : blockParts) {
        size += part.Size();
    }
    blockInfo->set_uncompressed_size(size);

    LargestBlockSize = std::max(LargestBlockSize, size);
    CurrentBufferCapacity += channelWriter->GetCapacity();

    auto* partitionAttributes = PartitionsExt.mutable_partitions(partitionTag);
    partitionAttributes->set_uncompressed_data_size(
        partitionAttributes->uncompressed_data_size() + size);

    EncodingWriter->WriteBlock(std::move(blockParts));
}

i64 TPartitionChunkWriter::GetMetaSize() const
{
    return BasicMetaSize + CurrentBlockIndex * sizeof(NProto::TBlockInfo);
}

NChunkClient::NProto::TChunkMeta TPartitionChunkWriter::GetMasterMeta() const
{
    static const int masterMetaTagsArray[] = { TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value };
    static const yhash_set<int> masterMetaTags(masterMetaTagsArray, masterMetaTagsArray + 1);

    auto meta = Meta;
    FilterProtoExtensions(
        meta.mutable_extensions(),
        Meta.extensions(),
        masterMetaTags);

    return meta;
}

NChunkClient::NProto::TChunkMeta TPartitionChunkWriter::GetSchedulerMeta() const
{
    static const int schedulerMetaTagsArray[] = {
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NProto::TPartitionsExt>::Value
    };
    static const yhash_set<int> schedulerMetaTags(schedulerMetaTagsArray, schedulerMetaTagsArray + 2);

    auto meta = Meta;
    FilterProtoExtensions(
        meta.mutable_extensions(),
        Meta.extensions(),
        schedulerMetaTags);

    return meta;
}

TAsyncError TPartitionChunkWriter::Close()
{
    YASSERT(!State.IsClosed());

    State.StartOperation();

    BIND(&TPartitionChunkWriter::FlushBlocks, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run()
        .Subscribe(
            BIND(&TPartitionChunkWriter::OnFinalBlocksWritten, MakeWeak(this))
            .Via(TDispatcher::Get()->GetWriterInvoker()));

    return State.GetOperationError();
}

void TPartitionChunkWriter::OnFinalBlocksWritten(TError error)
{
    if (!error.IsOK()) {
        State.FinishOperation(error);
        return;
    }

    SetProtoExtension(Meta.mutable_extensions(), PartitionsExt);
    FinalizeWriter();

    CurrentSize = EncodingWriter->GetCompressedSize();
    CurrentUncompressedSize = EncodingWriter->GetUncompressedSize();
}

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriterProvider::TPartitionChunkWriterProvider(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IPartitioner* partitioner)
    : Config(config)
    , Options(options)
    , Partitioner(partitioner)
    , ActiveWriterCount(0)
    , DataStatistics(NChunkClient::NProto::ZeroDataStatistics())
{ }

TPartitionChunkWriterPtr TPartitionChunkWriterProvider::CreateChunkWriter(NChunkClient::IChunkWriterPtr chunkWriter)
{
    YCHECK(ActiveWriterCount == 0);
    if (CurrentWriter) {
        DataStatistics += CurrentWriter->GetDataStatistics();
    }

    ++ActiveWriterCount;
    CurrentWriter = New<TPartitionChunkWriter>(
        Config,
        Options,
        chunkWriter,
        Partitioner);

    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(ActiveWriters.insert(CurrentWriter).second);
    return CurrentWriter;
}

void TPartitionChunkWriterProvider::OnChunkFinished()
{
    YCHECK(ActiveWriterCount == 1);
    --ActiveWriterCount;
    CurrentWriter.Reset();
}

void TPartitionChunkWriterProvider::OnChunkClosed(TPartitionChunkWriterPtr writer)
{
    TGuard<TSpinLock> guard(SpinLock);
    DataStatistics += writer->GetDataStatistics();
    YCHECK(ActiveWriters.erase(writer) == 1);
}


const TNullable<TKeyColumns>& TPartitionChunkWriterProvider::GetKeyColumns() const
{
    return Options->KeyColumns;
}

i64 TPartitionChunkWriterProvider::GetRowCount() const
{
    return GetDataStatistics().row_count();
}

NChunkClient::NProto::TDataStatistics TPartitionChunkWriterProvider::GetDataStatistics() const
{
    TGuard<TSpinLock> guard(SpinLock);

    auto result = DataStatistics;

    for (const auto& writer : ActiveWriters) {
        result += writer->GetDataStatistics();
    }
    return result;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
