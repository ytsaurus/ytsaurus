#include "stdafx.h"
#include "private.h"
#include "partition_chunk_reader.h"

#include <core/misc/varint.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkReaderFacade::TPartitionChunkReaderFacade(TPartitionChunkReader* reader)
    : Reader(reader)
{ }

const char* TPartitionChunkReaderFacade::GetRowPointer() const
{
    return Reader->GetRowPointer();
}

TValue TPartitionChunkReaderFacade::ReadValue(const TStringBuf& name) const
{
    return Reader->ReadValue(name);
}

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): eliminate copy-paste from table_chunk_reader.cpp
TPartitionChunkReader::TPartitionChunkReader(
    TPartitionChunkReaderProviderPtr provider,
    TSequentialReaderConfigPtr sequentialReader,
    NChunkClient::IChunkReaderPtr chunkReader,
    IBlockCachePtr uncompressedBlockCache,
    int partitionTag,
    NCompression::ECodec codecId)
    : RowPointer_(nullptr)
    , RowIndex_(-1)
    , Provider(provider)
    , Facade(this)
    , SequentialConfig(std::move(sequentialReader))
    , ChunkReader(std::move(chunkReader))
    , UncompressedBlockCache(std::move(uncompressedBlockCache))
    , PartitionTag(partitionTag)
    , CodecId(codecId)
    , Logger(TableClientLogger)
{ }

TAsyncError TPartitionChunkReader::AsyncOpen()
{
    State.StartOperation();

    Logger.AddTag("ChunkId: %v", ChunkReader->GetChunkId());

    std::vector<int> extensionTags;
    extensionTags.push_back(TProtoExtensionTag<NProto::TChannelsExt>::Value);

    LOG_INFO("Requesting chunk meta");
    ChunkReader->GetMeta(PartitionTag, &extensionTags)
        .Subscribe(BIND(&TPartitionChunkReader::OnGotMeta, MakeWeak(this))
            .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));

    return State.GetOperationError();
}

void TPartitionChunkReader::OnGotMeta(IChunkReader::TGetMetaResult result)
{
    if (!result.IsOK()) {
        OnFail(result);
        return;
    }

    LOG_INFO("Chunk meta received");

    const auto& chunkMeta = result.Value();

    auto type = EChunkType(chunkMeta.type());
    if (type != EChunkType::Table) {
        LOG_FATAL("Invalid chunk type %v", type);
    }

    if (chunkMeta.version() != FormatVersion) {
        OnFail(TError("Invalid chunk format version: expected %v, actual %v",
            FormatVersion,
            chunkMeta.version()));
        return;
    }

    auto channelsExt = GetProtoExtension<NProto::TChannelsExt>(chunkMeta.extensions());
    YCHECK(channelsExt.items_size() == 1);

    std::vector<TSequentialReader::TBlockInfo> blockSequence;
    {
        for (int i = 0; i < channelsExt.items(0).blocks_size(); ++i) {
            const auto& blockInfo = channelsExt.items(0).blocks(i);
            YCHECK(PartitionTag == blockInfo.partition_tag());
            blockSequence.push_back(TSequentialReader::TBlockInfo(
                blockInfo.block_index(),
                blockInfo.uncompressed_size()));
        }
    }

    SequentialReader = New<TSequentialReader>(
        SequentialConfig,
        std::move(blockSequence),
        ChunkReader,
        UncompressedBlockCache,
        CodecId);

    LOG_INFO("Reading %v blocks for partition %v",
        blockSequence.size(),
        PartitionTag);

    Blocks.reserve(blockSequence.size());

    if (SequentialReader->HasMoreBlocks()) {
        SequentialReader->FetchNextBlock().Subscribe(BIND(
            &TPartitionChunkReader::OnNextBlock,
            MakeWeak(this)));
    } else {
        State.FinishOperation();
    }
}

void TPartitionChunkReader::OnNextBlock(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Switching to next block at row %v", RowIndex_);

    Blocks.push_back(SequentialReader->GetCurrentBlock());
    YCHECK(Blocks.back().Size() > 0);

    TMemoryInput input(Blocks.back().Begin(), Blocks.back().Size());

    ui64 dataSize;
    ReadVarUint64(&input, &dataSize);
    YCHECK(dataSize > 0);

    RowPointer_ = input.Buf();
    SizeToNextRow = 0;

    const char* dataEnd = RowPointer_ + dataSize;
    SizeBuffer.Reset(dataEnd, Blocks.back().End() - dataEnd);

    YCHECK(NextRow());
    State.FinishOperation();
}

bool TPartitionChunkReader::NextRow()
{
    if (SizeBuffer.Avail() > 0) {
        RowPointer_ = RowPointer_ + SizeToNextRow;
        ReadVarUint64(&SizeBuffer, &SizeToNextRow);

        DataBuffer.Reset(RowPointer_, SizeToNextRow);

        CurrentRow.clear();
        while (true) {
            auto value = TValue::Load(&DataBuffer);
            if (value.IsNull()) {
                break;
            }

            i32 columnNameLength;
            ReadVarInt32(&DataBuffer, &columnNameLength);
            YASSERT(columnNameLength > 0);
            CurrentRow.insert(std::make_pair(TStringBuf(DataBuffer.Buf(), columnNameLength), value));
            DataBuffer.Skip(columnNameLength);
        }

        ++RowIndex_;
        ++Provider->RowIndex_;
        return true;
    } else {
        RowPointer_ = NULL;
        return false;
    }
}

auto TPartitionChunkReader::GetFacade() const -> const TFacade*
{
    return RowPointer_ ?  &Facade : nullptr;
}

bool TPartitionChunkReader::FetchNext()
{
    if (!NextRow() && SequentialReader->HasMoreBlocks()) {
        State.StartOperation();
        SequentialReader->FetchNextBlock().Subscribe(BIND(
            &TPartitionChunkReader::OnNextBlock,
            MakeWeak(this)));
        return false;
    }

    return true;
}

TAsyncError TPartitionChunkReader::GetReadyEvent()
{
    return State.GetOperationError();
}

TValue TPartitionChunkReader::ReadValue(const TStringBuf& name) const
{
    YASSERT(RowPointer_);

    auto it = CurrentRow.find(name);
    if (it == CurrentRow.end()) {
        // Null value.
        return TValue();
    } else {
        return it->second;
    }
}

TFuture<void> TPartitionChunkReader::GetFetchingCompleteEvent()
{
    return SequentialReader->GetFetchingCompleteEvent();
}

NChunkClient::NProto::TDataStatistics TPartitionChunkReader::GetDataStatistics() const
{
    NChunkClient::NProto::TDataStatistics result;
    result.set_chunk_count(1);

    if (SequentialReader) {
        result.set_row_count(GetRowIndex() + 1);
        result.set_uncompressed_data_size(SequentialReader->GetUncompressedDataSize());
        result.set_compressed_data_size(SequentialReader->GetCompressedDataSize());
    } else {
        result.set_row_count(0);
        result.set_uncompressed_data_size(0);
        result.set_compressed_data_size(0);
    }

    return result;
}

void TPartitionChunkReader::OnFail(const TError& error)
{
    LOG_WARNING(error);
    State.Fail(error);
}

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkReaderProvider::TPartitionChunkReaderProvider(
    TSequentialReaderConfigPtr config,
    IBlockCachePtr uncompressedBlockCache)
    : RowIndex_(-1)
    , Config(std::move(config))
    , UncompressedBlockCache(std::move(uncompressedBlockCache))
    , DataStatistics(NChunkClient::NProto::ZeroDataStatistics())
{ }

TPartitionChunkReaderPtr TPartitionChunkReaderProvider::CreateReader(
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    const NChunkClient::IChunkReaderPtr chunkReader)
{
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());

    return New<TPartitionChunkReader>(
        this,
        Config,
        std::move(chunkReader),
        UncompressedBlockCache,
        chunkSpec.partition_tag(),
        NCompression::ECodec(miscExt.compression_codec()));
}

bool TPartitionChunkReaderProvider::KeepInMemory() const
{
    return true;
}

void TPartitionChunkReaderProvider::OnReaderOpened(
    TPartitionChunkReaderPtr reader,
    NChunkClient::NProto::TChunkSpec& chunkSpec)
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(ActiveReaders.insert(reader).second);
}

void TPartitionChunkReaderProvider::OnReaderFinished(TPartitionChunkReaderPtr reader)
{
    TGuard<TSpinLock> guard(SpinLock);
    DataStatistics += reader->GetDataStatistics();
    YCHECK(ActiveReaders.erase(reader) == 1);
}

NChunkClient::NProto::TDataStatistics TPartitionChunkReaderProvider::GetDataStatistics() const
{
    TGuard<TSpinLock> guard(SpinLock);

    auto dataStatistics = DataStatistics;
    for (const auto& reader : ActiveReaders) {
        dataStatistics += reader->GetDataStatistics();
    }
    return dataStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
