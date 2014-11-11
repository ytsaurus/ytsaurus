#include "stdafx.h"
#include "file_chunk_reader.h"
#include "private.h"
#include "config.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/replication_reader.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>

namespace NYT {
namespace NFileClient {

using namespace NChunkClient;

static const auto& Logger = FileClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFileChunkReader::TFileChunkReader(
    TSequentialReaderConfigPtr sequentialConfig,
    NChunkClient::IChunkReaderPtr chunkReader,
    IBlockCachePtr uncompressedBlockCache,
    NCompression::ECodec codecId,
    i64 startOffset,
    i64 endOffset)
    : SequentialConfig(std::move(sequentialConfig))
    , ChunkReader(std::move(chunkReader))
    , UncompressedBlockCache(std::move(uncompressedBlockCache))
    , CodecId(codecId)
    , StartOffset(startOffset)
    , EndOffset(endOffset)
    , Facade(this)
    , Logger(FileClientLogger)
{ }

TAsyncError TFileChunkReader::AsyncOpen()
{
    State.StartOperation();

    Logger.AddTag("ChunkId: %v", ChunkReader->GetChunkId());

    LOG_INFO("Requesting chunk meta");
    ChunkReader->GetMeta().Subscribe(
        BIND(&TFileChunkReader::OnGotMeta, MakeWeak(this))
            .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));

    return State.GetOperationError();
}

void TFileChunkReader::OnGotMeta(NChunkClient::IChunkReader::TGetMetaResult result)
{
    if (!result.IsOK()) {
        auto error = TError("Failed to get file chunk meta") << result;
        LOG_WARNING(error);
        State.Fail(error);
        return;
    }

    LOG_INFO("Chunk meta received");

    const auto& chunkMeta = result.Value();

    if (chunkMeta.type() != EChunkType::File) {
        auto error = TError("Invalid chunk type: expected %Qlv, actual %Qlv",
            EChunkType(EChunkType::File),
            EChunkType(chunkMeta.type()));
        LOG_WARNING(error);
        State.Fail(error);
        return;
    }

    if (chunkMeta.version() != FormatVersion) {
        auto error = TError("Invalid file chunk format version: expected %v, actual %v",
            FormatVersion,
            chunkMeta.version());
        LOG_WARNING(error);
        State.Fail(error);
        return;
    }

    std::vector<TSequentialReader::TBlockInfo> blockSequence;

    // COMPAT(psushin): new file chunk meta!
    auto fileBlocksExt = FindProtoExtension<NFileClient::NProto::TBlocksExt>(chunkMeta.extensions());

    i64 selectedSize = 0;
    int blockIndex = 0;
    auto addBlock = [&] (int index, i64 size) -> bool {
        if (StartOffset >= size) {
            StartOffset -= size;
            EndOffset -= size;
            ++blockIndex;
            return true;
        } else if (selectedSize < EndOffset) {
            selectedSize += size;
            blockSequence.push_back(TSequentialReader::TBlockInfo(index, size));
            return true;
        }
        return false;
    };

    int blockCount = 0;
    if (fileBlocksExt) {
        // New chunk.
        blockCount = fileBlocksExt->blocks_size();
        blockSequence.reserve(blockCount);

        for (int index = 0; index < blockCount; ++index) {
            if (!addBlock(index, fileBlocksExt->blocks(index).size())) {
                break;
            }
        }
    } else {
        // Old chunk.
        auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta.extensions());
        blockCount = blocksExt.blocks_size();

        blockSequence.reserve(blockCount);
        for (int index = 0; index < blockCount; ++index) {
            if (!addBlock(index, blocksExt.blocks(index).size())) {
                break;
            }
        }
    }
    YCHECK(blockCount >= 0);

    LOG_INFO("Reading %v blocks out of %v starting from %v (SelectedSize: %v)",
        blockSequence.size(),
        blockCount,
        blockIndex,
        selectedSize);

    SequentialReader = New<TSequentialReader>(
        SequentialConfig,
        std::move(blockSequence),
        ChunkReader,
        UncompressedBlockCache,
        CodecId);

    LOG_INFO("File reader opened");

    if (SequentialReader->HasMoreBlocks()) {
        SequentialReader->FetchNextBlock().Subscribe(BIND(
            &TFileChunkReader::OnNextBlock,
            MakeWeak(this)));
    } else {
        State.Close();
    }

}

void TFileChunkReader::OnNextBlock(TError error)
{
    if (!error.IsOK()) {
        auto wrappedError = TError("Failed to fetch file block") << error;
        LOG_WARNING(wrappedError);
        State.Fail(wrappedError);
        return;
    }

    State.FinishOperation();
}

bool TFileChunkReader::FetchNext()
{
    YCHECK(!State.HasRunningOperation());
    auto block = SequentialReader->GetCurrentBlock();
    StartOffset = std::max(StartOffset - static_cast<i64>(block.Size()), (i64)0);
    EndOffset = std::max(EndOffset - static_cast<i64>(block.Size()), (i64)0);

    if (SequentialReader->HasMoreBlocks()) {
        State.StartOperation();
        SequentialReader->FetchNextBlock().Subscribe(BIND(
            &TFileChunkReader::OnNextBlock,
            MakeWeak(this)));
        return false;
    } else {
        State.Close();
        return true;
    }
}

TAsyncError TFileChunkReader::GetReadyEvent()
{
    return State.GetOperationError();
}

auto TFileChunkReader::GetFacade() const -> const TFacade*
{
    YCHECK(!State.HasRunningOperation());
    return State.IsClosed() ? nullptr : &Facade;
}

TSharedRef TFileChunkReader::GetBlock() const
{
    auto block = SequentialReader->GetCurrentBlock();

    auto* begin = block.Begin();
    auto* end = block.End();

    YCHECK(EndOffset > 0);

    if (EndOffset < block.Size()) {
        end = block.Begin() + EndOffset;
    }

    if (StartOffset > 0) {
        begin = block.Begin() + StartOffset;
    }

    return block.Slice(TRef(begin, end));
}

TFuture<void> TFileChunkReader::GetFetchingCompleteEvent()
{
    return SequentialReader->GetFetchingCompleteEvent();
}

////////////////////////////////////////////////////////////////////////////////

TFileChunkReaderProvider::TFileChunkReaderProvider(
    NChunkClient::TSequentialReaderConfigPtr config,
    IBlockCachePtr uncompressedBlockCache)
    : Config(std::move(config))
    , UncompressedBlockCache(std::move(uncompressedBlockCache))
{ }

TFileChunkReaderPtr TFileChunkReaderProvider::CreateReader(
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    NChunkClient::IChunkReaderPtr chunkReader)
{
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());

    i64 startOffset = 0;
    if (chunkSpec.has_lower_limit() && chunkSpec.lower_limit().has_offset()) {
        startOffset = chunkSpec.lower_limit().offset();
    }

    i64 endOffset = std::numeric_limits<i64>::max();
    if (chunkSpec.has_upper_limit() && chunkSpec.upper_limit().has_offset()) {
        endOffset = chunkSpec.upper_limit().offset();
    }

    LOG_INFO(
        "Creating file chunk reader (StartOffset: %" PRId64 ", EndOffset: %" PRId64 ")",
        startOffset,
        endOffset);

    return New<TFileChunkReader>(
        Config,
        std::move(chunkReader),
        UncompressedBlockCache,
        NCompression::ECodec(miscExt.compression_codec()),
        startOffset,
        endOffset);
}

void TFileChunkReaderProvider::OnReaderOpened(
    TFileChunkReaderPtr reader,
    NChunkClient::NProto::TChunkSpec& chunkSpec)
{
    UNUSED(reader);
    UNUSED(chunkSpec);
}

void TFileChunkReaderProvider::OnReaderFinished(TFileChunkReaderPtr reader)
{
    UNUSED(reader);
}

bool TFileChunkReaderProvider::KeepInMemory() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TFileChunkReaderFacade::TFileChunkReaderFacade(TFileChunkReader* reader)
    : Reader(reader)
{ }

TSharedRef TFileChunkReaderFacade::GetBlock() const
{
    return Reader->GetBlock();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
