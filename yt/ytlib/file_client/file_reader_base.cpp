#include "stdafx.h"
#include "file_reader_base.h"
#include "private.h"
#include "config.h"
#include "chunk_meta_extensions.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/block_cache.h>

#include <limits>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TFileReaderBase::TFileReaderBase()
    : IsOpen(false)
    , BlockIndex(0)
    , Size(0)
    , StartOffset(0)
    , EndOffset(std::numeric_limits<i64>::max())
    , Logger(FileReaderLogger)
{ }

void TFileReaderBase::Open(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    NChunkClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(blockCache);

    YCHECK(!IsOpen);

    Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(chunkId)));

    auto remoteReader = CreateRemoteReader(
        config,
        blockCache,
        masterChannel,
        nodeDirectory,
        Null,
        chunkId,
        replicas);

    LOG_INFO("Requesting chunk info");

    auto getMetaResult = remoteReader->AsyncGetChunkMeta().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(getMetaResult, "Error getting chunk meta");

    auto& chunkMeta = getMetaResult.Value();
    YCHECK(chunkMeta.type() == EChunkType::File);

    if (chunkMeta.version() != FormatVersion) {
        THROW_ERROR_EXCEPTION("Chunk format version mismatch: expected %d, actual %d",
            FormatVersion,
            chunkMeta.version());
    }

    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta.extensions());
    Size = miscExt.uncompressed_data_size();

    StartOffset = offset.Get(StartOffset);
    EndOffset = std::min(StartOffset + length.Get(Size), Size);

    std::vector<TSequentialReader::TBlockInfo> blockSequence;

    // COMPAT: new file chunk meta!
    auto fileBlocksExt = FindProtoExtension<NFileClient::NProto::TBlocksExt>(chunkMeta.extensions());

    i64 selectedSize = 0;
    auto addBlock = [&] (int index, i64 size) {
        if (StartOffset >= size) {
            StartOffset -= size;
            EndOffset -= size;
            ++BlockIndex;
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
    YCHECK(blockCount > 0);

    LOG_INFO("Chunk info received (BlockCount: %d, Size: %" PRId64 ")",
        blockCount,
        Size);
    LOG_INFO("Reading %d blocks starting from %d)",
        static_cast<int>(blockSequence.size()),
        BlockIndex);

    SequentialReader = New<TSequentialReader>(
        config,
        std::move(blockSequence),
        remoteReader,
        NCompression::ECodec(miscExt.compression_codec()));

    LOG_INFO("File reader opened");

    IsOpen = true;
}

TSharedRef TFileReaderBase::Read()
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(IsOpen);

    if (!SequentialReader->HasNext()) {
        return TSharedRef();
    }

    LOG_INFO("Reading block (BlockIndex: %d)", BlockIndex);
    Sync(~SequentialReader, &TSequentialReader::AsyncNextBlock);
    auto block = SequentialReader->GetBlock();
    ++BlockIndex;
    LOG_INFO("Block read (BlockIndex: %d)", BlockIndex);

    auto begin = block.Begin();
    auto end = block.End();

    YCHECK(EndOffset > 0);

    if (EndOffset < block.Size()) {
        end = block.Begin() + EndOffset;
    }

    EndOffset -= block.Size();

    if (StartOffset > 0) {
        begin = block.Begin() + StartOffset;
    }

    StartOffset -= block.Size();

    return block.Slice(TRef(begin, end));
}

i64 TFileReaderBase::GetSize() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(IsOpen);

    return Size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
