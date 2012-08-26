#include "stdafx.h"
#include "file_reader_base.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NFileClient {

using namespace NCypressClient;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileReaderBase::TFileReaderBase(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    IBlockCachePtr blockCache)
    : Config(config)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , IsOpen(false)
    , BlockCount(0)
    , BlockIndex(0)
    , Proxy(masterChannel)
    , Logger(FileReaderLogger)
{
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(blockCache);
}

void TFileReaderBase::Open(
    const TChunkId& chunkId,
    const std::vector<Stroka>& nodeAddresses)
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(!IsOpen);

    auto remoteReader = CreateRemoteReader(
        Config,
        BlockCache,
        MasterChannel,
        chunkId,
        nodeAddresses);

    LOG_INFO("Requesting chunk info");

    auto getMetaResult = remoteReader->AsyncGetChunkMeta().Get();
    if (!getMetaResult.IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error getting chunk meta\n%s",
            ~getMetaResult.ToString());
    }

    auto& chunkMeta = getMetaResult.Value();
    YCHECK(chunkMeta.type() == EChunkType::File);

    if (chunkMeta.version() != FormatVersion) {
        LOG_ERROR_AND_THROW(
            yexception(), 
            "Chunk format version mismatch (Expected: %d, Received: %d)",
            FormatVersion,
            chunkMeta.version());
    }

    auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta.extensions());
    BlockCount = blocksExt.blocks_size();

    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta.extensions());
    Size = miscExt.uncompressed_data_size();

    LOG_INFO("Chunk info received (BlockCount: %d, Size: %" PRId64 ")",
        BlockCount,
        Size);

    // Read all blocks.
    std::vector<TSequentialReader::TBlockInfo> blockSequence;
    blockSequence.reserve(BlockCount);
    for (int index = 0; index < BlockCount; ++index) {
        blockSequence.push_back(TSequentialReader::TBlockInfo(
            index, 
            blocksExt.blocks(index).size()));
    }

    SequentialReader = New<TSequentialReader>(
        Config,
        MoveRV(blockSequence),
        remoteReader,
        ECodecId(miscExt.codec_id()));

    LOG_INFO("File reader opened");

    IsOpen = true;
}

TSharedRef TFileReaderBase::Read()
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(IsOpen);

    CheckAborted();

    if (!SequentialReader->HasNext()) {
        return TSharedRef();
    }

    LOG_INFO("Reading block (BlockIndex: %d)", BlockIndex);
    Sync(~SequentialReader, &TSequentialReader::AsyncNextBlock);
    auto block = SequentialReader->GetBlock();
    ++BlockIndex;
    LOG_INFO("Block read (BlockIndex: %d)", BlockIndex);

    return block;
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
