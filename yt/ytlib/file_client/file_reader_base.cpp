#include "stdafx.h"
#include "file_reader_base.h"
#include <ytlib/file_client/file_chunk_meta.pb.h>

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>
#include <ytlib/file_server/file_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

using namespace NCypress;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NFileServer;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TFileReaderBase::TFileReaderBase(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    IBlockCache* blockCache)
    : Config(config)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , IsOpen(false)
    , BlockCount(0)
    , BlockIndex(0)
    , Proxy(masterChannel)
    , Logger(FileClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);
    YASSERT(blockCache);
}

void TFileReaderBase::Open(const NChunkServer::TChunkId& chunkId, const yvector<Stroka>& holderAddresses)
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);

    auto remoteReader = CreateRemoteReader(
        ~Config->RemoteReader,
        ~BlockCache,
        ~MasterChannel,
        chunkId,
        holderAddresses);

    LOG_INFO("Requesting chunk info");
    auto getInfoResult = remoteReader->AsyncGetChunkInfo()->Get();
    if (!getInfoResult.IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error getting chunk info\n%s",
            ~getInfoResult.ToString());
    }
    auto& chunkInfo = getInfoResult.Value();
    BlockCount = chunkInfo.blocks_size();
    Size = chunkInfo.size();
    auto fileAttributes = chunkInfo.attributes().GetExtension(TFileChunkAttributes::file_attributes);
    auto codecId = ECodecId(fileAttributes.codec_id());
    Codec = GetCodec(codecId);
    LOG_INFO("Chunk info received (BlockCount: %d, Size: %" PRId64 ", CodecId: %s)",
        BlockCount,
        Size,
        ~codecId.ToString());

    // Take all blocks.
    yvector<int> blockIndexes;
    blockIndexes.reserve(BlockCount);
    for (int index = 0; index < BlockCount; ++index) {
        blockIndexes.push_back(index);
    }

    SequentialReader = New<TSequentialReader>(
        ~Config->SequentialReader,
        blockIndexes,
        ~remoteReader);

    LOG_INFO("File reader opened");

    IsOpen = true;
}

TSharedRef TFileReaderBase::Read()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    if (!SequentialReader->HasNext()) {
        return TSharedRef();
    }

    LOG_INFO("Reading block (BlockIndex: %d)", BlockIndex);
    Sync(~SequentialReader, &TSequentialReader::AsyncNextBlock);
    auto compressedBlock = SequentialReader->GetBlock();
    auto block = Codec->Decompress(compressedBlock);
    ++BlockIndex;
    LOG_INFO("Block read (BlockIndex: %d)", BlockIndex);

    return block;
}

i64 TFileReaderBase::GetSize() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
