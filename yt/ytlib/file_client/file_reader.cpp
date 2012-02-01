#include "stdafx.h"
#include "file_reader.h"
#include "file_chunk_meta.pb.h"

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

TFileReader::TFileReader(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    ITransaction* transaction,
    IBlockCache* blockCache,
    const TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , BlockCache(blockCache)
    , Path(path)
    , IsOpen(false)
    , BlockCount(0)
    , BlockIndex(0)
    , Proxy(masterChannel)
    , Logger(FileClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);
    YASSERT(blockCache);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~Path,
        ~TransactionId.ToString()));

    Proxy.SetTimeout(Config->MasterRpcTimeout);
}

void TFileReader::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);

    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");
    auto fetchReq = TFileYPathProxy::Fetch(WithTransaction(Path, TransactionId));
    auto fetchRsp = Proxy.Execute(~fetchReq)->Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching file info\n%s",
            ~fetchRsp->GetError().ToString());
    }
    auto chunkId = TChunkId::FromProto(fetchRsp->chunk_id());
    auto holderAddresses = FromProto<Stroka>(fetchRsp->holder_addresses());
    FileName = fetchRsp->file_name();
    Executable = fetchRsp->executable();
    LOG_INFO("File info received (ChunkId: %s, FileName: %s, Executable: %s, HolderAddresses: [%s])",
        ~chunkId.ToString(),
        ~FileName,
        ~ToString(Executable),
        ~JoinToString(holderAddresses));

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

    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    LOG_INFO("File reader opened");

    IsOpen = true;
}

TSharedRef TFileReader::Read()
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

i64 TFileReader::GetSize() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Size;
}

Stroka TFileReader::GetFileName() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return FileName;
}

bool TFileReader::IsExecutable()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Executable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
