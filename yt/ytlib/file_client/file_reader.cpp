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
    , Path(path)
    , Closed(false)
    , Aborted(false)
    , BlockIndex(0)
    , Logger(FileClientLogger)
{
    YASSERT(masterChannel);

    auto transactionId =
        !Transaction
        ? NullTransactionId
        : Transaction->GetId();

    // Bind to the transaction.
    if (Transaction) {
        OnAborted_ = FromMethod(&TFileReader::OnAborted, TPtr(this));
        Transaction->SubscribeAborted(OnAborted_);
    }

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~Path,
        ~transactionId.ToString()));

    LOG_INFO("File reader open");

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(Config->MasterRpcTimeout);

    LOG_INFO("Fetching file info");
    auto fetchReq = TFileYPathProxy::Fetch();
    auto fetchRsp = CypressProxy->Execute(~fetchReq, Path, transactionId)->Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching file info\n%s",
            ~fetchRsp->GetError().ToString());
    }
    ChunkId = TChunkId::FromProto(fetchRsp->chunk_id());
    auto holderAddresses = FromProto<Stroka>(fetchRsp->holder_addresses());
    FileName = fetchRsp->file_name();
    Executable = fetchRsp->executable();
    LOG_INFO("Read info received (ChunkId: %s, FileName: %s, Executable: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~FileName,
        ~ToString(Executable),
        ~JoinToString(holderAddresses));

    auto remoteReader = CreateRemoteReader(
        ~Config->RemoteReader,
        blockCache,
        ~MasterChannel,
        ChunkId,
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
}

TSharedRef TFileReader::Read()
{
    if (Closed) {
        ythrow yexception() << "File reader is already closed";
    }

    CheckAborted();

    if (!SequentialReader->HasNext()) {
        Close();
        return TSharedRef();
    }

    LOG_INFO("Reading block (BlockIndex: %d)", BlockIndex);
    Sync(~SequentialReader, &TSequentialReader::AsyncNextBlock);
    auto compressedBlock = SequentialReader->GetBlock();
    auto block = Codec->Decompress(compressedBlock);
    LOG_INFO("Block read (BlockIndex: %d)", BlockIndex);

    ++BlockIndex;

    return block;
}

i64 TFileReader::GetSize() const
{
    return Size;
}

Stroka TFileReader::GetFileName() const
{
    return FileName;
}

bool TFileReader::IsExecutable()
{
    return Executable;
}

void TFileReader::Close()
{
    if (Closed)
        return;

    CheckAborted();

    Finish();

    LOG_INFO("File reader closed");
}

void TFileReader::Finish()
{
    if (Transaction) {
        Transaction->UnsubscribeAborted(OnAborted_);
        Transaction.Reset();
    }
    OnAborted_.Reset();
    SequentialReader.Reset();
    Closed = true;
}

void TFileReader::CheckAborted()
{
    if (Aborted) {
        Finish();

        LOG_WARNING_AND_THROW(yexception(), "Transaction aborted, file reader canceled");
    }
}

void TFileReader::OnAborted()
{
    Aborted = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
