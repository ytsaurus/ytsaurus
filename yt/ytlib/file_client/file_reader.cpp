#include "stdafx.h"
#include "file_reader.h"

#include "../misc/string.h"
#include "../misc/sync.h"
#include "../file_server/file_ypath_rpc.h"

namespace NYT {
namespace NFileClient {

using namespace NCypress;
using namespace NFileServer;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const TConfig& config,
    NRpc::IChannel* masterChannel,
    NTransactionClient::ITransaction* transaction,
    NYTree::TYPath path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , Path(path)
    , Closed(false)
    , Aborted(false)
    , BlockIndex(0)
{
    YASSERT(masterChannel != NULL);

    auto transactionId =
        ~Transaction == NULL 
        ? NullTransactionId
        : Transaction->GetId();

    LOG_INFO("File reader is open (Path: %s, TransactionId: %s)",
        ~Path,
        ~transactionId.ToString());

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(config.MasterRpcTimeout);

    // Get chunk info.
    LOG_INFO("Getting chunk info");

    auto getChunkRequest = TFileYPathProxy::GetFileChunk();
    auto getChunkResponse = CypressProxy->Execute(
        Path,
        transactionId,
        ~getChunkRequest)->Get();

    if (!getChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error getting chunk info\n%s",
            ~getChunkResponse->GetError().ToString());
    }

    ChunkId = TChunkId::FromProto(getChunkResponse->chunkid());
    BlockCount = getChunkResponse->blockcount();
    Size = getChunkResponse->size();
    auto addresses = FromProto<Stroka>(getChunkResponse->holderaddresses());
    auto codecId = ECodecId(getChunkResponse->codecid());

    LOG_INFO("Chunk info is received (ChunkId: %s, BlockCount: %d, Size: %" PRId64 ", HolderAddresses: [%s], CodecId: %s)",
        ~ChunkId.ToString(),
        BlockCount,
        Size,
        ~JoinToString(addresses),
        ~codecId.ToString());

    if (addresses.empty()) {
        // TODO: Monster says we should wait here
        LOG_ERROR_AND_THROW(yexception(), "Chunk is not available (ChunkId: %s)", ~ChunkId.ToString());
    }

    Codec = GetCodec(codecId);

    // Take all blocks.
    yvector<int> blockIndexes;
    blockIndexes.reserve(BlockCount);
    for (int index = 0; index < BlockCount; ++index) {
        blockIndexes.push_back(index);
    }

    // Construct readers.
    // ToDo: use TRetriableReader.
    auto remoteReader = New<TRemoteReader>(
        Config.RemoteReader,
        ChunkId,
        addresses);

    SequentialReader = New<TSequentialReader>(
        config.SequentialReader,
        blockIndexes,
        ~remoteReader);

    // Bind to the transaction.
    if (~Transaction != NULL) {
        OnAborted_ = FromMethod(&TFileReader::OnAborted, TPtr(this));
        transaction->SubscribeAborted(OnAborted_);
    }
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

    LOG_INFO("Block is read (BlockIndex: %d)", BlockIndex);

    ++BlockIndex;
    return block;
}

i64 TFileReader::GetSize() const
{
    return Size;
}

void TFileReader::Close()
{
    if (Closed)
        return;

    CheckAborted();

    Finish();

    LOG_INFO("File reader is closed");
}

void TFileReader::Finish()
{
    if (~Transaction != NULL) {
        Transaction->UnsubscribeAborted(OnAborted_);
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
