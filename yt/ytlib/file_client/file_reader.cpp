#include "stdafx.h"
#include "file_reader.h"

#include "../misc/string.h"
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

    // Get file chunk information.
    LOG_INFO("Getting file chunk information (Path: %s, TransactionId: %s)",
        ~Path,
        ~transactionId.ToString());

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(config.MasterRpcTimeout);

    auto getChunkRequest = TFileYPathProxy::GetFileChunk();
    auto getChunkResponse = CypressProxy->Execute(
        Path,
        transactionId,
        ~getChunkRequest)->Get();

    if (!getChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error getting file chunk information\n%s",
            ~getChunkResponse->GetError().ToString());
    }

    ChunkId = TChunkId::FromProto(getChunkResponse->GetChunkId());
    BlockCount = getChunkResponse->GetBlockCount();
    Size = getChunkResponse->GetSize();
    auto addresses = FromProto<Stroka>(getChunkResponse->GetHolderAddresses());

    CodecId = ECodecId::None; // TODO: fill in CodecId from server meta

    LOG_INFO("File chunk information received (ChunkId: %s, BlockCount: %d, Size: %" PRId64 ", HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        BlockCount,
        Size,
        ~JoinToString(addresses));

    if (addresses.empty()) {
        // TODO: Monster says we should wait here
        LOG_ERROR_AND_THROW(yexception(), "File chunk is not available (ChunkId: %s)", ~ChunkId.ToString());
    }

    // Take all blocks.
    yvector<int> blockIndexes;
    blockIndexes.reserve(BlockCount);
    for (int index = 0; index < BlockCount; ++index) {
        blockIndexes.push_back(index);
    }

    // Construct readers.
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

    if (BlockIndex >= BlockCount) {
        return TSharedRef();
    }

    LOG_INFO("Reading file block (BlockIndex: %d)", BlockIndex);
    auto result = SequentialReader->AsyncGetNextBlock()->Get();
    if (!result.IsOK) {
        // TODO: use TError
        ythrow yexception() << Sprintf("Error reading file block\n%s",
            "--here come the details--");
    }

    auto& codec = ICodec::GetCodec(CodecId);
    auto decompressedBlock = codec.Decode(result.Block);

    LOG_INFO("File block is read");

    ++BlockIndex;
    return decompressedBlock;
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

        LOG_WARNING_AND_THROW(yexception(), "Transaction aborted, file reading canceled");
    }
}

void TFileReader::OnAborted()
{
    Aborted = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
