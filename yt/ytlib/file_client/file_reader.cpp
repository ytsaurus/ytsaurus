#include "stdafx.h"
#include "file_reader.h"

#include "file_chunk_server_meta.pb.h"

#include "../misc/string.h"
#include "../misc/sync.h"
#include "../file_server/file_ypath_rpc.h"
#include "file_chunk_meta.pb.h"

namespace NYT {
namespace NFileClient {

using namespace NCypress;
using namespace NFileServer;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const TConfig& config,
    NRpc::IChannel* masterChannel,
    NTransactionClient::ITransaction* transaction,
    const NYTree::TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , Path(path)
    , Closed(false)
    , Aborted(false)
    , BlockIndex(0)
{
    YASSERT(masterChannel != NULL);
}

void TFileReader::Open()
{
    auto transactionId =
        ~Transaction == NULL 
        ? NullTransactionId
        : Transaction->GetId();

    LOG_INFO("File reader is open (Path: %s, TransactionId: %s)",
        ~Path,
        ~transactionId.ToString());

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(Config.MasterRpcTimeout);

    // Get chunk info.
    LOG_INFO("Getting chunk info");

    auto getChunkRequest = TFileYPathProxy::GetFileChunk();
    auto getChunkResponse = CypressProxy->Execute(
        Path,
        transactionId,
        ~getChunkRequest)->Get();

    if (!getChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error getting chunk info from master\n%s",
            ~getChunkResponse->GetError().ToString());
    }

    ChunkId = TChunkId::FromProto(getChunkResponse->chunkid());
    auto addresses = FromProto<Stroka>(getChunkResponse->holderaddresses());

    if (addresses.empty()) {
        // TODO: Monster says we should wait here
        LOG_ERROR_AND_THROW(yexception(), "Chunk is not available (ChunkId: %s)", ~ChunkId.ToString());
    }

    LOG_INFO("Chunk info is received from master (ChunkId: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    // ToDo: use TRetriableReader.
    auto remoteReader = New<TRemoteReader>(
        Config.RemoteReader,
        ChunkId,
        addresses);

    auto getInfoResult = remoteReader->AsyncGetChunkInfo()->Get();
    if (!getInfoResult.IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error getting chunk info from holder\n%s",
            ~getInfoResult.ToString());
    }
    auto& chunkInfo = getInfoResult.Value();

    BlockCount = chunkInfo.blocks_size();
    Size = chunkInfo.size();
    TFileChunkAttributes fileAttributes =
        chunkInfo.attributes().GetExtension(TFileChunkAttributes::FileAttributes);
    auto codecId = ECodecId(fileAttributes.codecid());
    Codec = GetCodec(codecId);

    // Take all blocks.
    yvector<int> blockIndexes;
    blockIndexes.reserve(BlockCount);
    for (int index = 0; index < BlockCount; ++index) {
        blockIndexes.push_back(index);
    }

    SequentialReader = New<TSequentialReader>(
        Config.SequentialReader,
        blockIndexes,
        ~remoteReader);

    // Bind to the transaction.
    if (~Transaction != NULL) {
        OnAborted_ = FromMethod(&TFileReader::OnAborted, TPtr(this));
        Transaction->SubscribeAborted(OnAborted_);
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
