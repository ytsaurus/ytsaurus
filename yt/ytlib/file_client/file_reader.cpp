#include "stdafx.h"
#include "file_reader.h"
#include "config.h"

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/chunk_client/chunk_replica.h>

namespace NYT {
namespace NFileClient {

using namespace NCypressClient;
using namespace NYTree;
using namespace NYPath;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath)
    : TFileReaderBase(config, masterChannel, blockCache)
    , Transaction(transaction)
    , RichPath(richPath)
{
    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));
}

void TFileReader::Open()
{
    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");
    auto fetchReq = TFileYPathProxy::FetchFile(RichPath);
    SetTransactionId(fetchReq, Transaction);

    auto fetchRsp = Proxy.Execute(fetchReq).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching file info");

    NodeDirectory->MergeFrom(fetchRsp->node_directory());
    auto chunkId = FromProto<TChunkId>(fetchRsp->chunk_id());
    auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(fetchRsp->replicas());
    LOG_INFO("File info received (ChunkId: %s, Addresses: [%s])",
        ~ToString(chunkId),
        ~JoinToString(replicas, NodeDirectory));

    TFileReaderBase::Open(chunkId, replicas);

    if (Transaction) {
        ListenTransaction(Transaction);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
