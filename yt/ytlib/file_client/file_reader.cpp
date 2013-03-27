#include "stdafx.h"
#include "file_reader.h"
#include "file_reader_base.h"
#include "private.h"
#include "config.h"
#include "file_ypath_proxy.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/node_directory.h>

namespace NYT {
namespace NFileClient {

//using namespace NCypressClient;
using namespace NYTree;
using namespace NYPath;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader()
    : BaseReader(new TFileReaderBase())
    , Logger(FileReaderLogger)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
}

TFileReader::~TFileReader()
{ }

void TFileReader::Open(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(blockCache);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~richPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));

    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");
    auto fetchReq = TFileYPathProxy::FetchFile(richPath);
    SetTransactionId(fetchReq, transaction);

    NObjectClient::TObjectServiceProxy proxy(masterChannel);

    auto fetchRsp = proxy.Execute(fetchReq).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching file info");

    auto nodeDirectory = New<TNodeDirectory>();
    nodeDirectory->MergeFrom(fetchRsp->node_directory());
    auto chunkId = FromProto<TChunkId>(fetchRsp->chunk_id());
    auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(fetchRsp->replicas());

    LOG_INFO("File info received (ChunkId: %s, Addresses: [%s])",
        ~ToString(chunkId),
        ~JoinToString(replicas, nodeDirectory));

    BaseReader->Open(
        config,
        masterChannel,
        blockCache,
        nodeDirectory,
        chunkId,
        replicas,
        offset,
        length);

    if (transaction) {
        ListenTransaction(transaction);
    }
}

TSharedRef TFileReader::Read()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    CheckAborted();
    return BaseReader->Read();
}

i64 TFileReader::GetSize() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    return BaseReader->GetSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
