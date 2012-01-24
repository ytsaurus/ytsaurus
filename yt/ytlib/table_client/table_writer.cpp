#include "stdafx.h"
#include "table_writer.h"

#include <ytlib/misc/sync.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypress;
using namespace NTransactionClient;
using namespace NTableServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): use totalReplicaCount

TTableWriter::TTableWriter(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    ITransaction* transaction,
    TTransactionManager* transactionManager,
    const TSchema& schema,
    const TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , Schema(schema)
    , Path(path)
    , IsOpen(false)
    , IsClosed(false)
    , Proxy(masterChannel)
    , Logger(TableClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);
    YASSERT(transactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~TransactionId.ToString()));

    Proxy.SetTimeout(Config->MasterRpcTimeout);
}

void TTableWriter::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YVERIFY(!IsOpen);
    YVERIFY(!IsClosed);

    LOG_INFO("Opening table writer");

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->Start();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating upload transaction\n%s",
            ex.what());
    }
    ListenTransaction(~UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)", ~UploadTransaction->GetId().ToString());

    LOG_INFO("Requesting chunk list id");
    auto getChunkListIdReq = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(Path, TransactionId));
    auto getChunkListIdRsp = Proxy.Execute(~getChunkListIdReq)->Get();
    if (!getChunkListIdRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error requesting chunk list id\n%s",
            ~getChunkListIdRsp->GetError().ToString());
    }
    ChunkListId = TChunkListId::FromProto(getChunkListIdRsp->chunk_list_id());
    LOG_INFO("Chunk list id received (ChunkListId: %s)", ~ChunkListId.ToString());

    Writer = New<TChunkSequenceWriter>(
        ~Config->ChunkSequenceWriter, 
        ~MasterChannel,
        UploadTransaction->GetId(),
        Schema);

    Sync(~Writer, &TChunkSequenceWriter::AsyncOpen);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table writer opened");
}

void TTableWriter::Write(const TColumn& column, TValue value)
{
    VERIFY_THREAD_AFFINITY(Client);
    YVERIFY(IsOpen);

    CheckAborted();
    Writer->Write(column, value);
}

void TTableWriter::EndRow()
{
    VERIFY_THREAD_AFFINITY(Client);
    YVERIFY(IsOpen);

    CheckAborted();
    Sync(~Writer, &TChunkSequenceWriter::AsyncEndRow);
}

void TTableWriter::Close()
{
    VERIFY_THREAD_AFFINITY(Client);

    if (!IsOpen)
        return;

    IsOpen = false;
    IsClosed = true;

    CheckAborted();

    LOG_INFO("Closing table writer");

    LOG_INFO("Closing chunk writer");
    Sync(~Writer, &TChunkSequenceWriter::AsyncClose);
    LOG_INFO("Chunk writer closed");

    LOG_INFO("Attaching chunks");
    auto req = TChunkListYPathProxy::Attach(FromObjectId(ChunkListId));
    ToProto<TChunkId, Stroka>(*req->mutable_children_ids(), Writer->GetWrittenChunkIds());
    auto rsp = Proxy.Execute(~req)->Get();
    if (!rsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error attaching chunks\n%s",
            ~rsp->GetError().ToString());
    }
    LOG_INFO("Chunks attached");

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error committing upload transaction\n%s",
            ex.what());
    }
    LOG_INFO("Upload transaction committed");

    LOG_INFO("Table writer closed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
