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
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , Proxy(masterChannel)
    , Logger(TableClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);

    OnAborted_ = FromMethod(&TTableWriter::OnAborted, TPtr(this));

    if (Transaction) {
        Transaction->SubscribeAborted(OnAborted_);
    }

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~TransactionId.ToString()));

    LOG_INFO("Table writer open");

    Proxy.SetTimeout(Config->MasterRpcTimeout);

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->Start();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating upload transaction\n%s",
            ex.what());
    }
    UploadTransaction->SubscribeAborted(OnAborted_);
    LOG_INFO("Upload transaction created (TransactionId: %s)",
        ~UploadTransaction->GetId().ToString());

    LOG_INFO("Requesting chunk list id");
    auto getChunkListIdReq = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(path, TransactionId));
    auto getChunkListIdRsp = Proxy.Execute(~getChunkListIdReq)->Get();
    if (!getChunkListIdRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error requesting chunk list id\n%s",
            ~getChunkListIdRsp->GetError().ToString());
    }
    ChunkListId = TChunkListId::FromProto(getChunkListIdRsp->chunk_list_id());
    LOG_INFO("Chunk list id received (ChunkListId: %s)", ~ChunkListId.ToString());

    Writer = New<TChunkSequenceWriter>(
        ~config->ChunkSequenceWriter, 
        masterChannel,
        UploadTransaction->GetId(),
        schema);
}

void TTableWriter::Open()
{
    Sync(~Writer, &TChunkSequenceWriter::AsyncOpen);
}

void TTableWriter::Write(const TColumn& column, TValue value)
{
    Writer->Write(column, value);
}

void TTableWriter::EndRow()
{
    Sync(~Writer, &TChunkSequenceWriter::AsyncEndRow);
}

void TTableWriter::Close()
{
    Sync(~Writer, &TChunkSequenceWriter::AsyncClose);

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
        UploadTransaction.Reset();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error committing upload transaction\n%s",
            ex.what());
    }
    LOG_INFO("Upload transaction committed");

    LOG_INFO("Table writer closed");

    Finish();
}

void TTableWriter::OnAborted()
{
    Writer->Cancel(TError("Transaction aborted"));
    Finish();
}

void TTableWriter::Finish()
{
    if (Transaction) {
        Transaction->UnsubscribeAborted(OnAborted_);
        Transaction.Reset();
    }
    
    if (UploadTransaction) {
        UploadTransaction->Abort();
        UploadTransaction->UnsubscribeAborted(OnAborted_);
        UploadTransaction.Reset();
    }

    OnAborted_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
