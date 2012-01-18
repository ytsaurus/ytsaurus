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
    const TSchema& schema,
    const TYPath& path)
    : Config(config)
    , Transaction(transaction)
    , Proxy(masterChannel)
    , Logger(TableClientLogger)
{
    YASSERT(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~transaction->GetId().ToString()));

    OnAborted_ = FromMethod(&TTableWriter::OnAborted, TPtr(this));
    Transaction->SubscribeAborted(OnAborted_);

    Proxy.SetTimeout(Config->MasterRpcTimeout);

    LOG_INFO("Requesting chunk list id");
    auto getChunkListIdReq = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(path, Transaction->GetId()));
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
        Transaction->GetId(),
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
    OnAborted_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
