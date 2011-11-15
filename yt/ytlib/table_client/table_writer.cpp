#include "stdafx.h"
#include "table_writer.h"

#include "../cypress/cypress_ypath_rpc.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTransactionClient;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    const TConfig& config,
    NRpc::IChannel::TPtr masterChannel,
    ITransaction::TPtr transaction,
    ICodec* codec,
    const TSchema& schema,
    const Stroka& ypath)
    : Config(config)
    , Transaction(transaction)
    , MasterChannel(masterChannel)
    , Writer(New<TChunkSetWriter>(
        config.ChunkSetConfig, 
        schema, 
        codec, 
        Transaction->GetId(), 
        MasterChannel))
{
    YASSERT(~masterChannel != NULL);
    YASSERT(~transaction != NULL);

    OnAborted = FromMethod(
        &TTableWriter::OnTransactionAborted,
        TPtr(this));
    Transaction->OnAborted().Subscribe(OnAborted);

    if (!NodeExists(ypath)) {
        CreateTableNode(ypath);
    }
}

void TTableWriter::Init()
{
    Writer->Sync(&TChunkSetWriter::AsyncInit);
}

bool TTableWriter::NodeExists(const Stroka& nodePath)
{
    TCypressProxy proxy(MasterChannel);
    proxy.SetTimeout(Config.RpcTimeout);

    auto req = proxy.GetNodeId();
    req->SetTransactionId(Transaction->GetId().ToProto());
    req->SetPath(nodePath);
    auto rsp = req->Invoke()->Get();
    if (!rsp->IsOK()) {
        const auto& error = rsp->GetError();
        if (!error.IsServiceError() || 
            error.GetCode() != TCypressProxy::EErrorCode::ResolutionError) 
        {
            Writer->Cancel(error.ToString());
            ythrow yexception() << Sprintf("Error checking for table existence (Path: %s)\n%s",
                ~nodePath,
                ~error.ToString());
        }
        return false;
    }

    NodeId = rsp->GetNodeId();
    return true;
}

void TTableWriter::CreateTableNode(const Stroka& nodePath)
{
    auto req = TCypressYPathProxy::Create(nodePath);
    req->SetManifest("{type=table}");

    TCypressProxy proxy(MasterChannel);
    auto rsp = proxy.Execute(Transaction->GetId(), ~req)->Get();

    if (!rsp->IsOK()) {
        const auto& error = rsp->GetError();
        Writer->Cancel(error.ToString());
        ythrow yexception() << Sprintf("Error creating table (Path: %s)\n%s",
            ~nodePath,
            ~error.ToString());
    }

    NodeId = rsp->GetNodeId();
}

void TTableWriter::Write(const TColumn& column, TValue value)
{
    Writer->Write(column, value);
}

void TTableWriter::EndRow()
{
    Writer->Sync(&TChunkSetWriter::AsyncEndRow);
}

void TTableWriter::Close()
{
    Writer->Sync(&TChunkSetWriter::AsyncClose);

    TTableProxy proxy(MasterChannel);
    proxy.SetTimeout(Config.RpcTimeout);

    auto req = proxy.AddTableChunks();
    req->SetTransactionId(Transaction->GetId().ToProto());
    req->SetNodeId(NodeId);
    FOREACH(const auto& chunkId, Writer->GetWrittenChunks()) {
        req->AddChunkIds(chunkId.ToProto());
    }

    auto rsp = req->Invoke()->Get();
    if (!rsp->IsOK()) {
        const auto& error = rsp->GetError();
        ythrow yexception() << Sprintf("Error adding chunks to table\n%s",
            ~error.ToString());
    }

    Transaction->OnAborted().Unsubscribe(OnAborted);
}

void TTableWriter::OnTransactionAborted()
{
    Writer->Cancel("Transaction aborted");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
