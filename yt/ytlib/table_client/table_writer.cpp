#include "stdafx.h"
#include "table_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTransactionClient;

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
    auto req = proxy.GetNodeId();
    req->SetTransactionId(Transaction->GetId().ToProto());
    req->SetPath(nodePath);
    auto rsp = req->Invoke(Config.RpcTimeout)->Get();
    if (!rsp->IsOK()) {
        const NRpc::TError& error = rsp->GetError();
        if (!error.IsServiceError() ||
            error.GetCode() != TCypressProxy::EErrorCode::RecoverableError) 
        {
            Writer->Cancel(error.ToString());
            ythrow yexception() << 
                Sprintf("Cypress call GetNodeId failed (Path: %s; Error: %s).",
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
    TCypressProxy proxy(MasterChannel);
    auto req = proxy.Set();
    req->SetTransactionId(Transaction->GetId().ToProto());
    req->SetPath(nodePath);
    req->SetValue("<type=table>");

    auto rsp = req->Invoke(Config.RpcTimeout)->Get();
    if (!rsp->IsOK()) {
        const NRpc::TError& error = rsp->GetError();
        Writer->Cancel(error.ToString());
        ythrow yexception() << 
            Sprintf("Couldn't create table (Path: %s; Error: %s).",
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
    auto req = proxy.AddTableChunks();
    req->SetTransactionId(Transaction->GetId().ToProto());
    req->SetNodeId(NodeId);
    FOREACH(const auto& chunkId, Writer->GetWrittenChunks()) {
        req->AddChunkIds(chunkId.ToProto());
    }

    auto rsp = req->Invoke(Config.RpcTimeout)->Get();
    if (!rsp->IsOK()) {
        const NRpc::TError& error = rsp->GetError();
        ythrow yexception() << 
            Sprintf("Failed to add chunks to table (Error: %s).",
                ~error.ToString());
    }

    Transaction->OnAborted().Unsubscribe(OnAborted);
}

void TTableWriter::OnTransactionAborted()
{
    Writer->Cancel("Transaction aborted.");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
