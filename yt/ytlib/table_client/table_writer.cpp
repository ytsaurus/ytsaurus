#include "stdafx.h"
#include "table_writer.h"

#include "../cypress/cypress_ypath_rpc.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTransactionClient;
using namespace NCypress;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    const TConfig& config,
    NRpc::IChannel::TPtr masterChannel,
    ITransaction::TPtr transaction,
    ICodec* codec,
    const TSchema& schema,
    const Stroka& path)
    : Config(config)
    , Path(path)
    , Transaction(transaction)
    , MasterChannel(masterChannel)
    , Writer(New<TChunkSetWriter>(
        config.ChunkSetConfig, 
        schema, 
        codec, 
        Transaction->GetId(), 
        MasterChannel))
    , Proxy(masterChannel)
{
    YASSERT(~masterChannel != NULL);
    YASSERT(~transaction != NULL);

    Proxy.SetTimeout(Config.RpcTimeout);

    OnAborted = FromMethod(
        &TTableWriter::OnTransactionAborted,
        TPtr(this));
    Transaction->OnAborted().Subscribe(OnAborted);

    if (!NodeExists(path)) {
        CreateTableNode(path);
    }
}

void TTableWriter::Init()
{
    Writer->Sync(&TChunkSetWriter::AsyncInit);
}

bool TTableWriter::NodeExists(const Stroka& nodePath)
{
    auto req = TCypressYPathProxy::GetId();

    auto rsp = Proxy.Execute(nodePath, Transaction->GetId(), ~req)->Get();

    if (!rsp->IsOK()) {
        const auto& error = rsp->GetError();
        if (error.IsRpcError()) {
            Writer->Cancel(error.ToString());
            ythrow yexception() << Sprintf("Error checking table existence (Path: %s)\n%s",
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
    auto req = TCypressYPathProxy::Create();
    req->SetManifest("{type=table}");

    auto rsp = Proxy.Execute(nodePath, Transaction->GetId(), ~req)->Get();

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

    // TODO: use node id
    auto req = TTableYPathProxy::AddTableChunks();
    FOREACH(const auto& chunkId, Writer->GetWrittenChunks()) {
        req->AddChunkIds(chunkId.ToProto());
    }

    auto rsp = Proxy.Execute(Path, Transaction->GetId(), ~req)->Get();

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
