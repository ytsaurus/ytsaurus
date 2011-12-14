#include "stdafx.h"
#include "table_reader.h"

#include "../misc/sync.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NCypress;
using namespace NTableServer;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTableReader::TTableReader(
    TConfig* config,
    NTransactionClient::ITransaction::TPtr transaction,
    NRpc::IChannel* masterChannel,
    const TChannel& readChannel,
    const Stroka& path)
    : Config(config)
    , Transaction(transaction)
{
    YASSERT(masterChannel != NULL);

    TTransactionId txId = ~Transaction == NULL ? NullTransactionId : Transaction->GetId();

    TCypressServiceProxy Proxy(masterChannel);
    Proxy.SetTimeout(Config->CypressRpcTimeout);

    auto req = TTableYPathProxy::GetTableChunks();
    req->SetPath(path);

    auto rsp = Proxy.Execute(path, txId, ~req)->Get();
    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Error adding chunks to table (YPath: %s)\n%s",
            ~path,
            ~rsp->GetError().ToString());
    }

    Reader = New<TChunkSequenceReader>(
        ~Config->ChunkSequenceReader,
        readChannel,
        txId,
        masterChannel,
        FromProto<NChunkClient::TChunkId, Stroka>(rsp->chunkids()),
        0,
        INT_MAX);
    Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

    if (~Transaction != NULL) {
        OnAborted_ = FromMethod(
            &TTableReader::OnAborted,
            TPtr(this));

        Transaction->SubscribeAborted(OnAborted_);
    }
}

void TTableReader::OnAborted()
{
    Reader->Cancel(TError("Transaction aborted"));
    OnAborted_.Reset();
}

bool TTableReader::NextRow()
{
    if (Reader->HasNextRow()) {
        Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
        return true;
    }

    return false;
}

bool TTableReader::NextColumn()
{
    return Reader->NextColumn();
}

TColumn TTableReader::GetColumn()
{
    return Reader->GetColumn();
}

TValue TTableReader::GetValue()
{
    return Reader->GetValue();
}

void TTableReader::Close()
{
    if (~Transaction != NULL) {
        Transaction->UnsubscribeAborted(OnAborted_);
        OnAborted_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
