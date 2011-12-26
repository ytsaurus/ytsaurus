#include "stdafx.h"
#include "table_reader.h"

#include "../misc/sync.h"

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypress;
using namespace NTableServer;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTableReader::TTableReader(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    NTransactionClient::ITransaction* transaction,
    const TChannel& readChannel,
    const TYPath& path)
    : Config(config)
    , Transaction(transaction)
{
    YASSERT(masterChannel);

    auto transactionId = Transaction ? Transaction->GetId() : NullTransactionId;

    TCypressServiceProxy Proxy(masterChannel);
    Proxy.SetTimeout(Config->MasterRpcTimeout);

    auto req = TTableYPathProxy::GetTableChunks();
    req->SetPath(path);

    auto rsp = Proxy.Execute(path, transactionId, ~req)->Get();
    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Error requesting table chunks (YPath: %s)\n%s",
            ~path,
            ~rsp->GetError().ToString());
    }

    Reader = New<TChunkSequenceReader>(
        ~Config->ChunkSequenceReader,
        readChannel,
        transactionId,
        masterChannel,
        FromProto<NChunkClient::TChunkId, Stroka>(rsp->chunk_ids()),
        0,
        INT_MAX);
    Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

    if (Transaction) {
        OnAborted_ = FromMethod(&TTableReader::OnAborted, TPtr(this));
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
    if (!Reader->HasNextRow()) {
        return false;
    }

    Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
    return true;
}

bool TTableReader::NextColumn()
{
    return Reader->NextColumn();
}

TColumn TTableReader::GetColumn() const
{
    return Reader->GetColumn();
}

TValue TTableReader::GetValue() const
{
    return Reader->GetValue();
}

void TTableReader::Close()
{
    if (Transaction) {
        Transaction->UnsubscribeAborted(OnAborted_);
        OnAborted_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
