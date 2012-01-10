#include "stdafx.h"
#include "table_reader.h"

#include <ytlib/misc/sync.h>
#include <ytlib/chunk_server/common.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypress;
using namespace NTableServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TTableReader::TTableReader(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    NTransactionClient::ITransaction* transaction,
    NChunkClient::IBlockCache* blockCache,
    const TChannel& readChannel,
    const TYPath& path)
    : Config(config)
    , Transaction(transaction)
    , Logger(TableClientLogger)
{
    YASSERT(masterChannel);

    OnAborted_ = FromMethod(&TTableReader::OnAborted, TPtr(this));

    auto transactionId = Transaction ? Transaction->GetId() : NullTransactionId;

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~transactionId.ToString()));

    TCypressServiceProxy Proxy(masterChannel);
    Proxy.SetTimeout(Config->MasterRpcTimeout);

    LOG_INFO("Fetching table info");
    auto req = TTableYPathProxy::Fetch();
    auto rsp = Proxy.Execute(path, transactionId, ~req)->Get();
    if (!rsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching table info\n%s",
            ~rsp->GetError().ToString());
    }

    yvector<TChunkId> chunkIds;
    chunkIds.reserve(rsp->chunks_size());
    FOREACH (const auto& chunkInfo, rsp->chunks()) {
        chunkIds.push_back(TChunkId::FromProto(chunkInfo.chunk_id()));
    }

    Reader = New<TChunkSequenceReader>(
        ~Config->ChunkSequenceReader,
        readChannel,
        transactionId,
        masterChannel,
        blockCache,
        chunkIds,
        0,
        // TODO(babenko): fixme, make i64
        std::numeric_limits<int>::max());
    Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

    if (Transaction) {
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
        Transaction.Reset();
    }
    OnAborted_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
