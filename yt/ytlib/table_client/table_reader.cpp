#include "stdafx.h"
#include "table_reader.h"

#include <ytlib/misc/sync.h>

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
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , BlockCache(blockCache)
    , ReadChannel(readChannel)
    , Path(path)
    , IsOpen(false)
    , IsClosed(false)
    , Proxy(masterChannel)
    , Logger(TableClientLogger)
{
    YASSERT(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~TransactionId.ToString()));

    Proxy.SetTimeout(Config->MasterRpcTimeout);
}

void TTableReader::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    LOG_INFO("Opening table reader");

    LOG_INFO("Fetching table info");
    auto req = TTableYPathProxy::Fetch(WithTransaction(Path, TransactionId));
    auto rsp = Proxy.Execute(~req)->Get();
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
        ReadChannel,
        TransactionId,
        ~MasterChannel,
        ~BlockCache,
        chunkIds,
        0,
        // TODO(babenko): fixme, make i64
        std::numeric_limits<int>::max());
    Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table reader opened");
}

bool TTableReader::NextRow()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    if (!Reader->HasNextRow()) {
        return false;
    }

    Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
    return true;
}

bool TTableReader::NextColumn()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();
    return Reader->NextColumn();
}

TColumn TTableReader::GetColumn() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Reader->GetColumn();
}

TValue TTableReader::GetValue() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Reader->GetValue();
}

void TTableReader::Close()
{
    VERIFY_THREAD_AFFINITY(Client);

    if (!IsOpen)
        return;

    IsOpen = false;
    IsClosed = true;

    LOG_INFO("Table reader closed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
