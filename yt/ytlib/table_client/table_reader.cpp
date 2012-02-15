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
    const TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , BlockCache(blockCache)
    , Path(path)
    , IsOpen(false)
    , Proxy(masterChannel)
    , Logger(TableClientLogger)
{
    YASSERT(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~TransactionId.ToString()));
}

void TTableReader::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);

    LOG_INFO("Opening table reader");

    LOG_INFO("Fetching table info");
    auto fetchReq = TTableYPathProxy::Fetch(WithTransaction(Path, TransactionId));
    auto fetchRsp = Proxy.Execute(~fetchReq)->Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching table info\n%s",
            ~fetchRsp->GetError().ToString());
    }

    yvector<TChunkId> chunkIds;
    chunkIds.reserve(fetchRsp->chunks_size());
    FOREACH (const auto& chunkInfo, fetchRsp->chunks()) {
        chunkIds.push_back(TChunkId::FromProto(chunkInfo.chunk_id()));
    }

    auto channel = TChannel::FromProto(fetchRsp->channel());

    Reader = New<TChunkSequenceReader>(
        ~Config->ChunkSequenceReader,
        channel,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
