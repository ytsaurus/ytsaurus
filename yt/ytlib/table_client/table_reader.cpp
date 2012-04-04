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
    auto fetchRsp = Proxy.Execute(fetchReq)->Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching table info\n%s",
            ~fetchRsp->GetError().ToString());
    }

    std::vector<NProto::TInputChunk> inputChunks = 
        FromProto<NProto::TInputChunk>(fetchRsp->chunks());

    Reader = New<TChunkSequenceReader>(
        ~Config->ChunkSequenceReader,
        ~MasterChannel,
        ~BlockCache,
        inputChunks);
    Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table reader opened");
}

void TTableReader::NextRow()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
}

bool TTableReader::IsValid() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    return Reader->IsValid();
}

const TRow& TTableReader::GetRow() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Reader->GetCurrentRow();
}

const TKey& TTableReader::GetKey() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Reader->GetCurrentKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
