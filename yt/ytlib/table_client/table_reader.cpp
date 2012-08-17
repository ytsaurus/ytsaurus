#include "stdafx.h"
#include "table_reader.h"
#include "config.h"
#include "table_chunk_reader.h"
#include "multi_chunk_sequential_reader.h"
#include "private.h"

#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/misc/sync.h>
#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypressClient;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

TTableReader::TTableReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::ITransactionPtr transaction,
    NChunkClient::IBlockCachePtr blockCache,
    const NYTree::TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , BlockCache(blockCache)
    , Path(path)
    , IsOpen(false)
    , Proxy(masterChannel)
    , Logger(TableReaderLogger)
{
    YASSERT(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
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
    fetchReq->add_extension_tags(TProtoExtensionTag<NChunkHolder::NProto::TMiscExt>::Value);
    fetchReq->set_fetch_node_addresses(true);

    auto fetchRsp = Proxy.Execute(fetchReq).Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching table info\n%s",
            ~fetchRsp->GetError().ToString());
    }

    auto inputChunks = FromProto<NProto::TInputChunk>(fetchRsp->chunks());

    auto provider = New<TTableChunkReaderProvider>(Config);
    Reader = New<TTableChunkSequenceReader>(
        Config,
        MasterChannel,
        BlockCache,
        MoveRV(inputChunks),
        provider);
    Sync(~Reader, &TTableChunkSequenceReader::AsyncOpen);

    if (Transaction) {
        ListenTransaction(Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table reader opened");
}

void TTableReader::NextRow()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    if (!Reader->FetchNextItem()) {
        Sync(~Reader, &TTableChunkSequenceReader::GetReadyEvent);
    }
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

    return Reader->CurrentReader()->GetRow();
}

const TNonOwningKey& TTableReader::GetKey() const 
{
    YUNREACHABLE();
}

/*
const NYTree::TYsonString& TTableReader::GetRowAttributes() const
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    return Reader->GetRowAttributes();
}
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
