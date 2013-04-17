
#include "stdafx.h"
#include "table_reader.h"
#include "config.h"
#include "table_chunk_reader.h"
#include "private.h"

#include <ytlib/misc/sync.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

namespace NYT {
namespace NTableClient {

using namespace NCypressClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TTableReader::TTableReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::ITransactionPtr transaction,
    NChunkClient::IBlockCachePtr blockCache,
    const NYPath::TRichYPath& richPath)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , BlockCache(blockCache)
    , RichPath(richPath)
    , IsOpen(false)
    , IsReadingStarted(false)
    , Proxy(masterChannel)
    , Logger(TableReaderLogger)
{
    YCHECK(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
        ~RichPath.GetPath(),
        ~TransactionId.ToString()));
}

void TTableReader::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);

    LOG_INFO("Opening table reader");

    LOG_INFO("Fetching table info");

    auto fetchReq = TTableYPathProxy::Fetch(RichPath);
    SetTransactionId(fetchReq, TransactionId);
    fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
    // ToDo(psushin): enable ignoring lost chunks.

    auto fetchRsp = Proxy.Execute(fetchReq).Get();
    if (!fetchRsp->IsOK()) {
        THROW_ERROR_EXCEPTION("Error fetching table info")
            << fetchRsp->GetError();
    }

    auto inputChunks = FromProto<NChunkClient::NProto::TInputChunk>(fetchRsp->chunks());

    auto provider = New<TTableChunkReaderProvider>(
        inputChunks,
        Config,
        New<TChunkReaderOptions>());

    Reader = New<TTableChunkSequenceReader>(
        Config,
        MasterChannel,
        BlockCache,
        std::move(inputChunks),
        provider);
    Sync(~Reader, &TTableChunkSequenceReader::AsyncOpen);

    if (Transaction) {
        ListenTransaction(Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table reader opened");
}

const TRow* TTableReader::GetRow()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    if ((Reader->GetFacade() != nullptr) && IsReadingStarted) {
        if (!Reader->FetchNext()) {
            Sync(~Reader, &TTableChunkSequenceReader::GetReadyEvent);
        }
    }
    IsReadingStarted = true;

    auto* facade = Reader->GetFacade();
    return facade ? &(facade->GetRow()) : nullptr;
}

const TNonOwningKey& TTableReader::GetKey() const
{
    return Reader->GetFacade()->GetKey();
}

i64 TTableReader::GetRowIndex() const
{
    return Reader->GetProvider()->GetRowIndex();
}

i64 TTableReader::GetRowCount() const
{
    return Reader->GetProvider()->GetRowCount();
}

std::vector<NChunkClient::TChunkId> TTableReader::GetFailedChunks() const
{
    return Reader->GetFailedChunks();
}

const NYTree::TYsonString& TTableReader::GetRowAttributes() const
{
    return Reader->GetFacade()->GetRowAttributes();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
