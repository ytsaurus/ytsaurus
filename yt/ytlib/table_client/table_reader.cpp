#include "stdafx.h"
#include "table_reader.h"
#include "config.h"
#include "table_chunk_reader.h"
#include "multi_chunk_sequential_reader.h"
#include "private.h"

#include <ytlib/actions/async_pipeline.h>
#include <ytlib/misc/sync.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/node_directory.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

namespace NYT {
namespace NTableClient {

using namespace NCypressClient;
using namespace NTableClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TFuture<TError> ConvertToTErrorFuture(TFuture<TValueOrError<void>> future)
{
    return future.Apply(BIND([](TValueOrError<void> error) -> TError {
        return error;
    }));
}

TAsyncTableReader::TAsyncTableReader(
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
    , NodeDirectory(New<TNodeDirectory>())
    , RichPath(richPath)
    , IsOpen(false)
    , Proxy(masterChannel)
    , Logger(TableReaderLogger)
{
    YCHECK(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
        ~RichPath.GetPath(),
        ~ToString(TransactionId)));
}

TFuture<TTableYPathProxy::TRspFetchPtr> TAsyncTableReader::FetchTableInfo()
{
    LOG_INFO("Fetching table info");

    auto fetchReq = TTableYPathProxy::Fetch(RichPath);
    SetTransactionId(fetchReq, TransactionId);
    fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
    // ToDo(psushin): enable ignoring lost chunks.

    return Proxy.Execute(fetchReq);
}

TAsyncError TAsyncTableReader::OpenChunkReader(TTableYPathProxy::TRspFetchPtr fetchRsp)
{
    THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching table info");

    NodeDirectory->MergeFrom(fetchRsp->node_directory());

    auto inputChunks = FromProto<NProto::TInputChunk>(fetchRsp->chunks());

    auto provider = New<TTableChunkReaderProvider>(Config);
    Reader = New<TTableChunkSequenceReader>(
        Config,
        MasterChannel,
        BlockCache,
        NodeDirectory,
        std::move(inputChunks),
        provider);
    return Reader->AsyncOpen();
}

void TAsyncTableReader::OnChunkReaderOpened()
{
    if (Transaction) {
        ListenTransaction(Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table reader opened");
}

TAsyncError TAsyncTableReader::AsyncOpen()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);

    LOG_INFO("Opening table reader");

    auto this_ = MakeStrong(this);
    return ConvertToTErrorFuture(
        StartAsyncPipeline(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            ->Add(BIND(&TThis::FetchTableInfo, this_))
            ->Add(BIND(&TThis::OpenChunkReader, this_))
            ->Add(BIND(&TThis::OnChunkReaderOpened, this_))
            ->Run());
}

bool TAsyncTableReader::FetchNextItem()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    if (Reader->IsValid()) {
        return Reader->FetchNextItem();
    }
    return false;
}

TAsyncError TAsyncTableReader::GetReadyEvent()
{
    if (IsAborted()) {
        return MakePromise<TError>(TError("Transaction aborted"));
    }
    return Reader->GetReadyEvent();
}

bool TAsyncTableReader::HasRow() const
{
    return Reader->IsValid();
}

const TRow& TAsyncTableReader::GetRow() const
{
    return Reader->CurrentReader()->GetRow();
}

const TNonOwningKey& TAsyncTableReader::GetKey() const
{
    YUNREACHABLE();
}

i64 TAsyncTableReader::GetRowIndex() const
{
    return Reader->GetItemIndex();
}

i64 TAsyncTableReader::GetRowCount() const
{
    return Reader->GetItemCount();
}

std::vector<NChunkClient::TChunkId> TAsyncTableReader::GetFailedChunks() const
{
    return Reader->GetFailedChunks();
}

const NYTree::TYsonString& TAsyncTableReader::GetRowAttributes() const
{
    return Reader->CurrentReader()->GetRowAttributes();
}

////////////////////////////////////////////////////////////////////////////////

TTableReader::TTableReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::ITransactionPtr transaction,
    NChunkClient::IBlockCachePtr blockCache,
    const NYPath::TRichYPath& richPath)
    : AsyncReader_(
        New<TAsyncTableReader>(
            config,
            masterChannel,
            transaction,
            blockCache,
            richPath))
    , IsReadStarted_(false)
{ }

void TTableReader::Open()
{
    Sync(~AsyncReader_, &TAsyncTableReader::AsyncOpen);
}

const TRow* TTableReader::GetRow()
{
    if (AsyncReader_->HasRow() && IsReadStarted_) {
        if (!AsyncReader_->FetchNextItem()) {
            Sync(~AsyncReader_, &TAsyncTableReader::GetReadyEvent);
        }
    }
    IsReadStarted_ = true;

    return AsyncReader_->HasRow() ? &(AsyncReader_->GetRow()) : nullptr;
}

const TNonOwningKey& TTableReader::GetKey() const
{
    YUNREACHABLE();
}

i64 TTableReader::GetRowIndex() const
{
    return AsyncReader_->GetRowIndex();
}

i64 TTableReader::GetRowCount() const
{
    return AsyncReader_->GetRowCount();
}

std::vector<NChunkClient::TChunkId> TTableReader::GetFailedChunks() const
{
    return AsyncReader_->GetFailedChunks();
}

const NYTree::TYsonString& TTableReader::GetRowAttributes() const
{
    return AsyncReader_->GetRowAttributes();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
