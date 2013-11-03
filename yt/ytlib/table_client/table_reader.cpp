#include "stdafx.h"

#include "table_reader.h"
#include "config.h"
#include "table_chunk_reader.h"
#include "private.h"

#include <ytlib/actions/async_pipeline.h>

#include <ytlib/misc/sync.h>

#include <ytlib/ytree/ypath_proxy.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

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
    , RichPath(richPath.Simplify())
    , IsOpen(false)
    , IsReadStarted_(false)
    , ObjectProxy(masterChannel)
    , Logger(TableReaderLogger)
{
    YCHECK(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
        ~RichPath.GetPath(),
        ~ToString(TransactionId)));
}

TObjectServiceProxy::TInvExecuteBatch TAsyncTableReader::FetchTableInfo()
{
    LOG_INFO("Fetching table info");

    const auto& path = RichPath.GetPath();
    auto batchReq = ObjectProxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(path + "/@type");
        SetTransactionId(req, TransactionId);
        SetSuppressAccessTracking(req, Config->SuppressAccessTracking);
        batchReq->AddRequest(req, "get_type");
    }

    {
        auto req = TTableYPathProxy::Fetch(path);
        ToProto(req->mutable_attributes(), RichPath.Attributes());
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        SetTransactionId(req, TransactionId);
        SetSuppressAccessTracking(req, Config->SuppressAccessTracking);
        // ToDo(psushin): enable ignoring lost chunks.
        batchReq->AddRequest(req, "fetch");
    }

    return batchReq->Invoke();
}

TAsyncError TAsyncTableReader::OpenChunkReader(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error fetching table info");

    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_type");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting object type");

        auto type = ConvertTo<EObjectType>(TYsonString(rsp->value()));
        if (type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %s: expected %s, actual %s",
                ~RichPath.GetPath(),
                ~FormatEnum(EObjectType(EObjectType::Table)).Quote(),
                ~FormatEnum(type).Quote());
        }
    }

    {
        auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspFetch>("fetch");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching table chunks");

        NodeDirectory->MergeFrom(rsp->node_directory());
        auto chunkSpecs = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());

        auto provider = New<TTableChunkReaderProvider>(
            chunkSpecs,
            Config,
            New<TChunkReaderOptions>());

        Reader = New<TTableChunkSequenceReader>(
            Config,
            MasterChannel,
            BlockCache,
            NodeDirectory,
            std::move(chunkSpecs),
            provider);
        return Reader->AsyncOpen();
    }
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
    YCHECK(!IsOpen);

    LOG_INFO("Opening table reader");

    auto this_ = MakeStrong(this);
    return StartAsyncPipeline(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        ->Add(BIND(&TThis::FetchTableInfo, this_))
        ->Add(BIND(&TThis::OpenChunkReader, this_))
        ->Add(BIND(&TThis::OnChunkReaderOpened, this_))
        ->Run();
}

bool TAsyncTableReader::FetchNextItem()
{
    YCHECK(IsOpen);

    if (Reader->GetFacade()) {
        if (IsReadStarted_) {
            return Reader->FetchNext();
        }
        IsReadStarted_ = true;
        return true;
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

bool TAsyncTableReader::IsValid() const
{
    return Reader->GetFacade() != nullptr;
}

const TRow& TAsyncTableReader::GetRow() const
{
    return Reader->GetFacade()->GetRow();
}

i64 TAsyncTableReader::GetSessionRowIndex() const
{
    return Reader->GetProvider()->GetRowIndex();
}

i64 TAsyncTableReader::GetSessionRowCount() const
{
    return Reader->GetProvider()->GetRowCount();
}

i64 TAsyncTableReader::GetTableRowIndex() const
{
    return Reader->GetFacade()->GetTableRowIndex();
}

std::vector<NChunkClient::TChunkId> TAsyncTableReader::GetFailedChunkIds() const
{
    return Reader->GetFailedChunkIds();
}

int TAsyncTableReader::GetTableIndex() const
{
    return Reader->GetFacade()->GetTableIndex();
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
{ }

void TTableReader::Open()
{
    Sync(~AsyncReader_, &TAsyncTableReader::AsyncOpen);
}

const TRow* TTableReader::GetRow()
{
    if (AsyncReader_->IsValid() && !AsyncReader_->FetchNextItem()) {
        Sync(~AsyncReader_, &TAsyncTableReader::GetReadyEvent);
    }

    return AsyncReader_->IsValid() ? &(AsyncReader_->GetRow()) : nullptr;
}

i64 TTableReader::GetSessionRowIndex() const
{
    return AsyncReader_->GetSessionRowIndex();
}

i64 TTableReader::GetSessionRowCount() const
{
    return AsyncReader_->GetSessionRowCount();
}

i64 TTableReader::GetTableRowIndex() const
{
    return AsyncReader_->GetTableRowIndex();
}

std::vector<NChunkClient::TChunkId> TTableReader::GetFailedChunkIds() const
{
    return AsyncReader_->GetFailedChunkIds();
}

int TTableReader::GetTableIndex() const
{
    return AsyncReader_->GetTableIndex();
}

const TNonOwningKey& TTableReader::GetKey() const
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
