#include "stdafx.h"
#include "table_reader.h"
#include "config.h"
#include "table_chunk_reader.h"
#include "private.h"

#include <core/misc/sync.h>

#include <core/concurrency/fiber.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NTableClient {

using namespace NCypressClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

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
    , Proxy(masterChannel)
    , Logger(TableReaderLogger)
{
    YCHECK(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
        ~RichPath.GetPath(),
        ~ToString(TransactionId)));
}

void TAsyncTableReader::Open()
{
    YCHECK(!IsOpen);

    LOG_INFO("Opening table reader");

    auto fetchReq = TTableYPathProxy::Fetch(RichPath.GetPath());
    ToProto(fetchReq->mutable_attributes(), RichPath.Attributes());
    fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
    SetTransactionId(fetchReq, TransactionId);
    SetSuppressAccessTracking(fetchReq, Config->SuppressAccessTracking);
    // ToDo(psushin): enable ignoring lost chunks.

    auto fetchRsp = WaitFor(Proxy.Execute(fetchReq));

    THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching table info");

    NodeDirectory->MergeFrom(fetchRsp->node_directory());
    auto chunkSpecs = FromProto<NChunkClient::NProto::TChunkSpec>(fetchRsp->chunks());

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

    auto error = WaitFor(Reader->AsyncOpen());

    THROW_ERROR_EXCEPTION_IF_FAILED(error);

    if (Transaction) {
        ListenTransaction(Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table reader opened");
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

NChunkClient::NProto::TDataStatistics TAsyncTableReader::GetDataStatistics() const
{
    return Reader->GetProvider()->GetDataStatistics();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
