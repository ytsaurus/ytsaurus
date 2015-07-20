#include "stdafx.h"
#include "table_reader.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/transaction_client/helpers.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/table_ypath_proxy.h>
#include <ytlib/new_table_client/name_table.h>

#include <ytlib/ypath/rich.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/throughput_throttler.h>

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/ypath_proxy.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NApi {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableReader
    : public ISchemalessMultiChunkReader
    , public TTransactionListener
{
public:
    TSchemalessTableReader(
        TTableReaderConfigPtr config,
        TRemoteReaderOptionsPtr options,
        IChannelPtr masterChannel,
        TTransactionPtr transaction,
        IBlockCachePtr blockCache,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        IThroughputThrottlerPtr throttler);

    virtual TFuture<void> Open() override;
    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TFuture<void> GetReadyEvent() override;

    virtual i64 GetTableRowIndex() const override;
    virtual i32 GetRangeIndex() const override;
    virtual TNameTablePtr GetNameTable() const override;
    virtual i64 GetTotalRowCount() const override;

    virtual TKeyColumns GetKeyColumns() const override;

    // not actually used
    virtual int GetTableIndex() const override;
    virtual i64 GetSessionRowIndex() const override;
    virtual bool IsFetchingCompleted() const override;
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;
    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

private:
    const TTableReaderConfigPtr Config_;
    const TRemoteReaderOptionsPtr Options_;
    const IChannelPtr MasterChannel_;
    const TTransactionPtr Transaction_;
    const IBlockCachePtr BlockCache_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const IThroughputThrottlerPtr Throttler_;

    const TTransactionId TransactionId_;

    ISchemalessMultiChunkReaderPtr UnderlyingReader_;

    NLogging::TLogger Logger = ApiLogger;

    void DoOpen();

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableReader::TSchemalessTableReader(
    TTableReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    IChannelPtr masterChannel,
    TTransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    IThroughputThrottlerPtr throttler)
    : Config_(config)
    , Options_(options)
    , MasterChannel_(masterChannel)
    , Transaction_(transaction)
    , BlockCache_(blockCache)
    , RichPath_(richPath)
    , NameTable_(nameTable)
    , Throttler_(throttler)
    , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
{
    YCHECK(Config_);
    YCHECK(MasterChannel_);
    YCHECK(BlockCache_);
    YCHECK(NameTable_);

    Logger.AddTag("Path: %v, TransactionId: %v",
        RichPath_.GetPath(),
        TransactionId_);
}

TFuture<void> TSchemalessTableReader::Open()
{
    return BIND(&TSchemalessTableReader::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

void TSchemalessTableReader::DoOpen()
{
    const auto& path = RichPath_.GetPath();

    LOG_INFO("Opening table reader");

    TObjectServiceProxy objectProxy(MasterChannel_);

    auto batchReq = objectProxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(path + "/@type");
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);
        batchReq->AddRequest(req, "get_type");
    }

    {
        auto req = TTableYPathProxy::Fetch(path);
        InitializeFetchRequest(req.Get(), RichPath_);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);
        batchReq->AddRequest(req, "fetch");
    }

    LOG_INFO("Fetching table info");
    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error fetching table info");

    auto batchRsp = batchRspOrError.Value();
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_type");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting object type");
        auto rsp = rspOrError.Value();
        
        auto type = ConvertTo<EObjectType>(TYsonString(rsp->value()));
        if (type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                type);
        }
    }

    {
        auto rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspFetch>("fetch");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching table chunks");
        auto rsp = rspOrError.Value();

        auto nodeDirectory = New<TNodeDirectory>();
        nodeDirectory->MergeFrom(rsp->node_directory());

        std::vector<TChunkSpec> chunkSpecs;
        for (const auto& chunkSpec : rsp->chunks()) {
            if (!IsUnavailable(chunkSpec)) {
                chunkSpecs.push_back(chunkSpec);
                continue;
            }
             
            if (Config_->IgnoreUnavailableChunks) {
                continue;
            }
             
            THROW_ERROR_EXCEPTION("Chunk %v is unavailable",
                NYT::FromProto<TChunkId>(chunkSpec.chunk_id()));
        }

        auto options = New<TMultiChunkReaderOptions>();
        options->NetworkName = Options_->NetworkName;

        UnderlyingReader_ = CreateSchemalessSequentialMultiChunkReader(
            Config_,
            options,
            MasterChannel_,
            BlockCache_,
            nodeDirectory,
            chunkSpecs,
            NameTable_,
            TColumnFilter(),
            TKeyColumns(),
            Throttler_);

        WaitFor(UnderlyingReader_->Open())
            .ThrowOnError();
    }

    if (Transaction_) {
        ListenTransaction(Transaction_);
    }

    LOG_INFO("Table reader opened");
}

bool TSchemalessTableReader::Read(std::vector<TUnversionedRow> *rows)
{
    if (IsAborted()) {
        return true;
    }

    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->Read(rows);
}

TFuture<void> TSchemalessTableReader::GetReadyEvent()
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction %v aborted",
            TransactionId_));
    }

    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetReadyEvent();
}

i64 TSchemalessTableReader::GetTableRowIndex() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetTableRowIndex();
}

i32 TSchemalessTableReader::GetRangeIndex() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetRangeIndex();
}

i64 TSchemalessTableReader::GetTotalRowCount() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetTotalRowCount();
}

TNameTablePtr TSchemalessTableReader::GetNameTable() const
{
    return NameTable_;
}

TKeyColumns TSchemalessTableReader::GetKeyColumns() const
{
    return TKeyColumns();
}

int TSchemalessTableReader::GetTableIndex() const
{
    return 0;
}

i64 TSchemalessTableReader::GetSessionRowIndex() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetSessionRowIndex();
}

bool TSchemalessTableReader::IsFetchingCompleted() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->IsFetchingCompleted();
}

NChunkClient::NProto::TDataStatistics TSchemalessTableReader::GetDataStatistics() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetDataStatistics();
}

std::vector<TChunkId> TSchemalessTableReader::GetFailedChunkIds() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetFailedChunkIds();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options)
{
    NTransactionClient::TTransactionPtr transaction;

    if (options.TransactionId != NTransactionClient::NullTransactionId) {
        NTransactionClient::TTransactionAttachOptions transactionOptions;
        transactionOptions.Ping = options.Ping;
        transactionOptions.PingAncestors = options.PingAncestors;

        transaction = client->GetTransactionManager()->Attach(
            options.TransactionId,
            transactionOptions);
    }

    return New<TSchemalessTableReader>(
        options.Config,
        options.RemoteReaderOptions,
        client->GetMasterChannel(EMasterChannelKind::LeaderOrFollower),
        transaction,
        client->GetConnection()->GetBlockCache(),
        path,
        New<TNameTable>(),
        NConcurrency::GetUnlimitedThrottler());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

