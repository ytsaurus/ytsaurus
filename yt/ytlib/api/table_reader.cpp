#include "stdafx.h"
#include "table_reader.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>
#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/transaction_client/helpers.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/table_client/schemaless_chunk_reader.h>
#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/name_table.h>

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
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
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
        IClientPtr client,
        TTransactionPtr transaction,
        IBlockCachePtr blockCache,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        IThroughputThrottlerPtr throttler,
        bool unordered);

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
    const IClientPtr Client_;
    const TTransactionPtr Transaction_;
    const IBlockCachePtr BlockCache_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const IThroughputThrottlerPtr Throttler_;

    const TTransactionId TransactionId_;
    const bool Unordered_;

    ISchemalessMultiChunkReaderPtr UnderlyingReader_;

    NLogging::TLogger Logger = ApiLogger;

    void DoOpen();

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableReader::TSchemalessTableReader(
    TTableReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    IClientPtr client,
    TTransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    IThroughputThrottlerPtr throttler,
    bool unordered)
    : Config_(config)
    , Options_(options)
    , Client_(client)
    , Transaction_(transaction)
    , BlockCache_(blockCache)
    , RichPath_(richPath)
    , NameTable_(nameTable)
    , Throttler_(throttler)
    , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
    , Unordered_(unordered)
{
    YCHECK(Config_);
    YCHECK(Client_);
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

    auto tableCellTag = InvalidCellTag;
    TObjectId objectId;

    {
        LOG_INFO("Requesting basic attributes");

        auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
        TObjectServiceProxy proxy(channel);

        auto req = TTableYPathProxy::GetBasicAttributes(path);
        req->set_permissions(static_cast<ui32>(EPermission::Read));
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes for table %v",
            path);

        const auto& rsp = rspOrError.Value();

        objectId = FromProto<TObjectId>(rsp->object_id());
        tableCellTag = rsp->cell_tag();

        LOG_INFO("Basic attributes received (ObjectId: %v, CellTag: %v)",
            objectId,
            tableCellTag);
    }

    auto objectIdPath = FromObjectId(objectId);

    {
        auto type = TypeFromId(objectId);
        if (type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                type);
        }
    }

    auto nodeDirectory = New<TNodeDirectory>();
    std::vector<TChunkSpec> chunkSpecs;

    {
        LOG_INFO("Fetching table chunks");

        auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, tableCellTag);
        TObjectServiceProxy proxy(channel);

        auto req = TTableYPathProxy::Fetch(objectIdPath);
        InitializeFetchRequest(req.Get(), RichPath_);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching chunks for table %v",
            path);
        const auto& rsp = rspOrError.Value();

        nodeDirectory->MergeFrom(rsp->node_directory());

        for (auto& chunkSpec : *rsp->mutable_chunks()) {
            chunkSpecs.push_back(TChunkSpec());
            chunkSpecs.back().Swap(&chunkSpec);
        }
    }

    yhash_map<TCellTag, std::vector<TChunkSpec*>> foreignChunkMap;
    for (auto& chunkSpec : chunkSpecs) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto chunkCellTag = CellTagFromId(chunkId);
        if (chunkCellTag != tableCellTag) {
            foreignChunkMap[chunkCellTag].push_back(&chunkSpec);
        }
    }

    for (const auto& pair : foreignChunkMap) {
        auto cellTag = pair.first;
        auto& chunkSpecs = pair.second;

        auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, cellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < chunkSpecs.size(); beginIndex += Config_->MaxChunksPerLocateRequest) {
            int endIndex = std::min(
                beginIndex + Config_->MaxChunksPerLocateRequest,
                static_cast<int>(chunkSpecs.size()));

            auto req = proxy.LocateChunks();
            for (int index = beginIndex; index < endIndex; ++index) {
                req->add_chunk_ids()->CopyFrom(chunkSpecs[index]->chunk_id());
            }

            LOG_INFO("Locating foreign chunks (CellTag: %v, ChunkCount: %v)",
                cellTag,
                req->chunk_ids_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locating foreign chunks at cell %v",
                cellTag);
            const auto& rsp = rspOrError.Value();

            nodeDirectory->MergeFrom(rsp->node_directory());

            for (int index = beginIndex; index < endIndex; ++index) {
                int rspIndex = index - beginIndex;
                auto expectedChunkId = FromProto<TChunkId>(chunkSpecs[index]->chunk_id());
                auto actualChunkId = rspIndex < rsp->chunks_size()
                    ? FromProto<TChunkId>(rsp->chunks(rspIndex).chunk_id())
                    : NullChunkId;
                if (expectedChunkId != actualChunkId) {
                    THROW_ERROR_EXCEPTION(
                        NChunkClient::EErrorCode::NoSuchChunk,
                        "No such chunk %v",
                        expectedChunkId);
                }
                chunkSpecs[index]->mutable_replicas()->Swap(rsp->mutable_chunks(rspIndex)->mutable_replicas());
            }
        }
    }

    if (!Config_->IgnoreUnavailableChunks) {
        for (const auto& chunkSpec : chunkSpecs) {
            if (IsUnavailable(chunkSpec)) {
                THROW_ERROR_EXCEPTION("Chunk %v is unavailable",
                    NYT::FromProto<TChunkId>(chunkSpec.chunk_id()));
            }
        }
    }

    {
        auto factory = Unordered_
            ? CreateSchemalessParallelMultiChunkReader
            : CreateSchemalessSequentialMultiChunkReader;
        UnderlyingReader_ = factory(
            Config_,
            New<TMultiChunkReaderOptions>(),
            Client_,
            Client_->GetConnection()->GetBlockCache(),
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
        options.Config ? options.Config : New<TTableReaderConfig>(),
        options.RemoteReaderOptions ? options.RemoteReaderOptions : New<TRemoteReaderOptions>(),
        client,
        transaction,
        client->GetConnection()->GetBlockCache(),
        path,
        New<TNameTable>(),
        NConcurrency::GetUnlimitedThrottler(),
        options.Unordered);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

