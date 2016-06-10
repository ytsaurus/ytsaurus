#include "table_reader.h"
#include "private.h"
#include "transaction.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/multi_reader_base.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/ypath_proxy.h>

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
using namespace NApi;
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
        ITransactionPtr transaction,
        const TRichYPath& richPath,
        bool unordered);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TFuture<void> GetReadyEvent() override;

    virtual i64 GetTableRowIndex() const override;
    virtual TNameTablePtr GetNameTable() const override;
    virtual i64 GetTotalRowCount() const override;

    virtual TKeyColumns GetKeyColumns() const override;

    // not actually used
    virtual i64 GetSessionRowIndex() const override;
    virtual bool IsFetchingCompleted() const override;
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;
    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

private:
    const TTableReaderConfigPtr Config_;
    const TRemoteReaderOptionsPtr Options_;
    const IClientPtr Client_;
    const ITransactionPtr Transaction_;
    const TRichYPath RichPath_;

    const TTransactionId TransactionId_;
    const bool Unordered_;

    TFuture<void> ReadyEvent_;

    ISchemalessMultiChunkReaderPtr UnderlyingReader_;

    NLogging::TLogger Logger = ApiLogger;

    void DoOpen();
    void RemoveUnavailableChunks(std::vector<TChunkSpec>* chunkSpecs) const;
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableReader::TSchemalessTableReader(
    TTableReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    IClientPtr client,
    ITransactionPtr transaction,
    const TRichYPath& richPath,
    bool unordered)
    : Config_(config)
    , Options_(options)
    , Client_(client)
    , Transaction_(transaction)
    , RichPath_(richPath)
    , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
    , Unordered_(unordered)
{
    YCHECK(Config_);
    YCHECK(Client_);

    Logger.AddTag("Path: %v, TransactionId: %v",
        RichPath_.GetPath(),
        TransactionId_);

    ReadyEvent_ = BIND(&TSchemalessTableReader::DoOpen, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

void TSchemalessTableReader::DoOpen()
{
    const auto& path = RichPath_.GetPath();

    LOG_INFO("Opening table reader");

    TUserObject userObject;
    userObject.Path = path;

    GetUserObjectBasicAttributes(
        Client_, 
        TMutableRange<TUserObject>(&userObject, 1),
        Transaction_ ? Transaction_->GetId() : NullTransactionId,
        Logger,
        EPermission::Read,
        Config_->SuppressAccessTracking);

    const auto& objectId = userObject.ObjectId;
    const auto tableCellTag = userObject.CellTag;

    auto objectIdPath = FromObjectId(objectId);
    
    if (userObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            userObject.Type);
    }

    bool dynamic;
    TTableSchema schema;

    {
        LOG_INFO("Requesting table schema");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::LeaderOrFollower);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(objectIdPath);
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);
        std::vector<Stroka> attributeKeys{
            "dynamic",
            "schema"
        };
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting table schema %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        dynamic = attributes.Get<bool>("dynamic");

        if (dynamic) {
            schema = attributes.Get<TTableSchema>("schema");
            if (!schema.IsSorted()) {
                THROW_ERROR_EXCEPTION("Table is not sorted");
            }
        }
    }

    auto nodeDirectory = New<TNodeDirectory>();
    std::vector<TChunkSpec> chunkSpecs;

    {
        LOG_INFO("Fetching table chunks");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::LeaderOrFollower, tableCellTag);
        TObjectServiceProxy proxy(channel);

        auto req = TTableYPathProxy::Fetch(objectIdPath);
        InitializeFetchRequest(req.Get(), RichPath_);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching chunks for table %v",
            path);
        const auto& rsp = rspOrError.Value();

        ProcessFetchResponse(
            Client_,
            rsp,
            tableCellTag,
            nodeDirectory,
            Config_->MaxChunksPerLocateRequest,
            Logger,
            &chunkSpecs);

        RemoveUnavailableChunks(&chunkSpecs);
    }

    auto options = New<NTableClient::TTableReaderOptions>();
    options->EnableTableIndex = true;
    options->EnableRangeIndex = true;
    options->EnableRowIndex = true;

    if (dynamic) {
        UnderlyingReader_ = CreateSchemalessMergingMultiChunkReader(
            Config_,
            options,
            Client_,
            // HTTP proxy doesn't have a node descriptor.
            TNodeDescriptor(),
            Client_->GetConnection()->GetBlockCache(),
            nodeDirectory,
            std::move(chunkSpecs),
            New<TNameTable>(),
            TColumnFilter(),
            schema);
    } else {
        auto factory = Unordered_
            ? CreateSchemalessParallelMultiChunkReader
            : CreateSchemalessSequentialMultiChunkReader;
        UnderlyingReader_ = factory(
            Config_,
            options,
            Client_,
            // HTTP proxy doesn't have a node descriptor.
            TNodeDescriptor(),
            Client_->GetConnection()->GetBlockCache(),
            nodeDirectory,
            std::move(chunkSpecs),
            New<TNameTable>(),
            TColumnFilter(),
            TKeyColumns(),
            Null,
            NConcurrency::GetUnlimitedThrottler());
    }

    WaitFor(UnderlyingReader_->GetReadyEvent())
        .ThrowOnError();

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

    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return true;
    }

    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->Read(rows);
}

TFuture<void> TSchemalessTableReader::GetReadyEvent()
{
    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return ReadyEvent_;
    }

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

i64 TSchemalessTableReader::GetTotalRowCount() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetTotalRowCount();
}

TNameTablePtr TSchemalessTableReader::GetNameTable() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetNameTable();
}

TKeyColumns TSchemalessTableReader::GetKeyColumns() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetKeyColumns();
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

void TSchemalessTableReader::RemoveUnavailableChunks(std::vector<TChunkSpec>* chunkSpecs) const
{
    std::vector<TChunkSpec> availableChunkSpecs;

    for (auto& chunkSpec : *chunkSpecs) {
        if (IsUnavailable(chunkSpec)) {
            if (!Config_->IgnoreUnavailableChunks) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::ChunkUnavailable,
                    "Chunk %v is unavailable",
                    NYT::FromProto<TChunkId>(chunkSpec.chunk_id()));
            }
        } else {
            availableChunkSpecs.push_back(std::move(chunkSpec));
        }
    }

    *chunkSpecs = std::move(availableChunkSpecs);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<ISchemalessMultiChunkReaderPtr> CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options)
{
    ITransactionPtr transaction;

    if (options.TransactionId != NTransactionClient::NullTransactionId) {
        TTransactionAttachOptions transactionOptions;
        transactionOptions.Ping = options.Ping;
        transactionOptions.PingAncestors = options.PingAncestors;
        transaction = client->AttachTransaction(options.TransactionId, transactionOptions);
    }

    auto reader = New<TSchemalessTableReader>(
        options.Config ? options.Config : New<TTableReaderConfig>(),
        New<TRemoteReaderOptions>(),
        client,
        transaction,
        path,
        options.Unordered);

    return reader->GetReadyEvent().Apply(BIND([=] () -> ISchemalessMultiChunkReaderPtr {
        return reader;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

