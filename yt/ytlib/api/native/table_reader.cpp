#include "table_reader.h"
#include "private.h"
#include "transaction.h"
#include "connection.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/multi_reader_base.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/api/table_reader.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/blob_table_writer.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/client/table_client/name_table.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/range.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NApi;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public ITableReader
    , public TTransactionListener
{
public:
    TTableReader(
        TTableReaderConfigPtr config,
        TTableReaderOptions options,
        IClientPtr client,
        NApi::ITransactionPtr transaction,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter,
        bool unordered,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , RichPath_(richPath)
        , NameTable_(std::move(nameTable))
        , ColumnFilter_(columnFilter)
        , BandwidthThrottler_(std::move(bandwidthThrottler))
        , RpsThrottler_(std::move(rpsThrottler))
        , TransactionId_(Transaction_ ? Transaction_->GetId() : NullTransactionId)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Client_);

        ReadyEvent_ = BIND(&TTableReader::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        if (NProfiling::GetCpuInstant() > ReadDeadline_) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::ReaderDeadlineExpired, "Reader deadline expired");
        }

        rows->clear();

        if (IsAborted()) {
            return true;
        }

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        YT_VERIFY(OpenResult_);
        return OpenResult_->Reader->Read(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return ReadyEvent_;
        }

        if (IsAborted()) {
            return MakeFuture(GetAbortError());
        }

        YT_VERIFY(OpenResult_);
        return OpenResult_->Reader->GetReadyEvent();
    }

    i64 GetStartRowIndex() const
    {
        YT_VERIFY(OpenResult_);
        return StartRowIndex_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        YT_VERIFY(OpenResult_);
        return OpenResult_->Reader->GetTotalRowCount();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YT_VERIFY(OpenResult_);
        return OpenResult_->Reader->GetDataStatistics();
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        YT_VERIFY(OpenResult_);
        return OpenResult_->Reader->GetNameTable();
    }

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        YT_VERIFY(OpenResult_);
        return OpenResult_->Reader->GetKeyColumns();
    }

    virtual const TTableSchema& GetTableSchema() const override
    {
        return OpenResult_->TableSchema;
    }

    virtual const std::vector<TString>& GetOmittedInaccessibleColumns() const override
    {
        YT_VERIFY(OpenResult_);
        return OpenResult_->OmittedInaccessibleColumns;
    }

private:
    const TTableReaderConfigPtr Config_;
    TTableReaderOptions Options_;
    const IClientPtr Client_;
    const NApi::ITransactionPtr Transaction_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const TColumnFilter ColumnFilter_;
    const IThroughputThrottlerPtr BandwidthThrottler_;
    const IThroughputThrottlerPtr RpsThrottler_;
    const TTransactionId TransactionId_;
    const TNodeDirectoryPtr NodeDirectory_;

    TFuture<void> ReadyEvent_;
    std::optional<TSchemalessMultiChunkReaderCreateResult> OpenResult_;
    i64 StartRowIndex_;
    NProfiling::TCpuInstant ReadDeadline_ = Max<NProfiling::TCpuInstant>();

    void DoOpen()
    {
        OpenResult_ = WaitFor(CreateSchemalessMultiChunkReader(
            Client_,
            RichPath_,
            Options_,
            NameTable_,
            ColumnFilter_,
            BandwidthThrottler_,
            RpsThrottler_))
            .ValueOrThrow();

        StartRowIndex_ = OpenResult_->Reader->GetTableRowIndex();

        if (Transaction_) {
            StartListenTransaction(Transaction_);
        }

        if (Config_->MaxReadDuration) {
            ReadDeadline_ = NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(*Config_->MaxReadDuration);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options,
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    NApi::ITransactionPtr transaction;
    if (options.TransactionId) {
        TTransactionAttachOptions transactionOptions;
        transactionOptions.Ping = options.Ping;
        transactionOptions.PingAncestors = options.PingAncestors;
        transaction = client->AttachTransaction(options.TransactionId, transactionOptions);
    }

    auto reader = New<TTableReader>(
        options.Config ? options.Config : New<TTableReaderConfig>(),
        options,
        client,
        transaction,
        path,
        nameTable,
        columnFilter,
        options.Unordered,
        bandwidthThrottler,
        rpsThrottler);

    return reader->GetReadyEvent().Apply(BIND([=] () -> ITableReaderPtr {
        return reader;
    }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TSchemalessMultiChunkReaderCreateResult> CreateSchemalessMultiChunkReader(
    const IClientPtr& client,
    const NYPath::TRichYPath& richPath,
    const TTableReaderOptions& options,
    const TNameTablePtr& nameTable,
    const TColumnFilter& columnFilter,
    const IThroughputThrottlerPtr& bandwidthThrottler,
    const IThroughputThrottlerPtr& rpsThrottler)
{
    const auto& path = richPath.GetPath();
    auto readSessionId = TReadSessionId::Create();

    auto Logger = NLogging::TLogger(ApiLogger)
        .AddTag("Path: %v, TransactionId: %v, ReadSessionId: %v",
            path,
            options.TransactionId,
            readSessionId);

    YT_LOG_INFO("Opening table reader");

    auto userObject = std::make_unique<TUserObject>(richPath);

    auto config = options.Config ? options.Config : New<TTableReaderConfig>();

    GetUserObjectBasicAttributes(
        client,
        {userObject.get()},
        options.TransactionId,
        Logger,
        EPermission::Read,
        TGetUserObjectBasicAttributesOptions{
            .SuppressAccessTracking = config->SuppressAccessTracking,
            .OmitInaccessibleColumns = options.OmitInaccessibleColumns
        });

    if (userObject->ObjectId) {
        if (userObject->Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                userObject->Type);
        }
    } else {
        YT_LOG_INFO("Table is virtual");
    }

    int chunkCount;
    bool dynamic;
    TTableSchema schema;
    {
        YT_LOG_INFO("Requesting extended table attributes");

        auto channel = client->GetMasterChannelOrThrow(
            EMasterChannelKind::Follower,
            userObject->ExternalCellTag);

        TObjectServiceProxy proxy(channel);

        // NB: objectId is null for virtual tables.
        auto req = TYPathProxy::Get(userObject->GetObjectIdPathIfAvailable() + "/@");
        if (userObject->ObjectId) {
            AddCellTagToSyncWith(req, userObject->ObjectId);
        }
        SetTransactionId(req, userObject->ExternalTransactionId);
        SetSuppressAccessTracking(req, config->SuppressAccessTracking);
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "chunk_count",
            "dynamic",
            "retained_timestamp",
            "schema",
            "unflushed_timestamp"
        });

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes of table %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
        dynamic = attributes->Get<bool>("dynamic");
        schema = attributes->Get<TTableSchema>("schema");

        ValidateDynamicTableTimestamp(richPath, dynamic, schema, *attributes);
    }

    std::vector<TChunkSpec> chunkSpecs;

    {
        YT_LOG_INFO("Fetching table chunks (ChunkCount: %v)",
            chunkCount);

        chunkSpecs = FetchChunkSpecs(
            client,
            client->GetNativeConnection()->GetNodeDirectory(),
            *userObject,
            richPath.GetRanges(),
            // XXX(babenko): YT-11825
            dynamic && !schema.IsSorted() ? -1 : chunkCount,
            config->MaxChunksPerFetch,
            config->MaxChunksPerLocateRequest,
            [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                req->set_fetch_parity_replicas(config->EnableAutoRepair);
                AddCellTagToSyncWith(req, userObject->ObjectId);
                SetTransactionId(req, userObject->ExternalTransactionId);
                SetSuppressAccessTracking(req, config->SuppressAccessTracking);
            },
            Logger,
            /* skipUnavailableChunks */ config->UnavailableChunkStrategy == EUnavailableChunkStrategy::Skip);

        CheckUnavailableChunks(config->UnavailableChunkStrategy, &chunkSpecs);
    }

    auto internalOptions = New<NTableClient::TTableReaderOptions>();
    internalOptions->EnableTableIndex = options.EnableTableIndex;
    internalOptions->EnableRangeIndex = options.EnableRangeIndex;
    internalOptions->EnableRowIndex = options.EnableRowIndex;
    internalOptions->EnableTabletIndex = options.EnableTabletIndex;

    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.WorkloadDescriptor = config->WorkloadDescriptor;
    blockReadOptions.WorkloadDescriptor.Annotations.push_back(Format("TablePath: %v", path));
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    blockReadOptions.ReadSessionId = readSessionId;

    ISchemalessMultiChunkReaderPtr reader;

    auto dataSourceDirectory = New<NChunkClient::TDataSourceDirectory>();
    if (dynamic && schema.IsSorted()) {
        dataSourceDirectory->DataSources().push_back(MakeVersionedDataSource(
            path,
            schema,
            richPath.GetColumns(),
            userObject->OmittedInaccessibleColumns,
            richPath.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
            richPath.GetRetentionTimestamp().value_or(NullTimestamp)));

        TDataSliceDescriptor dataSliceDescriptor(std::move(chunkSpecs));

        const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];
        auto adjustedColumnFilter = columnFilter.IsUniversal()
            ? CreateColumnFilter(dataSource.Columns(), nameTable)
            : columnFilter;

        reader = CreateSchemalessMergingMultiChunkReader(
            config,
            internalOptions,
            client,
            /* localDescriptor */ {},
            /* partitionTag */ std::nullopt,
            client->GetNativeConnection()->GetBlockCache(),
            client->GetNativeConnection()->GetNodeDirectory(),
            dataSourceDirectory,
            dataSliceDescriptor,
            nameTable,
            blockReadOptions,
            adjustedColumnFilter,
            /* trafficMeter */ nullptr,
            bandwidthThrottler,
            rpsThrottler);
    } else {
        dataSourceDirectory->DataSources().push_back(MakeUnversionedDataSource(
            path,
            schema,
            richPath.GetColumns(),
            userObject->OmittedInaccessibleColumns));

        std::vector<TDataSliceDescriptor> dataSliceDescriptors;
        for (const auto& chunkSpec : chunkSpecs) {
            dataSliceDescriptors.emplace_back(chunkSpec);
        }

        auto factory = options.Unordered
            ? CreateSchemalessParallelMultiReader
            : CreateSchemalessSequentialMultiReader;
        reader = factory(
            config,
            internalOptions,
            client,
            // Client doesn't have a node descriptor.
            /* localDescriptor */ {},
            std::nullopt,
            client->GetNativeConnection()->GetBlockCache(),
            client->GetNativeConnection()->GetNodeDirectory(),
            dataSourceDirectory,
            std::move(dataSliceDescriptors),
            nameTable,
            blockReadOptions,
            columnFilter,
            schema.GetKeyColumns(),
            /* partitionTag */ std::nullopt,
            /* trafficMeter */ nullptr,
            bandwidthThrottler,
            rpsThrottler,
            /* multiReaderMemoryManager */ nullptr);
    }

    return reader->GetReadyEvent()
        .Apply(BIND([=, userObject = std::move(userObject)] {
            return TSchemalessMultiChunkReaderCreateResult{
                reader,
                userObject->OmittedInaccessibleColumns,
                schema
            };
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

