#include "client_impl.h"
#include "box.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"
#include "file_reader.h"
#include "file_writer.h"
#include "journal_reader.h"
#include "journal_writer.h"
#include "table_reader.h"
#include "table_writer.h"
#include "skynet.h"
#include "private.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/operation_archive_schema.h>
#include <yt/client/api/file_reader.h>
#include <yt/client/api/file_writer.h>
#include <yt/client/api/journal_reader.h>
#include <yt/client/api/journal_writer.h>

#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/security_client/helpers.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/wire_protocol.h>
#include <yt/client/table_client/proto/wire_protocol.pb.h>

#include <yt/client/tablet_client/public.h>
#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/api/native/tablet_helpers.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/ytlib/chunk_client/medium_directory.pb.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>
#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/job_proxy/job_spec_helper.h>
#include <yt/ytlib/job_proxy/helpers.h>
#include <yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/ytlib/job_tracker_client/helpers.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/query_client/executor.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions_cache.h>
#include <yt/ytlib/query_client/helpers.h>
#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>
#include <yt/client/scheduler/operation_id_or_alias.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>
#include <yt/ytlib/security_client/helpers.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/schema_inferer.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/row_merger.h>
#include <yt/ytlib/table_client/columnar_statistics_fetcher.h>

#include <yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/ytlib/tablet_client/tablet_cell_bundle_ypath_proxy.h>

#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/async_stream_pipe.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/helpers.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/ypath_proxy.h>
#include <yt/core/ytree/ypath_resolver.h>

#include <yt/core/misc/collection_helpers.h>

#include <util/string/join.h>

namespace NYT::NApi::NNative {

using namespace NCrypto;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NSecurityClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler;
using namespace NHiveClient;
using namespace NHydra;
using namespace NJobTrackerClient;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::TDataSliceDescriptor;
using NNodeTrackerClient::CreateNodeChannelFactory;
using NNodeTrackerClient::INodeChannelFactoryPtr;
using NNodeTrackerClient::TNetworkPreferenceList;
using NNodeTrackerClient::TNodeDescriptor;
using NTableClient::TColumnSchema;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobInputReader)
DECLARE_REFCOUNTED_CLASS(TTransaction)

////////////////////////////////////////////////////////////////////////////////

namespace {

TUnversionedOwningRow CreateJobKey(TJobId jobId, const TNameTablePtr& nameTable)
{
    TOwningRowBuilder keyBuilder(2);

    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[0], nameTable->GetIdOrRegisterName("job_id_hi")));
    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[1], nameTable->GetIdOrRegisterName("job_id_lo")));

    return keyBuilder.FinishRow();
}

TUnversionedRow CreateOperationKey(const TOperationId& operationId, const TOrderedByIdTableDescriptor::TIndex& Index, const TRowBufferPtr& rowBuffer)
{
    auto key = rowBuffer->AllocateUnversioned(2);
    key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], Index.IdHi);
    key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], Index.IdLo);
    return key;
}

TYsonString GetLatestProgress(const TYsonString& cypressProgress, const TYsonString& archiveProgress)
{
    auto getBuildTime = [&] (const TYsonString& progress) {
        if (!progress) {
            return TInstant();
        }
        auto maybeTimeString = NYTree::TryGetString(progress.GetData(), "/build_time");
        if (!maybeTimeString) {
            return TInstant();
        }
        return ConvertTo<TInstant>(*maybeTimeString);
    };

    return getBuildTime(cypressProgress) > getBuildTime(archiveProgress)
        ? cypressProgress
        : archiveProgress;
}

TYsonString GetValueIgnoringTimeout(const TErrorOr<TYsonString>& resultOrError)
{
    if (resultOrError.GetCode() != NYT::EErrorCode::Timeout) {
        resultOrError.ThrowOnError();
    }
    return resultOrError.IsOK()
        ? resultOrError.Value()
        : TYsonString();
}

template <class TReq>
void SetDynamicTableCypressRequestFullPath(TReq* req, const TYPath& fullPath)
{ }

template <>
void SetDynamicTableCypressRequestFullPath<NTabletClient::NProto::TReqMount>(
    NTabletClient::NProto::TReqMount* req,
    const TYPath& fullPath)
{
    req->set_path(fullPath);
}

constexpr i64 ListJobsFromArchiveInProgressJobLimit = 100000;

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TClient::TCountingFilter
{
    THashMap<TString, i64> PoolCounts;
    THashMap<TString, i64> UserCounts;
    TEnumIndexedVector<i64, NScheduler::EOperationState> StateCounts;
    TEnumIndexedVector<i64, NScheduler::EOperationType> TypeCounts;
    i64 FailedJobsCount = 0;

    const TListOperationsOptions& Options;

    explicit TCountingFilter(const TListOperationsOptions& options)
        : Options(options)
    { }

    bool Filter(
        std::optional<std::vector<TString>> pools,
        TStringBuf user,
        const EOperationState& state,
        const NScheduler::EOperationType& type,
        i64 count)
    {
        UserCounts[user] += count;

        if (Options.UserFilter && *Options.UserFilter != user) {
            return false;
        }

        if (pools) {
            for (const auto& pool : *pools) {
                PoolCounts[pool] += count;
            }
        }

        if (Options.Pool && (!pools || std::find(pools->begin(), pools->end(), *Options.Pool) == pools->end())) {
            return false;
        }

        StateCounts[state] += count;

        if (Options.StateFilter && *Options.StateFilter != state) {
            return false;
        }

        TypeCounts[type] += count;

        if (Options.TypeFilter && *Options.TypeFilter != type) {
            return false;
        }

        return true;
    }

    bool FilterByFailedJobs(const NYson::TYsonString& briefProgress)
    {
        bool hasFailedJobs = false;
        if (briefProgress) {
            auto briefProgressMapNode = ConvertToNode(briefProgress)->AsMap();
            auto jobsNode = briefProgressMapNode->FindChild("jobs");
            hasFailedJobs = jobsNode && jobsNode->AsMap()->GetChild("failed")->GetValue<i64>() > 0;
        }
        FailedJobsCount += hasFailedJobs;
        return !Options.WithFailedJobs || (*Options.WithFailedJobs == hasFailedJobs);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClient::TTabletCellLookupSession
    : public TIntrinsicRefCounted
{
public:
    using TEncoder = std::function<std::vector<TSharedRef>(const std::vector<NTableClient::TUnversionedRow>&)>;
    using TDecoder = std::function<NTableClient::TTypeErasedRow(NTableClient::TWireProtocolReader*)>;

    TTabletCellLookupSession(
        TConnectionConfigPtr config,
        const TNetworkPreferenceList& networks,
        NObjectClient::TCellId cellId,
        const TLookupRowsOptionsBase& options,
        NTabletClient::TTableMountInfoPtr tableInfo,
        const std::optional<TString>& retentionConfig,
        TEncoder encoder,
        TDecoder decoder)
        : Config_(std::move(config))
        , Networks_(networks)
        , CellId_(cellId)
        , Options_(options)
        , TableInfo_(std::move(tableInfo))
        , RetentionConfig_(retentionConfig)
        , Encoder_(std::move(encoder))
        , Decoder_(std::move(decoder))
    { }

    void AddKey(int index, TTabletInfoPtr tabletInfo, NTableClient::TKey key)
    {
        if (Batches_.empty() ||
            Batches_.back()->TabletInfo->TabletId != tabletInfo->TabletId ||
            Batches_.back()->Indexes.size() >= Config_->MaxRowsPerLookupRequest)
        {
            Batches_.emplace_back(new TBatch(std::move(tabletInfo)));
        }

        auto& batch = Batches_.back();
        batch->Indexes.push_back(index);
        batch->Keys.push_back(key);
    }

    TFuture<void> Invoke(IChannelFactoryPtr channelFactory, TCellDirectoryPtr cellDirectory)
    {
        auto* codec = NCompression::GetCodec(Config_->LookupRowsRequestCodec);

        // Do all the heavy lifting here.
        for (auto& batch : Batches_) {
            batch->RequestData = codec->Compress(Encoder_(batch->Keys));
        }

        const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(CellId_);
        auto channel = CreateTabletReadChannel(
            channelFactory,
            cellDescriptor,
            Options_,
            Networks_);

        InvokeProxy_ = std::make_unique<TQueryServiceProxy>(std::move(channel));
        InvokeProxy_->SetDefaultTimeout(Options_.Timeout.value_or(Config_->DefaultLookupRowsTimeout));
        InvokeProxy_->SetDefaultRequestAck(false);

        InvokeNextBatch();
        return InvokePromise_;
    }

    void ParseResponse(
        const TRowBufferPtr& rowBuffer,
        std::vector<NTableClient::TTypeErasedRow>* resultRows)
    {
        auto* responseCodec = NCompression::GetCodec(Config_->LookupRowsResponseCodec);
        for (const auto& batch : Batches_) {
            auto responseData = responseCodec->Decompress(batch->Response->Attachments()[0]);
            NTableClient::TWireProtocolReader reader(responseData, rowBuffer);
            auto batchSize = batch->Keys.size();
            for (int index = 0; index < batchSize; ++index) {
                (*resultRows)[batch->Indexes[index]] = Decoder_(&reader);
            }
        }
    }

private:
    const TConnectionConfigPtr Config_;
    const TNetworkPreferenceList Networks_;
    const NObjectClient::TCellId CellId_;
    const TLookupRowsOptionsBase Options_;
    const NTabletClient::TTableMountInfoPtr TableInfo_;
    const std::optional<TString> RetentionConfig_;
    const TEncoder Encoder_;
    const TDecoder Decoder_;

    struct TBatch
    {
        explicit TBatch(TTabletInfoPtr tabletInfo)
            : TabletInfo(std::move(tabletInfo))
        { }

        TTabletInfoPtr TabletInfo;
        std::vector<int> Indexes;
        std::vector<NTableClient::TKey> Keys;
        TSharedRef RequestData;
        TQueryServiceProxy::TRspReadPtr Response;
    };

    std::vector<std::unique_ptr<TBatch>> Batches_;
    std::unique_ptr<TQueryServiceProxy> InvokeProxy_;
    int InvokeBatchIndex_ = 0;
    TPromise<void> InvokePromise_ = NewPromise<void>();


    void InvokeNextBatch()
    {
        if (InvokeBatchIndex_ >= Batches_.size()) {
            InvokePromise_.Set(TError());
            return;
        }

        const auto& batch = Batches_[InvokeBatchIndex_];

        auto req = InvokeProxy_->Read();
        // TODO(babenko): set proper band
        ToProto(req->mutable_tablet_id(), batch->TabletInfo->TabletId);
        req->set_mount_revision(batch->TabletInfo->MountRevision);
        req->set_timestamp(Options_.Timestamp);
        req->set_request_codec(static_cast<int>(Config_->LookupRowsRequestCodec));
        req->set_response_codec(static_cast<int>(Config_->LookupRowsResponseCodec));
        req->Attachments().push_back(batch->RequestData);
        if (batch->TabletInfo->IsInMemory()) {
            req->Header().set_uncancelable(true);
        }
        if (RetentionConfig_) {
            req->set_retention_config(*RetentionConfig_);
        }

        req->Invoke().Subscribe(
            BIND(&TTabletCellLookupSession::OnResponse, MakeStrong(this)));
    }

    void OnResponse(const TQueryServiceProxy::TErrorOrRspReadPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            Batches_[InvokeBatchIndex_]->Response = rspOrError.Value();
            ++InvokeBatchIndex_;
            InvokeNextBatch();
        } else {
            InvokePromise_.Set(rspOrError);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJobInputReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TJobInputReader(NJobProxy::IUserJobReadControllerPtr userJobReadController, IInvokerPtr invoker)
        : Invoker_(std::move(invoker))
        , UserJobReadController_(std::move(userJobReadController))
        , AsyncStreamPipe_(New<TAsyncStreamPipe>())
    { }

    ~TJobInputReader()
    {
        if (TransferResultFuture_) {
            TransferResultFuture_.Cancel();
        }
    }

    void Open()
    {
        auto transferClosure = UserJobReadController_->PrepareJobInputTransfer(AsyncStreamPipe_);
        TransferResultFuture_ = transferClosure
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<TSharedRef> Read() override
    {
        return AsyncStreamPipe_->Read();
    }

private:
    const IInvokerPtr Invoker_;
    const NJobProxy::IUserJobReadControllerPtr UserJobReadController_;
    const NConcurrency::TAsyncStreamPipePtr AsyncStreamPipe_;

    TFuture<void> TransferResultFuture_;
};

DEFINE_REFCOUNTED_TYPE(TJobInputReader)

////////////////////////////////////////////////////////////////////////////////

class TQueryPreparer
    : public virtual TRefCounted
    , public IPrepareCallbacks
{
public:
    TQueryPreparer(
        NTabletClient::ITableMountCachePtr mountTableCache,
        IInvokerPtr invoker)
        : MountTableCache_(std::move(mountTableCache))
        , Invoker_(std::move(invoker))
    { }

    // IPrepareCallbacks implementation.

    virtual TFuture<TDataSplit> GetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryPreparer::DoGetInitialSplit, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(path, timestamp);
    }

private:
    const NTabletClient::ITableMountCachePtr MountTableCache_;
    const IInvokerPtr Invoker_;

    static TTableSchema GetTableSchema(
        const TRichYPath& path,
        const TTableMountInfoPtr& tableInfo)
    {
        if (auto optionalPathSchema = path.GetSchema()) {
            if (tableInfo->Dynamic) {
                THROW_ERROR_EXCEPTION("Explicit YPath \"schema\" specification is only allowed for static tables");
            }
            return *optionalPathSchema;
        }

        return tableInfo->Schemas[ETableSchemaKind::Query];
    }

    TDataSplit DoGetInitialSplit(
        const TRichYPath& path,
        TTimestamp timestamp)
    {
        auto tableInfo = WaitFor(MountTableCache_->GetTableInfo(path.GetPath()))
            .ValueOrThrow();

        tableInfo->ValidateNotReplicated();

        TDataSplit result;
        SetObjectId(&result, tableInfo->TableId);
        SetTableSchema(&result, GetTableSchema(path, tableInfo));
        SetTimestamp(&result, timestamp);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLookupRowsInputBufferTag
{ };

struct TLookupRowsOutputBufferTag
{ };

struct TGetInSyncReplicasTag
{ };

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options)
{
    YT_VERIFY(connection);

    return New<TClient>(std::move(connection), options);
}

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    IConnectionPtr connection,
    const TClientOptions& options)
    : Connection_(std::move(connection))
    , Options_(options)
    , ConcurrentRequestsSemaphore_(New<TAsyncSemaphore>(Connection_->GetConfig()->MaxConcurrentRequests))
    , Logger(NLogging::TLogger(ApiLogger)
        .AddTag("ClientId: %v", TGuid::Create()))
{
    auto wrapChannel = [&] (IChannelPtr channel) {
        channel = CreateAuthenticatedChannel(channel, options.GetUser());
        return channel;
    };
    auto wrapChannelFactory = [&] (IChannelFactoryPtr factory) {
        factory = CreateAuthenticatedChannelFactory(factory, options.GetUser());
        return factory;
    };

    auto initMasterChannel = [&] (EMasterChannelKind kind, TCellTag cellTag) {
        MasterChannels_[kind][cellTag] = wrapChannel(Connection_->GetMasterChannelOrThrow(kind, cellTag));
    };
    for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
        initMasterChannel(kind, Connection_->GetPrimaryMasterCellTag());
        for (auto cellTag : Connection_->GetSecondaryMasterCellTags()) {
            initMasterChannel(kind, cellTag);
        }
    }

    SchedulerChannel_ = wrapChannel(Connection_->GetSchedulerChannel());

    ChannelFactory_ = CreateNodeChannelFactory(
        wrapChannelFactory(Connection_->GetChannelFactory()),
        Connection_->GetNetworks());

    SchedulerProxy_.reset(new TSchedulerServiceProxy(GetSchedulerChannel()));
    JobProberProxy_.reset(new TJobProberServiceProxy(GetSchedulerChannel()));

    TransactionManager_ = New<TTransactionManager>(
        Connection_->GetConfig()->TransactionManager,
        Connection_->GetConfig()->PrimaryMaster->CellId,
        Connection_,
        Options_.GetUser(),
        Connection_->GetTimestampProvider(),
        Connection_->GetCellDirectory(),
        Connection_->GetDownedCellTracker());

    FunctionImplCache_ = CreateFunctionImplCache(
        Connection_->GetConfig()->FunctionImplCache,
        MakeWeak(this));

    FunctionRegistry_ = CreateFunctionRegistryCache(
        Connection_->GetConfig()->FunctionRegistryCache,
        MakeWeak(this),
        Connection_->GetInvoker());
}

NApi::IConnectionPtr TClient::GetConnection()
{
    return Connection_;
}

const ITableMountCachePtr& TClient::GetTableMountCache()
{
    return Connection_->GetTableMountCache();
}

const ITimestampProviderPtr& TClient::GetTimestampProvider()
{
    return Connection_->GetTimestampProvider();
}

const IConnectionPtr& TClient::GetNativeConnection()
{
    return Connection_;
}

IFunctionRegistryPtr TClient::GetFunctionRegistry()
{
    return FunctionRegistry_;
}

TFunctionImplCachePtr TClient::GetFunctionImplCache()
{
    return FunctionImplCache_;
}

const TClientOptions& TClient::GetOptions()
{
    return Options_;
}

IChannelPtr TClient::GetMasterChannelOrThrow(
    EMasterChannelKind kind,
    TCellTag cellTag)
{
    const auto& channels = MasterChannels_[kind];
    auto it = channels.find(cellTag == PrimaryMasterCellTag ? Connection_->GetPrimaryMasterCellTag() : cellTag);
    if (it == channels.end()) {
        THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
            cellTag);
    }
    return it->second;
}

IChannelPtr TClient::GetCellChannelOrThrow(TCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(cellId);
    return CreateAuthenticatedChannel(std::move(channel), Options_.GetUser());
}

IChannelPtr TClient::GetSchedulerChannel()
{
    return SchedulerChannel_;
}

const INodeChannelFactoryPtr& TClient::GetChannelFactory()
{
    return ChannelFactory_;
}

TFuture<void> TClient::Terminate()
{
    TransactionManager_->AbortAll();

    auto error = TError("Client terminated");
    std::vector<TFuture<void>> asyncResults;

    for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
        for (const auto& pair : MasterChannels_[kind]) {
            auto channel = pair.second;
            asyncResults.push_back(channel->Terminate(error));
        }
    }
    asyncResults.push_back(SchedulerChannel_->Terminate(error));

    return Combine(asyncResults);
}

TFuture<IFileReaderPtr> TClient::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options)
{
    return NNative::CreateFileReader(this, path, options);
}

IFileWriterPtr TClient::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    return NNative::CreateFileWriter(this, path, options);
}

IJournalReaderPtr TClient::CreateJournalReader(
    const TYPath& path,
    const TJournalReaderOptions& options)
{
    return NNative::CreateJournalReader(this, path, options);
}

IJournalWriterPtr TClient::CreateJournalWriter(
    const TYPath& path,
    const TJournalWriterOptions& options)
{
    return NNative::CreateJournalWriter(this, path, options);
}

TFuture<ITableReaderPtr> TClient::CreateTableReader(
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options)
{
    return NNative::CreateTableReader(this, path, options, New<TNameTable>());
}

TFuture<TSkynetSharePartsLocationsPtr> TClient::LocateSkynetShare(
    const NYPath::TRichYPath& path,
    const TLocateSkynetShareOptions& options)
{
    return NNative::LocateSkynetShare(this, path, options);
}

TFuture<ITableWriterPtr> TClient::CreateTableWriter(
    const NYPath::TRichYPath& path,
    const NApi::TTableWriterOptions& options)
{
    return NNative::CreateTableWriter(this, path, options);
}

const IChannelPtr& TClient::GetOperationArchiveChannel(EMasterChannelKind kind)
{
    if (!AreOperationsArchiveChannelsInitialized_) {
        // COMPAT(levysotsky): If user "operations_client" does not exist, fallback to "application_operations".
        TString operationsClientUserName;
        {
            auto path = GetUserPath(OperationsClientUserName);
            if (DoNodeExists(path, TNodeExistsOptions())) {
                operationsClientUserName = OperationsClientUserName;
            } else {
                operationsClientUserName = "application_operations";
            }
        }
        for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
            // NOTE(asaitgalin): Cache is tied to user so to utilize cache properly all Cypress
            // requests for operations archive should be performed under the same user.
            OperationsArchiveChannels_[kind] = CreateAuthenticatedChannel(
                Connection_->GetMasterChannelOrThrow(kind, PrimaryMasterCellTag),
                operationsClientUserName);
        }
        AreOperationsArchiveChannelsInitialized_ = true;
    }
    return OperationsArchiveChannels_[kind];
}

template <class T>
TFuture<T> TClient::Execute(
    TStringBuf commandName,
    const TTimeoutOptions& options,
    TCallback<T()> callback)
{
    auto promise = NewPromise<T>();
    ConcurrentRequestsSemaphore_->AsyncAcquire(
        BIND([
            commandName,
            promise,
            callback = std::move(callback),
            Logger = Logger
        ] (TAsyncSemaphoreGuard /*guard*/) mutable {
            if (promise.IsCanceled()) {
                return;
            }

            auto canceler = NConcurrency::GetCurrentFiberCanceler();
            if (canceler) {
                promise.OnCanceled(std::move(canceler));
            }

            try {
                YT_LOG_DEBUG("Command started (Command: %v)", commandName);
                TBox<T> result(callback);
                YT_LOG_DEBUG("Command completed (Command: %v)", commandName);
                result.SetPromise(promise);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Command failed (Command: %v)", commandName);
                promise.Set(TError(ex));
            }
        }),
        Connection_->GetInvoker());
    return promise
        .ToFuture()
        .WithTimeout(options.Timeout);
}

template <class T>
auto TClient::CallAndRetryIfMetadataCacheIsInconsistent(T&& callback) -> decltype(callback())
{
    int retryCount = 0;
    while (true) {
        TError error;

        try {
            return callback();
        } catch (const NYT::TErrorException& ex) {
            error = ex.Error();
        }

        const auto& config = Connection_->GetConfig();
        const auto& tableMountCache = Connection_->GetTableMountCache();
        bool retry;
        TTabletInfoPtr tabletInfo;
        std::tie(retry, tabletInfo) = tableMountCache->InvalidateOnError(error);

        if (retry && ++retryCount <= config->TableMountCache->OnErrorRetryCount) {
            YT_LOG_DEBUG(error, "Got error, will retry (attempt %v of %v)",
                retryCount,
                config->TableMountCache->OnErrorRetryCount);
            auto now = Now();
            auto retryTime = (tabletInfo ? tabletInfo->UpdateTime : now) +
                config->TableMountCache->OnErrorSlackPeriod;
            if (retryTime > now) {
                TDelayedExecutor::WaitForDuration(retryTime - now);
            }
            continue;
        }

        THROW_ERROR error;
    }
}

void TClient::SetMutationId(const IClientRequestPtr& request, const TMutatingOptions& options)
{
    NRpc::SetMutationId(request, options.GetOrGenerateMutationId(), options.Retry);
}

TTransactionId TClient::GetTransactionId(const TTransactionalOptions& options, bool allowNullTransaction)
{
    if (!options.TransactionId) {
        if (!allowNullTransaction) {
            THROW_ERROR_EXCEPTION("A valid master transaction is required");
        }
        return {};
    }

    if (options.Ping) {
        // XXX(babenko): this is just to make a ping; shall we even support this?
        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = options.Ping;
        attachOptions.PingAncestors = options.PingAncestors;
        TransactionManager_->Attach(options.TransactionId, attachOptions);
    }

    return options.TransactionId;
}

void TClient::SetTransactionId(
    const IClientRequestPtr& request,
    const TTransactionalOptions& options,
    bool allowNullTransaction)
{
    NCypressClient::SetTransactionId(request, GetTransactionId(options, allowNullTransaction));
}

void TClient::SetPrerequisites(
    const IClientRequestPtr& request,
    const TPrerequisiteOptions& options)
{
    if (options.PrerequisiteTransactionIds.empty() && options.PrerequisiteRevisions.empty()) {
        return;
    }

    auto* prerequisitesExt = request->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
    for (const auto& id : options.PrerequisiteTransactionIds) {
        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
        ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
    }
    for (const auto& revision : options.PrerequisiteRevisions) {
        auto* prerequisiteRevision = prerequisitesExt->add_revisions();
        prerequisiteRevision->set_path(revision->Path);
        ToProto(prerequisiteRevision->mutable_transaction_id(), revision->TransactionId);
        prerequisiteRevision->set_revision(revision->Revision);
    }
}

void TClient::SetSuppressAccessTracking(
    const IClientRequestPtr& request,
    const TSuppressableAccessTrackingOptions& commandOptions)
{
    if (commandOptions.SuppressAccessTracking) {
        NCypressClient::SetSuppressAccessTracking(request, true);
    }
    if (commandOptions.SuppressModificationTracking) {
        NCypressClient::SetSuppressModificationTracking(request, true);
    }
}

void TClient::SetCachingHeader(
    const IClientRequestPtr& request,
    const TMasterReadOptions& options)
{
    if (options.ReadFrom == EMasterChannelKind::Cache) {
        auto* cachingHeaderExt = request->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
        cachingHeaderExt->set_success_expiration_time(ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
        cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(options.ExpireAfterFailedUpdateTime));
    }
}

void TClient::SetBalancingHeader(
    const IClientRequestPtr& request,
    const TMasterReadOptions& options)
{
    if (options.ReadFrom == EMasterChannelKind::Cache) {
        auto* balancingHeaderExt = request->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
        balancingHeaderExt->set_enable_stickness(true);
        balancingHeaderExt->set_sticky_group_size(options.CacheStickyGroupSize);
    }
}

template <class TProxy>
std::unique_ptr<TProxy> TClient::CreateReadProxy(
    const TMasterReadOptions& options,
    TCellTag cellTag)
{
    auto channel = GetMasterChannelOrThrow(options.ReadFrom, cellTag);
    return std::make_unique<TProxy>(channel);
}

template <class TProxy>
std::unique_ptr<TProxy> TClient::CreateWriteProxy(
    TCellTag cellTag)
{
    auto channel = GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    return std::make_unique<TProxy>(channel);
}

template std::unique_ptr<TChunkServiceProxy> TClient::CreateWriteProxy<TChunkServiceProxy>(TCellTag cellTag);

IChannelPtr TClient::GetReadCellChannelOrThrow(TTabletCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(cellId);
    const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(cellDescriptor, EPeerKind::Leader);
    return ChannelFactory_->CreateChannel(primaryPeerDescriptor.GetAddressWithNetworkOrThrow(Connection_->GetNetworks()));
}

NTableClient::TColumnFilter TClient::RemapColumnFilter(
    const NTableClient::TColumnFilter& columnFilter,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable)
{
    if (columnFilter.IsUniversal()) {
        return columnFilter;
    }
    auto remappedFilterIndexes = columnFilter.GetIndexes();
    for (auto& index : remappedFilterIndexes) {
        if (index < 0 || index >= idMapping.size()) {
            THROW_ERROR_EXCEPTION(
                "Column filter contains invalid index: actual %v, expected in range [0, %v]",
                index,
                idMapping.size() - 1);
        }
        if (idMapping[index] == -1) {
            THROW_ERROR_EXCEPTION("Invalid column %Qv in column filter", nameTable->GetName(index));
        }
        index = idMapping[index];
    }
    return NTableClient::TColumnFilter(std::move(remappedFilterIndexes));
}

IUnversionedRowsetPtr TClient::DoLookupRows(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptions& options)
{
    TEncoderWithMapping encoder = [] (
        const NTableClient::TColumnFilter& remappedColumnFilter,
        const std::vector<TUnversionedRow>& remappedKeys) -> std::vector<TSharedRef>
    {
        TReqLookupRows req;
        if (remappedColumnFilter.IsUniversal()) {
            req.clear_column_filter();
        } else {
            ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.GetIndexes());
        }
        TWireProtocolWriter writer;
        writer.WriteCommand(EWireProtocolCommand::LookupRows);
        writer.WriteMessage(req);
        writer.WriteSchemafulRowset(remappedKeys);
        return writer.Finish();
    };

    TDecoderWithMapping decoder = [] (
        const TSchemaData& schemaData,
        TWireProtocolReader* reader) -> TTypeErasedRow
    {
        return reader->ReadSchemafulRow(schemaData, true).ToTypeErasedRow();
    };

    TReplicaFallbackHandler<IUnversionedRowsetPtr> fallbackHandler = [&] (
        const NApi::IClientPtr& replicaClient,
        const TTableReplicaInfoPtr& replicaInfo)
    {
        return replicaClient->LookupRows(replicaInfo->ReplicaPath, nameTable, keys, options);
    };

    return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
        return DoLookupRowsOnce<IUnversionedRowsetPtr, TUnversionedRow>(
            path,
            nameTable,
            keys,
            options,
            std::nullopt,
            encoder,
            decoder,
            fallbackHandler);
    });
}

IVersionedRowsetPtr TClient::DoVersionedLookupRows(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    TEncoderWithMapping encoder = [] (
        const NTableClient::TColumnFilter& remappedColumnFilter,
        const std::vector<TUnversionedRow>& remappedKeys) -> std::vector<TSharedRef>
    {
        TReqVersionedLookupRows req;
        if (remappedColumnFilter.IsUniversal()) {
            req.clear_column_filter();
        } else {
            ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.GetIndexes());
        }
        TWireProtocolWriter writer;
        writer.WriteCommand(EWireProtocolCommand::VersionedLookupRows);
        writer.WriteMessage(req);
        writer.WriteSchemafulRowset(remappedKeys);
        return writer.Finish();
    };

    TDecoderWithMapping decoder = [] (
        const TSchemaData& schemaData,
        TWireProtocolReader* reader) -> TTypeErasedRow
    {
        return reader->ReadVersionedRow(schemaData, true).ToTypeErasedRow();
    };

    TReplicaFallbackHandler<IVersionedRowsetPtr> fallbackHandler = [&] (
        const NApi::IClientPtr& replicaClient,
        const TTableReplicaInfoPtr& replicaInfo)
    {
        return replicaClient->VersionedLookupRows(replicaInfo->ReplicaPath, nameTable, keys, options);
    };

    std::optional<TString> retentionConfig;
    if (options.RetentionConfig) {
        retentionConfig = ConvertToYsonString(options.RetentionConfig).GetData();
    }

    return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
        return DoLookupRowsOnce<IVersionedRowsetPtr, TVersionedRow>(
            path,
            nameTable,
            keys,
            options,
            retentionConfig,
            encoder,
            decoder,
            fallbackHandler);
    });
}

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const std::vector<std::pair<NTableClient::TKey, int>>& keys)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    THashSet<TTabletId> tabletIds;
    for (const auto& pair : keys) {
        auto key = pair.first;
        auto tabletInfo = GetSortedTabletForRow(tableInfo, key);
        auto tabletId = tabletInfo->TabletId;
        if (tabletIds.insert(tabletId).second) {
            cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
        }
    }
    return PickInSyncReplicas(tableInfo, options, cellIdToTabletIds);
}

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    for (const auto& tabletInfo : tableInfo->Tablets) {
        cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
    }
    return PickInSyncReplicas(tableInfo, options, cellIdToTabletIds);
}

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const THashMap<TCellId, std::vector<TTabletId>>& cellIdToTabletIds)
{
    size_t cellCount = cellIdToTabletIds.size();
    size_t tabletCount = 0;
    for (const auto& pair : cellIdToTabletIds) {
        tabletCount += pair.second.size();
    }

    YT_LOG_DEBUG("Looking for in-sync replicas (Path: %v, CellCount: %v, TabletCount: %v)",
        tableInfo->Path,
        cellCount,
        tabletCount);

    const auto& channelFactory = Connection_->GetChannelFactory();
    const auto& cellDirectory = Connection_->GetCellDirectory();
    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncResults;
    for (const auto& pair : cellIdToTabletIds) {
        const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(pair.first);
        auto channel = CreateTabletReadChannel(
            channelFactory,
            cellDescriptor,
            options,
            Connection_->GetNetworks());

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        ToProto(req->mutable_tablet_ids(), pair.second);
        asyncResults.push_back(req->Invoke());
    }

    return Combine(asyncResults).Apply(
        BIND([=, this_ = MakeStrong(this)] (const std::vector<TQueryServiceProxy::TRspGetTabletInfoPtr>& rsps) {
            THashMap<TTableReplicaId, int> replicaIdToCount;
            for (const auto& rsp : rsps) {
                for (const auto& protoTabletInfo : rsp->tablets()) {
                    for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                        if (IsReplicaInSync(protoReplicaInfo, protoTabletInfo)) {
                            ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                        }
                    }
                }
            }

            TTableReplicaInfoPtrList inSyncReplicaInfos;
            for (const auto& replicaInfo : tableInfo->Replicas) {
                auto it = replicaIdToCount.find(replicaInfo->ReplicaId);
                if (it != replicaIdToCount.end() && it->second == tabletCount) {
                    YT_LOG_DEBUG("In-sync replica found (Path: %v, ReplicaId: %v, ClusterName: %v)",
                        tableInfo->Path,
                        replicaInfo->ReplicaId,
                        replicaInfo->ClusterName);
                    inSyncReplicaInfos.push_back(replicaInfo);
                }
            }

            if (inSyncReplicaInfos.empty()) {
                THROW_ERROR_EXCEPTION("No in-sync replicas found for table %v",
                    tableInfo->Path);
            }

            return inSyncReplicaInfos;
        }));
}

std::optional<TString> TClient::PickInSyncClusterAndPatchQuery(
    const TTabletReadOptions& options,
    NAst::TQuery* query)
{
    std::vector<TYPath> paths{query->Table.Path};
    for (const auto& join : query->Joins) {
        paths.push_back(join.Table.Path);
    }

    const auto& tableMountCache = Connection_->GetTableMountCache();
    std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;
    for (const auto& path : paths) {
        asyncTableInfos.push_back(tableMountCache->GetTableInfo(path));
    }

    auto tableInfos = WaitFor(Combine(asyncTableInfos))
        .ValueOrThrow();

    bool someReplicated = false;
    bool someNotReplicated = false;
    for (const auto& tableInfo : tableInfos) {
        if (tableInfo->IsReplicated()) {
            someReplicated = true;
        } else {
            someNotReplicated = true;
        }
    }

    if (someReplicated && someNotReplicated) {
        THROW_ERROR_EXCEPTION("Query involves both replicated and non-replicated tables");
    }

    if (!someReplicated) {
        return std::nullopt;
    }

    std::vector<TFuture<TTableReplicaInfoPtrList>> asyncCandidates;
    for (size_t tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
        asyncCandidates.push_back(PickInSyncReplicas(tableInfos[tableIndex], options));
    }

    auto candidates = WaitFor(Combine(asyncCandidates))
        .ValueOrThrow();

    THashMap<TString, int> clusterNameToCount;
    for (const auto& replicaInfos : candidates) {
        SmallVector<TString, TypicalReplicaCount> clusterNames;
        for (const auto& replicaInfo : replicaInfos) {
            clusterNames.push_back(replicaInfo->ClusterName);
        }
        std::sort(clusterNames.begin(), clusterNames.end());
        clusterNames.erase(std::unique(clusterNames.begin(), clusterNames.end()), clusterNames.end());
        for (const auto& clusterName : clusterNames) {
            ++clusterNameToCount[clusterName];
        }
    }

    SmallVector<TString, TypicalReplicaCount> inSyncClusterNames;
    for (const auto& pair : clusterNameToCount) {
        if (pair.second == paths.size()) {
            inSyncClusterNames.push_back(pair.first);
        }
    }

    if (inSyncClusterNames.empty()) {
        THROW_ERROR_EXCEPTION("No single cluster contains in-sync replicas for all involved tables %v",
            paths);
    }

    // TODO(babenko): break ties in a smarter way
    const auto& inSyncClusterName = inSyncClusterNames[0];
    YT_LOG_DEBUG("In-sync cluster selected (Paths: %v, ClusterName: %v)",
        paths,
        inSyncClusterName);

    auto patchTableDescriptor = [&] (NAst::TTableDescriptor* descriptor, const TTableReplicaInfoPtrList& replicaInfos) {
        for (const auto& replicaInfo : replicaInfos) {
            if (replicaInfo->ClusterName == inSyncClusterName) {
                descriptor->Path = replicaInfo->ReplicaPath;
                return;
            }
        }
        YT_ABORT();
    };

    patchTableDescriptor(&query->Table, candidates[0]);
    for (size_t index = 0; index < query->Joins.size(); ++index) {
        patchTableDescriptor(&query->Joins[index].Table, candidates[index + 1]);
    }
    return inSyncClusterName;
}

NApi::IConnectionPtr TClient::GetReplicaConnectionOrThrow(const TString& clusterName)
{
    const auto& clusterDirectory = Connection_->GetClusterDirectory();
    auto replicaConnection = clusterDirectory->FindConnection(clusterName);
    if (replicaConnection) {
        return replicaConnection;
    }

    WaitFor(Connection_->GetClusterDirectorySynchronizer()->Sync())
        .ThrowOnError();

    return clusterDirectory->GetConnectionOrThrow(clusterName);
}

NApi::IClientPtr TClient::CreateReplicaClient(const TString& clusterName)
{
    auto replicaConnection = GetReplicaConnectionOrThrow(clusterName);
    return replicaConnection->CreateClient(Options_);
}

void TClient::RemapValueIds(
    TVersionedRow /*row*/,
    std::vector<TTypeErasedRow>& rows,
    const std::vector<int>& mapping)
{
    for (auto untypedRow : rows) {
        auto row = TMutableVersionedRow(untypedRow);
        if (!row) {
            continue;
        }
        for (int index = 0; index < row.GetKeyCount(); ++index) {
            auto id = row.BeginKeys()[index].Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            row.BeginKeys()[index].Id = mapping[id];
        }
        for (int index = 0; index < row.GetValueCount(); ++index) {
            auto id = row.BeginValues()[index].Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            row.BeginValues()[index].Id = mapping[id];
        }
    }

}

void TClient::RemapValueIds(
    TUnversionedRow /*row*/,
    std::vector<TTypeErasedRow>& rows,
    const std::vector<int>& mapping)
{
    for (auto untypedRow : rows) {
        auto row = TMutableUnversionedRow(untypedRow);
        if (!row) {
            continue;
        }
        for (int index = 0; index < row.GetCount(); ++index) {
            auto id = row[index].Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            row[index].Id = mapping[id];
        }
    }
}

std::vector<int> TClient::BuildResponseIdMapping(const NTableClient::TColumnFilter& remappedColumnFilter)
{
    std::vector<int> mapping;
    for (int index = 0; index < remappedColumnFilter.GetIndexes().size(); ++index) {
        int id = remappedColumnFilter.GetIndexes()[index];
        if (id >= mapping.size()) {
            mapping.resize(id + 1, -1);
        }
        mapping[id] = index;
    }

    return mapping;
}

template <class TRowset, class TRow>
TRowset TClient::DoLookupRowsOnce(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptionsBase& options,
    const std::optional<TString>& retentionConfig,
    TEncoderWithMapping encoderWithMapping,
    TDecoderWithMapping decoderWithMapping,
    TReplicaFallbackHandler<TRowset> replicaFallbackHandler)
{
    if (options.EnablePartialResult && options.KeepMissingRows) {
        THROW_ERROR_EXCEPTION("Options \"enable_partial_result\" and \"keep_missing_rows\" cannot be used together");
    }

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
    auto idMapping = BuildColumnIdMapping(schema, nameTable);
    auto remappedColumnFilter = RemapColumnFilter(options.ColumnFilter, idMapping, nameTable);
    auto resultSchema = tableInfo->Schemas[ETableSchemaKind::Primary].Filter(remappedColumnFilter, true);
    auto resultSchemaData = TWireProtocolReader::GetSchemaData(schema, remappedColumnFilter);

    if (keys.Empty()) {
        return CreateRowset(resultSchema, TSharedRange<TRow>());
    }

    // NB: The server-side requires the keys to be sorted.
    std::vector<std::pair<NTableClient::TKey, int>> sortedKeys;
    sortedKeys.reserve(keys.Size());

    auto inputRowBuffer = New<TRowBuffer>(TLookupRowsInputBufferTag());
    auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
    auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

    for (int index = 0; index < keys.Size(); ++index) {
        ValidateClientKey(keys[index], schema, idMapping, nameTable);
        auto capturedKey = inputRowBuffer->CaptureAndPermuteRow(keys[index], schema, idMapping, nullptr);

        if (evaluator) {
            evaluator->EvaluateKeys(capturedKey, inputRowBuffer);
        }

        sortedKeys.emplace_back(capturedKey, index);
    }

    if (tableInfo->IsReplicated()) {
        auto inSyncReplicaInfos = WaitFor(PickInSyncReplicas(tableInfo, options, sortedKeys))
            .ValueOrThrow();
        // TODO(babenko): break ties in a smarter way
        const auto& inSyncReplicaInfo = inSyncReplicaInfos[0];
        auto replicaClient = CreateReplicaClient(inSyncReplicaInfo->ClusterName);
        auto asyncResult = replicaFallbackHandler(replicaClient, inSyncReplicaInfo);
        return WaitFor(asyncResult)
            .ValueOrThrow();
    }

    // TODO(sandello): Use code-generated comparer here.
    std::sort(sortedKeys.begin(), sortedKeys.end());
    std::vector<size_t> keyIndexToResultIndex(keys.Size());
    size_t currentResultIndex = 0;

    auto outputRowBuffer = New<TRowBuffer>(TLookupRowsOutputBufferTag());
    std::vector<TTypeErasedRow> uniqueResultRows;

    if (Connection_->GetConfig()->EnableLookupMultiread) {
        struct TBatch
        {
            NObjectClient::TObjectId TabletId;
            i64 MountRevision = 0;
            std::vector<TKey> Keys;
            size_t OffsetInResult;

            TQueryServiceProxy::TRspMultireadPtr Response;
        };

        std::vector<std::vector<TBatch>> batchesByCells;
        THashMap<TCellId, size_t> cellIdToBatchIndex;

        std::optional<bool> inMemory;

        {
            auto itemsBegin = sortedKeys.begin();
            auto itemsEnd = sortedKeys.end();

            size_t keySize = schema.GetKeyColumnCount();

            itemsBegin = std::lower_bound(
                itemsBegin,
                itemsEnd,
                tableInfo->LowerCapBound.Get(),
                [&] (const auto& item, TKey pivot) {
                    return CompareRows(item.first, pivot, keySize) < 0;
                });

            itemsEnd = std::upper_bound(
                itemsBegin,
                itemsEnd,
                tableInfo->UpperCapBound.Get(),
                [&] (TKey pivot, const auto& item) {
                    return CompareRows(pivot, item.first, keySize) < 0;
                });

            auto nextShardIt = tableInfo->Tablets.begin() + 1;
            for (auto itemsIt = itemsBegin; itemsIt != itemsEnd;) {
                YT_VERIFY(!tableInfo->Tablets.empty());

                // Run binary search to find the relevant tablets.
                nextShardIt = std::upper_bound(
                    nextShardIt,
                    tableInfo->Tablets.end(),
                    itemsIt->first,
                    [&] (TKey key, const TTabletInfoPtr& tabletInfo) {
                        return CompareRows(key, tabletInfo->PivotKey.Get(), keySize) < 0;
                    });

                const auto& startShard = *(nextShardIt - 1);
                auto nextPivotKey = (nextShardIt == tableInfo->Tablets.end())
                    ? tableInfo->UpperCapBound
                    : (*nextShardIt)->PivotKey;

                // Binary search to reduce expensive row comparisons
                auto endItemsIt = std::lower_bound(
                    itemsIt,
                    itemsEnd,
                    nextPivotKey.Get(),
                    [&] (const auto& item, TKey pivot) {
                        return CompareRows(item.first, pivot) < 0;
                    });

                ValidateTabletMountedOrFrozen(tableInfo, startShard);

                auto emplaced = cellIdToBatchIndex.emplace(startShard->CellId, batchesByCells.size());
                if (emplaced.second) {
                    batchesByCells.emplace_back();
                }

                TBatch batch;
                batch.TabletId = startShard->TabletId;
                batch.MountRevision = startShard->MountRevision;
                batch.OffsetInResult = currentResultIndex;

                if (startShard->InMemoryMode) {
                    YT_VERIFY(!inMemory || *inMemory == startShard->IsInMemory());
                    inMemory = startShard->IsInMemory();
                }

                std::vector<TKey> rows;
                rows.reserve(endItemsIt - itemsIt);

                while (itemsIt != endItemsIt) {
                    auto key = itemsIt->first;
                    rows.push_back(key);

                    do {
                        keyIndexToResultIndex[itemsIt->second] = currentResultIndex;
                        ++itemsIt;
                    } while (itemsIt != endItemsIt && itemsIt->first == key);
                    ++currentResultIndex;
                }

                batch.Keys = std::move(rows);
                batchesByCells[emplaced.first->second].push_back(std::move(batch));
            }
        }

        TTabletCellLookupSession::TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
        TTabletCellLookupSession::TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);

        auto* codec = NCompression::GetCodec(Connection_->GetConfig()->LookupRowsRequestCodec);

        std::vector<TFuture<TQueryServiceProxy::TRspMultireadPtr>> asyncResults(batchesByCells.size());

        const auto& cellDirectory = Connection_->GetCellDirectory();
        const auto& networks = Connection_->GetNetworks();

        for (const auto& item : cellIdToBatchIndex) {
            size_t cellIndex = item.second;
            const auto& batches = batchesByCells[cellIndex];

            auto channel = CreateTabletReadChannel(
                ChannelFactory_,
                cellDirectory->GetDescriptorOrThrow(item.first),
                options,
                networks);

            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultLookupRowsTimeout));
            proxy.SetDefaultRequestAck(false);

            auto req = proxy.Multiread();
            // TODO(babenko): set proper band
            req->set_request_codec(static_cast<int>(Connection_->GetConfig()->LookupRowsRequestCodec));
            req->set_response_codec(static_cast<int>(Connection_->GetConfig()->LookupRowsResponseCodec));
            req->set_timestamp(options.Timestamp);
            req->set_enable_partial_result(options.EnablePartialResult);

            if (inMemory && *inMemory) {
                req->Header().set_uncancelable(true);
            }
            if (retentionConfig) {
                req->set_retention_config(*retentionConfig);
            }

            for (const auto& batch : batches) {
                ToProto(req->add_tablet_ids(), batch.TabletId);
                req->add_mount_revisions(batch.MountRevision);
                TSharedRef requestData = codec->Compress(boundEncoder(batch.Keys));
                req->Attachments().push_back(requestData);
            }

            asyncResults[cellIndex] = req->Invoke();
        }

        auto results = WaitFor(CombineAll(std::move(asyncResults)))
            .ValueOrThrow();

        uniqueResultRows.resize(currentResultIndex, TTypeErasedRow{nullptr});

        auto* responseCodec = NCompression::GetCodec(Connection_->GetConfig()->LookupRowsResponseCodec);

        for (size_t cellIndex = 0; cellIndex < results.size(); ++cellIndex) {
            if (options.EnablePartialResult && !results[cellIndex].IsOK()) {
                continue;
            }

            const auto& batches = batchesByCells[cellIndex];
            const auto& result = results[cellIndex].ValueOrThrow();

            for (size_t batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
                const auto& attachment = result->Attachments()[batchIndex];

                if (options.EnablePartialResult && attachment.Empty()) {
                    continue;
                }

                auto responseData = responseCodec->Decompress(result->Attachments()[batchIndex]);
                TWireProtocolReader reader(responseData, outputRowBuffer);

                const auto& batch = batches[batchIndex];

                for (size_t index = 0; index < batch.Keys.size(); ++index) {
                    uniqueResultRows[batch.OffsetInResult + index] = boundDecoder(&reader);
                }
            }
        }
    } else {
        THashMap<TCellId, TTabletCellLookupSessionPtr> cellIdToSession;

        // TODO(sandello): Reuse code from QL here to partition sorted keys between tablets.
        // Get rid of hash map.
        // TODO(sandello): Those bind states must be in a cross-session shared state. Check this when refactor out batches.
        TTabletCellLookupSession::TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
        TTabletCellLookupSession::TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);
        for (int index = 0; index < sortedKeys.size();) {
            auto key = sortedKeys[index].first;
            auto tabletInfo = GetSortedTabletForRow(tableInfo, key);
            auto cellId = tabletInfo->CellId;
            auto it = cellIdToSession.find(cellId);
            if (it == cellIdToSession.end()) {
                auto session = New<TTabletCellLookupSession>(
                    Connection_->GetConfig(),
                    Connection_->GetNetworks(),
                    cellId,
                    options,
                    tableInfo,
                    retentionConfig,
                    boundEncoder,
                    boundDecoder);
                it = cellIdToSession.insert(std::make_pair(cellId, std::move(session))).first;
            }
            const auto& session = it->second;
            session->AddKey(currentResultIndex, std::move(tabletInfo), key);

            do {
                keyIndexToResultIndex[sortedKeys[index].second] = currentResultIndex;
                ++index;
            } while (index < sortedKeys.size() && sortedKeys[index].first == key);
            ++currentResultIndex;
        }

        std::vector<TFuture<void>> asyncResults;
        for (const auto& pair : cellIdToSession) {
            const auto& session = pair.second;
            asyncResults.push_back(session->Invoke(
                ChannelFactory_,
                Connection_->GetCellDirectory()));
        }

        WaitFor(Combine(std::move(asyncResults)))
            .ThrowOnError();

        // Rows are type-erased here and below to handle different kinds of rowsets.
        uniqueResultRows.resize(currentResultIndex);

        for (const auto& pair : cellIdToSession) {
            const auto& session = pair.second;
            session->ParseResponse(outputRowBuffer, &uniqueResultRows);
        }
    }

    if (!remappedColumnFilter.IsUniversal()) {
        RemapValueIds(TRow(), uniqueResultRows, BuildResponseIdMapping(remappedColumnFilter));
    }

    std::vector<TTypeErasedRow> resultRows;
    resultRows.resize(keys.Size());

    for (int index = 0; index < keys.Size(); ++index) {
        resultRows[index] = uniqueResultRows[keyIndexToResultIndex[index]];
    }

    if (!options.KeepMissingRows && !options.EnablePartialResult) {
        resultRows.erase(
            std::remove_if(
                resultRows.begin(),
                resultRows.end(),
                [] (TTypeErasedRow row) {
                    return !static_cast<bool>(row);
                }),
            resultRows.end());
    }

    auto rowRange = ReinterpretCastRange<TRow>(MakeSharedRange(std::move(resultRows), outputRowBuffer));
    return CreateRowset(resultSchema, std::move(rowRange));
}

TSelectRowsResult TClient::DoSelectRows(
    const TString& queryString,
    const TSelectRowsOptions& options)
{
    return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
        return DoSelectRowsOnce(queryString, options);
    });
}

TSelectRowsResult TClient::DoSelectRowsOnce(
    const TString& queryString,
    const TSelectRowsOptions& options)
{
    auto parsedQuery = ParseSource(queryString, EParseMode::Query);
    auto* astQuery = &std::get<NAst::TQuery>(parsedQuery->AstHead.Ast);
    auto optionalClusterName = PickInSyncClusterAndPatchQuery(options, astQuery);
    if (optionalClusterName) {
        auto replicaClient = CreateReplicaClient(*optionalClusterName);
        auto updatedQueryString = NAst::FormatQuery(*astQuery);
        auto asyncResult = replicaClient->SelectRows(updatedQueryString, options);
        return WaitFor(asyncResult)
            .ValueOrThrow();
    }

    auto inputRowLimit = options.InputRowLimit.value_or(Connection_->GetConfig()->DefaultInputRowLimit);
    auto outputRowLimit = options.OutputRowLimit.value_or(Connection_->GetConfig()->DefaultOutputRowLimit);

    const auto& udfRegistryPath = options.UdfRegistryPath
        ? *options.UdfRegistryPath
        : Connection_->GetConfig()->UdfRegistryPath;

    auto externalCGInfo = New<TExternalCGInfo>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);

        std::vector<TString> externalNames;
        for (const auto& name : names) {
            auto found = typeInferrers->find(name);
            if (found == typeInferrers->end()) {
                externalNames.push_back(name);
            }
        }

        auto descriptors = WaitFor(FunctionRegistry_->FetchFunctions(udfRegistryPath, externalNames))
            .ValueOrThrow();

        AppendUdfDescriptors(typeInferrers, externalCGInfo, externalNames, descriptors);
    };

    auto queryPreparer = New<TQueryPreparer>(Connection_->GetTableMountCache(), Connection_->GetInvoker());

    auto queryExecutor = CreateQueryExecutor(
        Connection_,
        Connection_->GetInvoker(),
        Connection_->GetColumnEvaluatorCache(),
        Connection_->GetQueryEvaluator(),
        ChannelFactory_,
        FunctionImplCache_);

    auto fragment = PreparePlanFragment(
        queryPreparer.Get(),
        *parsedQuery,
        fetchFunctions,
        options.Timestamp);
    const auto& query = fragment->Query;
    const auto& dataSource = fragment->Ranges;

    for (size_t index = 0; index < query->JoinClauses.size(); ++index) {
        if (query->JoinClauses[index]->ForeignKeyPrefix == 0 && !options.AllowJoinWithoutIndex) {
            const auto& ast = std::get<NAst::TQuery>(parsedQuery->AstHead.Ast);
            THROW_ERROR_EXCEPTION("Foreign table key is not used in the join clause; "
                "the query is inefficient, consider rewriting it")
                << TErrorAttribute("source", NAst::FormatJoin(ast.Joins[index]));
        }
    }

    TQueryOptions queryOptions;
    queryOptions.Timestamp = options.Timestamp;
    queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
    queryOptions.VerboseLogging = options.VerboseLogging;
    queryOptions.EnableCodeCache = options.EnableCodeCache;
    queryOptions.MaxSubqueries = options.MaxSubqueries;
    queryOptions.WorkloadDescriptor = options.WorkloadDescriptor;
    queryOptions.InputRowLimit = inputRowLimit;
    queryOptions.OutputRowLimit = outputRowLimit;
    queryOptions.UseMultijoin = options.UseMultijoin;
    queryOptions.AllowFullScan = options.AllowFullScan;
    queryOptions.ReadSessionId = TReadSessionId::Create();
    queryOptions.MemoryLimitPerNode = options.MemoryLimitPerNode;
    queryOptions.ExecutionPool = options.ExecutionPool;
    queryOptions.Deadline = options.Timeout.value_or(Connection_->GetConfig()->DefaultSelectRowsTimeout).ToDeadLine();
    queryOptions.SuppressAccessTracking = options.SuppressAccessTracking;

    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.WorkloadDescriptor = queryOptions.WorkloadDescriptor;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    blockReadOptions.ReadSessionId = queryOptions.ReadSessionId;

    IUnversionedRowsetWriterPtr writer;
    TFuture<IUnversionedRowsetPtr> asyncRowset;
    std::tie(writer, asyncRowset) = CreateSchemafulRowsetWriter(query->GetTableSchema());

    auto statistics = WaitFor(queryExecutor->Execute(
        query,
        externalCGInfo,
        dataSource,
        writer,
        blockReadOptions,
        queryOptions))
        .ValueOrThrow();

    auto rowset = WaitFor(asyncRowset)
        .ValueOrThrow();

    if (options.FailOnIncompleteResult) {
        if (statistics.IncompleteInput) {
            THROW_ERROR_EXCEPTION("Query terminated prematurely due to excessive input; consider rewriting your query or changing input limit")
                << TErrorAttribute("input_row_limit", inputRowLimit);
        }
        if (statistics.IncompleteOutput) {
            THROW_ERROR_EXCEPTION("Query terminated prematurely due to excessive output; consider rewriting your query or changing output limit")
                << TErrorAttribute("output_row_limit", outputRowLimit);
        }
    }

    return TSelectRowsResult{rowset, statistics};
}

bool TClient::IsReplicaInSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo)
{
    return
        ETableReplicaMode(replicaInfo.mode()) == ETableReplicaMode::Sync &&
        replicaInfo.current_replication_row_index() >= tabletInfo.total_row_count();
}

bool TClient::IsReplicaInSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo,
    TTimestamp timestamp)
{
    return
        replicaInfo.last_replication_timestamp() >= timestamp ||
        IsReplicaInSync(replicaInfo, tabletInfo);
}

std::vector<TTableReplicaId> TClient::DoGetInSyncReplicas(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    ValidateSyncTimestamp(options.Timestamp);

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();
    tableInfo->ValidateReplicated();

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
    auto idMapping = BuildColumnIdMapping(schema, nameTable);

    auto rowBuffer = New<TRowBuffer>(TGetInSyncReplicasTag());

    auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
    auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

    std::vector<TTableReplicaId> replicaIds;

    if (keys.Empty()) {
        for (const auto& replica : tableInfo->Replicas) {
            replicaIds.push_back(replica->ReplicaId);
        }
    } else {
        THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
        THashSet<TTabletId> tabletIds;
        for (auto key : keys) {
            ValidateClientKey(key, schema, idMapping, nameTable);
            auto capturedKey = rowBuffer->CaptureAndPermuteRow(key, schema, idMapping, nullptr);

            if (evaluator) {
                evaluator->EvaluateKeys(capturedKey, rowBuffer);
            }
            auto tabletInfo = tableInfo->GetTabletForRow(capturedKey);
            if (tabletIds.insert(tabletInfo->TabletId).second) {
                ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);
                cellToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
            }
        }

        std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
        for (const auto& pair : cellToTabletIds) {
            auto cellId = pair.first;
            const auto& perCellTabletIds = pair.second;
            auto channel = GetReadCellChannelOrThrow(cellId);

            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

            auto req = proxy.GetTabletInfo();
            ToProto(req->mutable_tablet_ids(), perCellTabletIds);
            futures.push_back(req->Invoke());
        }
        auto responsesResult = WaitFor(Combine(futures));
        auto responses = responsesResult.ValueOrThrow();

        THashMap<TTableReplicaId, int> replicaIdToCount;
        for (const auto& response : responses) {
            for (const auto& protoTabletInfo : response->tablets()) {
                for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                    if (IsReplicaInSync(protoReplicaInfo, protoTabletInfo, options.Timestamp)) {
                        ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                    }
                }
            }
        }

        for (const auto& pair : replicaIdToCount) {
            auto replicaId = pair.first;
            auto count = pair.second;
            if (count == tabletIds.size()) {
                replicaIds.push_back(replicaId);
            }
        }
    }

    YT_LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %llx)",
        tableInfo->TableId,
        replicaIds,
        options.Timestamp);

    return replicaIds;
}

std::vector<TColumnarStatistics> TClient::DoGetColumnarStatistics(
    const std::vector<TRichYPath>& paths,
    const TGetColumnarStatisticsOptions& options)
{
    std::vector<TColumnarStatistics> allStatistics;
    allStatistics.reserve(paths.size());

    std::vector<ui64> chunkCount;
    chunkCount.reserve(paths.size());

    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto fetcher = New<TColumnarStatisticsFetcher>(
        options.FetcherConfig,
        nodeDirectory,
        CreateSerializedInvoker(GetCurrentInvoker()),
        nullptr /* scraper */,
        this,
        Logger);

    for (const auto& path : paths) {
        YT_LOG_INFO("Collecting table input chunks (Path: %v)", path);

        auto transactionId = path.GetTransactionId();

        auto inputChunks = CollectTableInputChunks(
            path,
            this,
            nodeDirectory,
            options.FetchChunkSpecConfig,
            transactionId
                ? *transactionId
                : options.TransactionId,
            Logger);

        YT_LOG_INFO("Fetching columnar statistics (Columns: %v)", *path.GetColumns());


        for (const auto& inputChunk : inputChunks) {
            fetcher->AddChunk(inputChunk, *path.GetColumns());
        }
        chunkCount.push_back(inputChunks.size());
    }

    WaitFor(fetcher->Fetch())
        .ThrowOnError();

    const auto& chunkStatistics = fetcher->GetChunkStatistics();

    ui64 statisticsIndex = 0;

    for (int pathIndex = 0; pathIndex < paths.size(); ++pathIndex) {
        allStatistics.push_back(TColumnarStatistics::MakeEmpty(paths[pathIndex].GetColumns()->size()));
        for (ui64 chunkIndex = 0; chunkIndex < chunkCount[pathIndex]; ++statisticsIndex, ++chunkIndex) {
            allStatistics[pathIndex] += chunkStatistics[statisticsIndex];
        }
    }
    return allStatistics;
}

std::vector<TTabletInfo> TClient::DoGetTabletInfos(
    const TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletsInfoOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    struct TSubrequest
    {
        TQueryServiceProxy::TReqGetTabletInfoPtr Request;
        std::vector<size_t> ResultIndexes;
    };

    THashMap<TCellId, TSubrequest> cellIdToSubrequest;

    for (size_t resultIndex = 0; resultIndex < tabletIndexes.size(); ++resultIndex) {
        auto tabletIndex = tabletIndexes[resultIndex];
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);
        auto& subrequest = cellIdToSubrequest[tabletInfo->CellId];
        if (!subrequest.Request) {
            auto channel = GetReadCellChannelOrThrow(tabletInfo->CellId);
            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetTabletInfosTimeout));
            subrequest.Request = proxy.GetTabletInfo();
        }
        ToProto(subrequest.Request->add_tablet_ids(), tabletInfo->TabletId);
        subrequest.ResultIndexes.push_back(resultIndex);
    }

    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncRspsOrErrors;
    std::vector<const TSubrequest*> subrequests;
    for (const auto& pair : cellIdToSubrequest) {
        const auto& subrequest = pair.second;
        subrequests.push_back(&subrequest);
        asyncRspsOrErrors.push_back(subrequest.Request->Invoke());
    }

    auto rspsOrErrors = WaitFor(Combine(asyncRspsOrErrors))
        .ValueOrThrow();

    std::vector<TTabletInfo> results(tabletIndexes.size());
    for (size_t subrequestIndex = 0; subrequestIndex < rspsOrErrors.size(); ++subrequestIndex) {
        const auto& subrequest = *subrequests[subrequestIndex];
        const auto& rsp = rspsOrErrors[subrequestIndex];
        YT_VERIFY(rsp->tablets_size() == subrequest.ResultIndexes.size());
        for (size_t resultIndexIndex = 0; resultIndexIndex < subrequest.ResultIndexes.size(); ++resultIndexIndex) {
            auto& result = results[subrequest.ResultIndexes[resultIndexIndex]];
            const auto& tabletInfo = rsp->tablets(static_cast<int>(resultIndexIndex));
            result.TotalRowCount = tabletInfo.total_row_count();
            result.TrimmedRowCount = tabletInfo.trimmed_row_count();
            result.BarrierTimestamp = tabletInfo.barrier_timestamp();
        }
    }
    return results;
}

std::unique_ptr<IAttributeDictionary> TClient::ResolveExternalTable(
    const TYPath& path,
    TTableId* tableId,
    TCellTag* cellTag,
    const std::vector<TString>& extraAttributes)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
    auto batchReq = proxy->ExecuteBatch();

    {
        auto req = TTableYPathProxy::Get(path + "/@");
        std::vector<TString> attributeKeys{
            "id",
            "type",
            "external_cell_tag"
        };
        for (const auto& attribute : extraAttributes) {
            attributeKeys.push_back(attribute);
        }
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "get_attributes");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of table %v", path);

    const auto& batchRsp = batchRspOrError.Value();
    auto getAttributesRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");

    auto& rsp = getAttributesRspOrError.Value();
    auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
    if (!IsTableType(attributes->Get<EObjectType>("type"))) {
        THROW_ERROR_EXCEPTION("%v is not a table");
    }
    *tableId = attributes->Get<TTableId>("id");
    *cellTag = attributes->Get<TCellTag>("external_cell_tag", PrimaryMasterCellTag);
    return attributes;
}

template <class TReq>
void TClient::ExecuteTabletServiceRequest(const TYPath& path, TReq* req)
{
    TTableId tableId;
    TCellTag cellTag;
    auto attributes = ResolveExternalTable(
        path,
        &tableId,
        &cellTag,
        {"path"});

    if (!IsTableType(TypeFromId(tableId))) {
        THROW_ERROR_EXCEPTION("Object %v is not a table", path);
    }

    TTransactionStartOptions txOptions;
    txOptions.Multicell = cellTag != PrimaryMasterCellTag;
    txOptions.CellTag = cellTag;
    auto asyncTransaction = StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        txOptions);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    ToProto(req->mutable_table_id(), tableId);

    auto fullPath = attributes->Get<TString>("path");
    SetDynamicTableCypressRequestFullPath(req, fullPath);

    auto actionData = MakeTransactionActionData(*req);
    auto primaryCellId = GetNativeConnection()->GetPrimaryMasterCellId();
    transaction->AddAction(primaryCellId, actionData);

    if (cellTag != PrimaryMasterCellTag) {
        transaction->AddAction(ReplaceCellTagInId(primaryCellId, cellTag), actionData);
    }

    TTransactionCommitOptions commitOptions;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
    commitOptions.Force2PC = true;

    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();
}

void TClient::DoMountTable(
    const TYPath& path,
    const TMountTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqMount req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }
        if (options.CellId) {
            ToProto(req.mutable_cell_id(), options.CellId);
        }
        if (!options.TargetCellIds.empty()) {
            ToProto(req.mutable_target_cell_ids(), options.TargetCellIds);
        }
        req.set_freeze(options.Freeze);

        auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req.set_mount_timestamp(mountTimestamp);

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Mount(path);
        SetMutationId(req, options);

        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        if (options.CellId) {
            ToProto(req->mutable_cell_id(), options.CellId);
        }
        if (!options.TargetCellIds.empty()) {
            ToProto(req->mutable_target_cell_ids(), options.TargetCellIds);
        }
        req->set_freeze(options.Freeze);

        auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req->set_mount_timestamp(mountTimestamp);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoUnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqUnmount req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }
        req.set_force(options.Force);

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Unmount(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        req->set_force(options.Force);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoRemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqRemount req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_first_tablet_index(*options.LastTabletIndex);
        }

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Remount(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_first_tablet_index(*options.LastTabletIndex);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoFreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqFreeze req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Freeze(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoUnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqUnfreeze req;

        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Unfreeze(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

NTabletClient::NProto::TReqReshard TClient::MakeReshardRequest(
    const TReshardTableOptions& options)
{
    NTabletClient::NProto::TReqReshard req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }
    return req;
}

TTableYPathProxy::TReqReshardPtr TClient::MakeYPathReshardRequest(
    const TYPath& path,
    const TReshardTableOptions& options)
{
    auto req = TTableYPathProxy::Reshard(path);
    SetMutationId(req, options);

    if (options.FirstTabletIndex) {
        req->set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req->set_last_tablet_index(*options.LastTabletIndex);
    }
    return req;
}

void TClient::DoReshardTableWithPivotKeys(
    const TYPath& path,
    const std::vector<NTableClient::TOwningKey>& pivotKeys,
    const TReshardTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        auto req = MakeReshardRequest(options);
        ToProto(req.mutable_pivot_keys(), pivotKeys);
        req.set_tablet_count(pivotKeys.size());

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = MakeYPathReshardRequest(path, options);
        ToProto(req->mutable_pivot_keys(), pivotKeys);
        req->set_tablet_count(pivotKeys.size());

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoReshardTableWithTabletCount(
    const TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        auto req = MakeReshardRequest(options);
        req.set_tablet_count(tabletCount);

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = MakeYPathReshardRequest(path, options);
        req->set_tablet_count(tabletCount);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

std::vector<TTabletActionId> TClient::DoReshardTableAutomatic(
    const TYPath& path,
    const TReshardTableAutomaticOptions& options)
{
    if (options.FirstTabletIndex || options.LastTabletIndex) {
        THROW_ERROR_EXCEPTION("Tablet indices cannot be specified for automatic reshard");
    }

    TTableId tableId;
    TCellTag cellTag;
    auto attributes = ResolveExternalTable(
        path,
        &tableId,
        &cellTag,
        {"tablet_cell_bundle", "dynamic"});

    if (TypeFromId(tableId) != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid object type: expected %v, got %v", EObjectType::Table, TypeFromId(tableId))
            << TErrorAttribute("path", path);
    }

    if (!attributes->Get<bool>("dynamic")) {
        THROW_ERROR_EXCEPTION("Table %v must be dynamic",
            path);
    }

    auto bundle = attributes->Get<TString>("tablet_cell_bundle");
    InternalValidatePermission("//sys/tablet_cell_bundles/" + ToYPathLiteral(bundle), EPermission::Use);

    auto req = TTableYPathProxy::ReshardAutomatic(FromObjectId(tableId));
    SetMutationId(req, options);
    req->set_keep_actions(options.KeepActions);
    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    auto protoRsp = WaitFor(proxy->Execute(req))
        .ValueOrThrow();
    return FromProto<std::vector<TTabletActionId>>(protoRsp->tablet_actions());
}

void TClient::DoAlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    auto req = TTableYPathProxy::Alter(path);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);

    if (options.Schema) {
        ToProto(req->mutable_schema(), *options.Schema);
    }
    if (options.Dynamic) {
        req->set_dynamic(*options.Dynamic);
    }
    if (options.UpstreamReplicaId) {
        ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
    }

    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    WaitFor(proxy->Execute(req))
        .ThrowOnError();
}

void TClient::DoTrimTable(
    const TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const TTrimTableOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateOrdered();

    auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);

    auto channel = GetCellChannelOrThrow(tabletInfo->CellId);

    TTabletServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Connection_->GetConfig()->DefaultTrimTableTimeout);

    auto req = proxy.Trim();
    ToProto(req->mutable_tablet_id(), tabletInfo->TabletId);
    req->set_mount_revision(tabletInfo->MountRevision);
    req->set_trimmed_row_count(trimmedRowCount);

    WaitFor(req->Invoke())
        .ValueOrThrow();
}

void TClient::DoAlterTableReplica(
    TTableReplicaId replicaId,
    const TAlterTableReplicaOptions& options)
{
    InternalValidateTableReplicaPermission(replicaId, EPermission::Write);

    auto req = TTableReplicaYPathProxy::Alter(FromObjectId(replicaId));
    if (options.Enabled) {
        req->set_enabled(*options.Enabled);
    }
    if (options.Mode) {
        req->set_mode(static_cast<int>(*options.Mode));
    }
    if (options.PreserveTimestamps) {
        req->set_preserve_timestamps(*options.PreserveTimestamps);
    }
    if (options.Atomicity) {
        req->set_atomicity(static_cast<int>(*options.Atomicity));
    }

    auto cellTag = CellTagFromId(replicaId);
    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    WaitFor(proxy->Execute(req))
        .ThrowOnError();
}

std::vector<TTabletActionId> TClient::DoBalanceTabletCells(
    const TString& tabletCellBundle,
    const std::vector<TYPath>& movableTables,
    const TBalanceTabletCellsOptions& options)
{
    InternalValidatePermission("//sys/tablet_cell_bundles/" + ToYPathLiteral(tabletCellBundle), EPermission::Use);

    std::vector<TFuture<TTabletCellBundleYPathProxy::TRspBalanceTabletCellsPtr>> cellResponses;

    if (movableTables.empty()) {
        auto cellTags = Connection_->GetSecondaryMasterCellTags();
        cellTags.push_back(Connection_->GetPrimaryMasterCellTag());
        auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
        SetMutationId(req, options);
        req->set_keep_actions(options.KeepActions);
        for (const auto& cellTag : cellTags) {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
            cellResponses.emplace_back(proxy->Execute(req));
        }
    } else {
        THashMap<TCellTag, std::vector<TTableId>> tablesByCells;

        for (const auto& path : movableTables) {
            TTableId tableId;
            TCellTag cellTag;
            auto attributes = ResolveExternalTable(
                path,
                &tableId,
                &cellTag,
                {"dynamic", "tablet_cell_bundle"});

            if (TypeFromId(tableId) != EObjectType::Table) {
                THROW_ERROR_EXCEPTION(
                    "Invalid object type: expected %v, got %v",
                    EObjectType::Table, TypeFromId(tableId))
                    << TErrorAttribute("path", path);
            }

            if (!attributes->Get<bool>("dynamic")) {
                THROW_ERROR_EXCEPTION("Table must be dynamic")
                    << TErrorAttribute("path", path);
            }

            auto actualBundle = attributes->Find<TString>("tablet_cell_bundle");
            if (!actualBundle || *actualBundle != tabletCellBundle) {
                THROW_ERROR_EXCEPTION("All tables must be from the tablet cell bundle %Qv", tabletCellBundle);
            }

            tablesByCells[cellTag].push_back(tableId);
        }

        for (const auto& [cellTag, tableIds] : tablesByCells) {
            auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
            req->set_keep_actions(options.KeepActions);
            SetMutationId(req, options);
            ToProto(req->mutable_movable_tables(), tableIds);
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
            cellResponses.emplace_back(proxy->Execute(req));
        }
    }

    std::vector<TTabletActionId> tabletActions;
    for (auto& future : cellResponses) {
        auto errorOrRsp = WaitFor(future);
        if (errorOrRsp.IsOK()) {
            auto protoRsp = errorOrRsp.Value();
            auto tabletActionsFromCell = FromProto<std::vector<TTabletActionId>>(protoRsp->tablet_actions());
            tabletActions.insert(tabletActions.end(), tabletActionsFromCell.begin(), tabletActionsFromCell.end());
        } else {
            YT_LOG_DEBUG(errorOrRsp, "\"balance_tablet_cells\" subrequest to master cell failed");
        }
    }

    return tabletActions;
}

TOperationId TClient::DoStartOperation(
    EOperationType type,
    const TYsonString& spec,
    const TStartOperationOptions& options)
{
    auto req = SchedulerProxy_->StartOperation();
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    req->set_type(static_cast<int>(type));
    req->set_spec(spec.GetData());

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return FromProto<TOperationId>(rsp->operation_id());
}

void TClient::DoAbortOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TAbortOperationOptions& options)
{
    auto req = SchedulerProxy_->AbortOperation();
    ToProto(req, operationIdOrAlias);
    if (options.AbortMessage) {
        req->set_abort_message(*options.AbortMessage);
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoSuspendOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TSuspendOperationOptions& options)
{
    auto req = SchedulerProxy_->SuspendOperation();
    ToProto(req, operationIdOrAlias);
    req->set_abort_running_jobs(options.AbortRunningJobs);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoResumeOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TResumeOperationOptions& /*options*/)
{
    auto req = SchedulerProxy_->ResumeOperation();
    ToProto(req, operationIdOrAlias);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoCompleteOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TCompleteOperationOptions& /*options*/)
{
    auto req = SchedulerProxy_->CompleteOperation();
    ToProto(req, operationIdOrAlias);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoUpdateOperationParameters(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TYsonString& parameters,
    const TUpdateOperationParametersOptions& options)
{
    auto req = SchedulerProxy_->UpdateOperationParameters();
    ToProto(req, operationIdOrAlias);
    req->set_parameters(parameters.GetData());

    WaitFor(req->Invoke())
        .ThrowOnError();
}

bool TClient::DoesOperationsArchiveExist()
{
    // NB: we suppose that archive should exist and work correctly if this map node is presented.
    return WaitFor(NodeExists("//sys/operations_archive", TNodeExistsOptions()))
        .ValueOrThrow();
}

int TClient::DoGetOperationsArchiveVersion()
{
    auto asyncVersionResult = GetNode(GetOperationsArchiveVersionPath(), TGetNodeOptions());
    auto versionNodeOrError = WaitFor(asyncVersionResult);

    if (!versionNodeOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to get operations archive version")
            << versionNodeOrError;
    }

    int version = 0;
    try {
        version = ConvertTo<int>(versionNodeOrError.Value());
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse operations archive version")
            << ex;
    }

    return version;
}

// Attribute names allowed for 'get_operation' and 'list_operation' commands.
static const THashSet<TString> SupportedOperationAttributes = {
    "id",
    "state",
    "authenticated_user",
    "type",
    // COMPAT(levysotsky): "operation_type" is deprecated
    "operation_type",
    "progress",
    "spec",
    "annotations",
    "full_spec",
    "unrecognized_spec",
    "brief_progress",
    "brief_spec",
    "runtime_parameters",
    "start_time",
    "finish_time",
    "result",
    "events",
    "memory_usage",
    "suspended",
    "slot_index_per_pool_tree",
    "alerts",
};

// Attribute names allowed for 'get_job' and 'list_jobs' commands.
static const THashSet<TString> SupportedJobAttributes = {
    "operation_id",
    "job_id",
    "type",
    "state",
    "start_time",
    "finish_time",
    "address",
    "error",
    "statistics",
    "events",
    "has_spec",
};

// Map operation attribute names as they are requested in 'get_operation' or 'list_operations'
// commands to Cypress node attribute names.
std::vector<TString> TClient::MakeCypressOperationAttributes(const THashSet<TString>& attributes)
{
    std::vector<TString> result;
    result.reserve(attributes.size());
    for (const auto& attribute : attributes) {
        if (!SupportedOperationAttributes.contains(attribute)) {
            THROW_ERROR_EXCEPTION(
                NApi::EErrorCode::NoSuchAttribute,
                "Operation attribute %Qv is not supported",
                attribute)
                << TErrorAttribute("attribute_name", attribute);

        }
        if (attribute == "id") {
            result.push_back("key");
        } else if (attribute == "type") {
            result.push_back("operation_type");
        } else {
            result.push_back(attribute);
        }
    }
    return result;
}

// Map operation attribute names as they are requested in 'get_operation' or 'list_operations'
// commands to operations archive column names.
std::vector<TString> TClient::MakeArchiveOperationAttributes(const THashSet<TString>& attributes)
{
    std::vector<TString> result;
    result.reserve(attributes.size() + 1); // Plus 1 for 'id_lo' and 'id_hi' instead of 'id'.
    for (const auto& attribute : attributes) {
        if (!SupportedOperationAttributes.contains(attribute)) {
            THROW_ERROR_EXCEPTION(
                NApi::EErrorCode::NoSuchAttribute,
                "Operation attribute %Qv is not supported",
                attribute)
                << TErrorAttribute("attribute_name", attribute);
        }
        if (attribute == "id") {
            result.push_back("id_hi");
            result.push_back("id_lo");
        } else if (attribute == "type") {
            result.push_back("operation_type");
        } else if (attribute == "annotations") {
            if (DoGetOperationsArchiveVersion() >= 29) {
                result.push_back(attribute);
            }
        } else {
            result.push_back(attribute);
        }
    }
    return result;
}

TYsonString TClient::DoGetOperationFromCypress(
    NScheduler::TOperationId operationId,
    TInstant deadline,
    const TGetOperationOptions& options)
{
    std::optional<std::vector<TString>> cypressAttributes;
    if (options.Attributes) {
        cypressAttributes = MakeCypressOperationAttributes(*options.Attributes);

        if (!options.Attributes->contains("controller_agent_address")) {
            cypressAttributes->push_back("controller_agent_address");
        }
    }

    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    {
        auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@");
        if (cypressAttributes) {
            ToProto(req->mutable_attributes()->mutable_keys(), *cypressAttributes);
        }
        batchReq->AddRequest(req, "get_operation");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto cypressNodeRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operation");

    INodePtr cypressNode;
    if (cypressNodeRspOrError.IsOK()) {
        const auto& cypressNodeRsp = cypressNodeRspOrError.Value();
        cypressNode = ConvertToNode(TYsonString(cypressNodeRsp->value()));
    } else {
        if (!cypressNodeRspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR cypressNodeRspOrError;
        }
    }

    if (!cypressNode) {
        return TYsonString();
    }

    auto attrNode = cypressNode->AsMap();

    // XXX(ignat): remove opaque from node. Make option to ignore it in conversion methods.
    if (auto fullSpecNode = attrNode->FindChild("full_spec")) {
        fullSpecNode->MutableAttributes()->Remove("opaque");
    }

    if (auto child = attrNode->FindChild("operation_type")) {
        // COMPAT(levysotsky): When "operation_type" is disallowed, this code
        // will be simplified to unconditionally removing the child
        // (and also child will not have to be cloned).
        if (options.Attributes && !options.Attributes->contains("operation_type")) {
            attrNode->RemoveChild("operation_type");
        }

        attrNode->RemoveChild("type");
        YT_VERIFY(attrNode->AddChild("type", CloneNode(child)));
    }

    if (auto child = attrNode->FindChild("key")) {
        attrNode->RemoveChild("key");
        attrNode->RemoveChild("id");
        YT_VERIFY(attrNode->AddChild("id", child));
    }

    if (options.Attributes && !options.Attributes->contains("state")) {
        attrNode->RemoveChild("state");
    }

    if (!options.Attributes) {
        auto keysToKeep = ConvertTo<THashSet<TString>>(attrNode->GetChild("user_attribute_keys"));
        keysToKeep.insert("id");
        keysToKeep.insert("type");
        for (const auto& key : attrNode->GetKeys()) {
            if (!keysToKeep.contains(key)) {
                attrNode->RemoveChild(key);
            }
        }
    }

    std::optional<TString> controllerAgentAddress;
    if (auto child = attrNode->FindChild("controller_agent_address")) {
        controllerAgentAddress = child->AsString()->GetValue();
        if (options.Attributes && !options.Attributes->contains("controller_agent_address")) {
            attrNode->RemoveChild(child);
        }
    }

    std::vector<std::pair<TString, bool>> runtimeAttributes ={
        /* {Name, ShouldRequestFromScheduler} */
        {"progress", true},
        {"brief_progress", false},
        {"memory_usage", false}
    };

    if (options.IncludeRuntime) {
        auto batchReq = proxy->ExecuteBatch();

        auto addProgressAttributeRequest = [&] (const TString& attribute, bool shouldRequestFromScheduler) {
            if (shouldRequestFromScheduler) {
                auto req = TYPathProxy::Get(GetSchedulerOrchidOperationPath(operationId) + "/" + attribute);
                batchReq->AddRequest(req, "get_operation_" + attribute);
            }
            if (controllerAgentAddress) {
                auto path = GetControllerAgentOrchidOperationPath(*controllerAgentAddress, operationId);
                auto req = TYPathProxy::Get(path + "/" + attribute);
                batchReq->AddRequest(req, "get_operation_" + attribute);
            }
        };

        for (const auto& attribute : runtimeAttributes) {
            if (!options.Attributes || options.Attributes->contains(attribute.first)) {
                addProgressAttributeRequest(attribute.first, attribute.second);
            }
        }

        if (batchReq->GetSize() != 0) {
            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            auto handleProgressAttributeRequest = [&] (const TString& attribute) {
                INodePtr progressAttributeNode;

                auto responses = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_operation_" + attribute);
                for (const auto& rsp : responses) {
                    if (rsp.IsOK()) {
                        auto node = ConvertToNode(TYsonString(rsp.Value()->value()));
                        if (!progressAttributeNode) {
                            progressAttributeNode = node;
                        } else {
                            progressAttributeNode = PatchNode(progressAttributeNode, node);
                        }
                    } else {
                        if (!rsp.FindMatching(NYTree::EErrorCode::ResolveError)) {
                            THROW_ERROR rsp;
                        }
                    }

                    if (progressAttributeNode) {
                        attrNode->RemoveChild(attribute);
                        YT_VERIFY(attrNode->AddChild(attribute, progressAttributeNode));
                    }
                }
            };

            for (const auto& attribute : runtimeAttributes) {
                if (!options.Attributes || options.Attributes->contains(attribute.first)) {
                    handleProgressAttributeRequest(attribute.first);
                }
            }
        }
    }

    return ConvertToYsonString(attrNode);
}

TYsonString TClient::DoGetOperationFromArchive(
    NScheduler::TOperationId operationId,
    TInstant deadline,
    const TGetOperationOptions& options)
{
    auto attributes = options.Attributes.value_or(SupportedOperationAttributes);
    // Ignoring memory_usage and suspended in archive.
    attributes.erase("memory_usage");
    attributes.erase("suspended");

    auto fieldsVector = MakeArchiveOperationAttributes(attributes);
    THashSet<TString> fields(fieldsVector.begin(), fieldsVector.end());

    TOrderedByIdTableDescriptor tableDescriptor;
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TUnversionedRow> keys;
    keys.push_back(CreateOperationKey(operationId, tableDescriptor.Index, rowBuffer));

    std::vector<int> columnIndexes;
    THashMap<TString, int> fieldToIndex;

    int index = 0;
    for (const auto& field : fields) {
        columnIndexes.push_back(tableDescriptor.NameTable->GetIdOrThrow(field));
        fieldToIndex[field] = index++;
    }

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timeout = deadline - Now();

    auto rowset = WaitFor(LookupRows(
        GetOperationsArchiveOrderedByIdPath(),
        tableDescriptor.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions))
        .ValueOrThrow();

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.Empty());

    if (rows[0]) {
#define SET_ITEM_STRING_VALUE_WITH_FIELD(itemKey, fieldName) \
        SET_ITEM_VALUE_WITH_FIELD(itemKey, fieldName, TString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_STRING_VALUE(itemKey) SET_ITEM_STRING_VALUE_WITH_FIELD(itemKey, itemKey)
#define SET_ITEM_YSON_STRING_VALUE(itemKey) \
        SET_ITEM_VALUE(itemKey, TYsonString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_INSTANT_VALUE(itemKey) \
        SET_ITEM_VALUE(itemKey, TInstant::MicroSeconds(rows[0][index].Data.Int64))
#define SET_ITEM_VALUE_WITH_FIELD(itemKey, fieldName, operation) \
        .DoIf(fields.find(fieldName) != fields.end() && rows[0][GET_INDEX(fieldName)].Type != EValueType::Null, \
        [&] (TFluentMap fluent) { \
            auto index = GET_INDEX(fieldName); \
            fluent.Item(itemKey).Value(operation); \
        })
#define SET_ITEM_VALUE(itemKey, operation) SET_ITEM_VALUE_WITH_FIELD(itemKey, itemKey, operation)
#define GET_INDEX(itemKey) fieldToIndex.find(itemKey)->second

        auto ysonResult = BuildYsonStringFluently()
            .BeginMap()
                .DoIf(fields.contains("id_lo"), [&] (TFluentMap fluent) {
                    fluent.Item("id").Value(operationId);
                })
                SET_ITEM_STRING_VALUE("state")
                SET_ITEM_STRING_VALUE("authenticated_user")
                SET_ITEM_STRING_VALUE_WITH_FIELD("type", "operation_type")
                // COMPAT(levysotsky): Add this field under old name for
                // backward compatibility. Should be removed when all the clients migrate.
                SET_ITEM_STRING_VALUE("operation_type")
                SET_ITEM_YSON_STRING_VALUE("progress")
                SET_ITEM_YSON_STRING_VALUE("spec")
                SET_ITEM_YSON_STRING_VALUE("full_spec")
                SET_ITEM_YSON_STRING_VALUE("unrecognized_spec")
                SET_ITEM_YSON_STRING_VALUE("brief_progress")
                SET_ITEM_YSON_STRING_VALUE("brief_spec")
                SET_ITEM_YSON_STRING_VALUE("runtime_parameters")
                SET_ITEM_INSTANT_VALUE("start_time")
                SET_ITEM_INSTANT_VALUE("finish_time")
                SET_ITEM_YSON_STRING_VALUE("result")
                SET_ITEM_YSON_STRING_VALUE("events")
                SET_ITEM_YSON_STRING_VALUE("slot_index_per_pool_tree")
                SET_ITEM_YSON_STRING_VALUE("alerts")
            .EndMap();
#undef SET_ITEM_STRING_VALUE
#undef SET_ITEM_YSON_STRING_VALUE
#undef SET_ITEM_INSTANT_VALUE
#undef SET_ITEM_VALUE
#undef GET_INDEX
        return ysonResult;
    }

    return TYsonString();
}

TOperationId TClient::ResolveOperationAlias(
    const TString& alias,
    const TGetOperationOptions& options,
    TInstant deadline)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto req = TYPathProxy::Get(GetSchedulerOrchidAliasPath(alias) + "/operation_id");
    auto rspOrError = WaitFor(proxy->Execute(req));
    if (rspOrError.IsOK()) {
        return ConvertTo<TOperationId>(TYsonString(rspOrError.Value()->value()));
    } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        THROW_ERROR_EXCEPTION("Error while resolving alias from scheduler")
            << rspOrError
            << TErrorAttribute("operation_alias", alias);
    }

    TOperationAliasesTableDescriptor tableDescriptor;
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TUnversionedRow> keys;
    auto key = rowBuffer->AllocateUnversioned(1);
    key[0] = MakeUnversionedStringValue(alias, tableDescriptor.Index.Alias);
    keys.push_back(key);

    TLookupRowsOptions lookupOptions;
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timeout = deadline - Now();

    auto rowset = WaitFor(LookupRows(
        GetOperationsArchiveOperationAliasesPath(),
        tableDescriptor.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions))
        .ValueOrThrow();

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.Empty());
    if (rows[0]) {
        TOperationId operationId;
        operationId.Parts64[0] = rows[0][tableDescriptor.Index.OperationIdHi].Data.Uint64;
        operationId.Parts64[1] = rows[0][tableDescriptor.Index.OperationIdLo].Data.Uint64;
        return operationId;
    }

    THROW_ERROR_EXCEPTION("Operation alias is unknown")
        << TErrorAttribute("alias", alias);
}

TYsonString TClient::DoGetOperation(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TGetOperationOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetOperationTimeout);
    auto deadline = timeout.ToDeadLine();

    TOperationId operationId;
    Visit(operationIdOrAlias.Payload,
        [&] (const TOperationId& id) {
            operationId = id;
        },
        [&] (const TString& alias) {
            if (!options.IncludeRuntime) {
                THROW_ERROR_EXCEPTION(
                    "Operation alias cannot be resolved without using runtime information; "
                    "consider setting include_runtime = %true");
            }
            operationId = ResolveOperationAlias(alias, options, deadline);
        });

    std::vector<TFuture<TYsonString>> getOperationFutures;

    auto cypressFuture = BIND(&TClient::DoGetOperationFromCypress, MakeStrong(this), operationId, deadline, options)
        .AsyncVia(Connection_->GetInvoker())
        .Run()
        .WithTimeout(options.CypressTimeout);
    getOperationFutures.push_back(cypressFuture);

    TFuture<TYsonString> archiveFuture = MakeFuture<TYsonString>(TYsonString());
    if (DoesOperationsArchiveExist()) {
         archiveFuture = BIND(&TClient::DoGetOperationFromArchive, MakeStrong(this), operationId, deadline, options)
            .AsyncVia(Connection_->GetInvoker())
            .Run()
            .WithTimeout(options.ArchiveTimeout);
    }
    getOperationFutures.push_back(archiveFuture);

    auto getOperationResponses = WaitFor(CombineAll<TYsonString>(getOperationFutures))
        .ValueOrThrow();

    auto cypressResult = GetValueIgnoringTimeout(cypressFuture.Get());
    auto archiveResult = GetValueIgnoringTimeout(archiveFuture.Get());

    if (archiveResult && cypressResult) {
        // Merging goes here.
        auto cypressNode = ConvertToNode(cypressResult)->AsMap();

        std::vector<TString> fieldNames = {"brief_progress", "progress"};
        for (const auto& fieldName : fieldNames) {
            auto cypressFieldNode = cypressNode->FindChild(fieldName);
            cypressNode->RemoveChild(fieldName);

            auto archiveFieldString = NYTree::TryGetAny(archiveResult.GetData(), "/" + fieldName);

            TYsonString archiveFieldYsonString;
            if (archiveFieldString) {
                archiveFieldYsonString = TYsonString(*archiveFieldString);
            }

            TYsonString cypressFieldYsonString;
            if (cypressFieldNode) {
                cypressFieldYsonString = ConvertToYsonString(cypressFieldNode);
            }

            if (auto result = GetLatestProgress(cypressFieldYsonString, archiveFieldYsonString)) {
                cypressNode->AddChild(fieldName, ConvertToNode(result));
            }
        }
        return ConvertToYsonString(cypressNode);
    } else if (archiveResult) {
        return archiveResult;
    } else if (cypressResult) {
        return cypressResult;
    } else {
        THROW_ERROR_EXCEPTION(
            NApi::EErrorCode::NoSuchOperation,
            "No such operation %v",
            operationId)
            << cypressFuture.Get()
            << archiveFuture.Get();
    }
}

void TClient::DoDumpJobContext(
    TJobId jobId,
    const TYPath& path,
    const TDumpJobContextOptions& /*options*/)
{
    auto req = JobProberProxy_->DumpInputContext();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_path(), path);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::ValidateJobSpecVersion(
    TJobId jobId,
    const NYT::NJobTrackerClient::NProto::TJobSpec& jobSpec)
{
    if (!jobSpec.has_version() || jobSpec.version() != GetJobSpecVersion()) {
        THROW_ERROR_EXCEPTION("Job spec found in operation archive is of unsupported version")
            << TErrorAttribute("job_id", jobId)
            << TErrorAttribute("found_version", jobSpec.version())
            << TErrorAttribute("supported_version", GetJobSpecVersion());
    }
}

bool TClient::IsNoSuchJobOrOperationError(const TError& error)
{
    return
        error.FindMatching(NScheduler::EErrorCode::NoSuchJob) ||
        error.FindMatching(NScheduler::EErrorCode::NoSuchOperation);
}

// Get job node descriptor from scheduler and check that user has |requiredPermissions|
// for accessing the corresponding operation.
TErrorOr<TNodeDescriptor> TClient::GetJobNodeDescriptor(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    TNodeDescriptor jobNodeDescriptor;
    auto req = JobProberProxy_->GetJobNode();
    ToProto(req->mutable_job_id(), jobId);
    req->set_required_permissions(static_cast<ui32>(requiredPermissions));
    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        return TError("Failed to get job node descriptor")
            << std::move(rspOrError)
            << TErrorAttribute("job_id", jobId);
    }
    auto rsp = rspOrError.Value();
    FromProto(&jobNodeDescriptor, rsp->node_descriptor());
    return jobNodeDescriptor;
}

IChannelPtr TClient::TryCreateChannelToJobNode(
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = GetJobNodeDescriptor(jobId, requiredPermissions);
    if (jobNodeDescriptorOrError.IsOK()) {
        return ChannelFactory_->CreateChannel(jobNodeDescriptorOrError.ValueOrThrow());
    }

    if (!IsNoSuchJobOrOperationError(jobNodeDescriptorOrError)) {
        THROW_ERROR_EXCEPTION("Failed to get job node descriptor from scheduler")
            << jobNodeDescriptorOrError;
    }

    try {
        TGetJobOptions options;
        options.Attributes = {TString("address")};
        // TODO(ignat): support structured return value in GetJob.
        auto jobYsonString = WaitFor(GetJob(operationId, jobId, options))
            .ValueOrThrow();
        auto address = ConvertToNode(jobYsonString)->AsMap()->GetChild("address")->GetValue<TString>();
        auto nodeChannel = ChannelFactory_->CreateChannel(address);

        NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);
        auto jobSpecOrError = GetJobSpecFromJobNode(jobId, jobProberServiceProxy);
        if (!jobSpecOrError.IsOK()) {
            return nullptr;
        }

        auto jobSpec = jobSpecOrError
            .ValueOrThrow();

        ValidateJobSpecVersion(jobId, jobSpec);
        ValidateOperationAccess(jobId, jobSpec, requiredPermissions);

        return nodeChannel;
    } catch (const TErrorException& ex) {
        YT_LOG_DEBUG(ex, "Failed create node channel to job using address from archive (JobId: %v)", jobId);
        return nullptr;
    }
}

TErrorOr<NJobTrackerClient::NProto::TJobSpec> TClient::GetJobSpecFromJobNode(
    TJobId jobId,
    NJobProberClient::TJobProberServiceProxy& jobProberServiceProxy)
{
    auto req = jobProberServiceProxy.GetSpec();
    ToProto(req->mutable_job_id(), jobId);
    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        return TError("Failed to get job spec from job node")
            << std::move(rspOrError)
            << TErrorAttribute("job_id", jobId);
    }
    const auto& spec = rspOrError.Value()->spec();
    ValidateJobSpecVersion(jobId, spec);
    return spec;
}

// Get job spec from node and check that user has |requiredPermissions|
// for accessing the corresponding operation.
TErrorOr<NJobTrackerClient::NProto::TJobSpec> TClient::GetJobSpecFromJobNode(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = GetJobNodeDescriptor(jobId, requiredPermissions);
    if (!jobNodeDescriptorOrError.IsOK()) {
        return TError(std::move(jobNodeDescriptorOrError));
    }
    auto nodeChannel = ChannelFactory_->CreateChannel(jobNodeDescriptorOrError.ValueOrThrow());
    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);
    return GetJobSpecFromJobNode(jobId, jobProberServiceProxy);
}

// Get job spec from job archive and check that user has |requiredPermissions|
// for accessing the corresponding operation.
NJobTrackerClient::NProto::TJobSpec TClient::GetJobSpecFromArchive(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto nameTable = New<TNameTable>();

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = NTableClient::TColumnFilter({nameTable->RegisterName("spec")});
    lookupOptions.KeepMissingRows = true;

    auto owningKey = CreateJobKey(jobId, nameTable);

    std::vector<TUnversionedRow> keys;
    keys.push_back(owningKey);

    auto lookupResult = WaitFor(LookupRows(
        GetOperationsArchiveJobSpecsPath(),
        nameTable,
        MakeSharedRange(keys, owningKey),
        lookupOptions));

    if (!lookupResult.IsOK()) {
        THROW_ERROR_EXCEPTION(lookupResult)
            .Wrap("Lookup job spec in operation archive failed")
            << TErrorAttribute("job_id", jobId);
    }

    auto rows = lookupResult.Value()->GetRows();
    YT_VERIFY(!rows.Empty());

    if (!rows[0]) {
        THROW_ERROR_EXCEPTION("Missing job spec in job archive table")
            << TErrorAttribute("job_id", jobId);
    }

    auto value = rows[0][0];

    if (value.Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Found job spec has unexpected value type")
            << TErrorAttribute("job_id", jobId)
            << TErrorAttribute("value_type", value.Type);
    }

    NJobTrackerClient::NProto::TJobSpec jobSpec;
    bool ok = jobSpec.ParseFromArray(value.Data.String, value.Length);
    if (!ok) {
        THROW_ERROR_EXCEPTION("Cannot parse job spec")
            << TErrorAttribute("job_id", jobId);
    }

    ValidateJobSpecVersion(jobId, jobSpec);
    ValidateOperationAccess(jobId, jobSpec, requiredPermissions);

    return jobSpec;
}

IAsyncZeroCopyInputStreamPtr TClient::DoGetJobInput(
    TJobId jobId,
    const TGetJobInputOptions& /*options*/)
{
    NJobTrackerClient::NProto::TJobSpec jobSpec;
    auto jobSpecFromProxyOrError = GetJobSpecFromJobNode(jobId, EPermissionSet(EPermission::Read));
    if (!jobSpecFromProxyOrError.IsOK() && !IsNoSuchJobOrOperationError(jobSpecFromProxyOrError)) {
        THROW_ERROR jobSpecFromProxyOrError;
    }
    if (jobSpecFromProxyOrError.IsOK()) {
        jobSpec = std::move(jobSpecFromProxyOrError.Value());
    } else {
        jobSpec = GetJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));
    }

    auto* schedulerJobSpecExt = jobSpec.MutableExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto locateChunks = BIND([=] {
        std::vector<TChunkSpec*> chunkSpecList;
        for (auto& tableSpec : *schedulerJobSpecExt->mutable_input_table_specs()) {
            for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                chunkSpecList.push_back(&chunkSpec);
            }
        }

        for (auto& tableSpec : *schedulerJobSpecExt->mutable_foreign_input_table_specs()) {
            for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                chunkSpecList.push_back(&chunkSpec);
            }
        }

        LocateChunks(
            MakeStrong(this),
            New<TMultiChunkReaderConfig>()->MaxChunksPerLocateRequest,
            chunkSpecList,
            nodeDirectory,
            Logger);
        nodeDirectory->DumpTo(schedulerJobSpecExt->mutable_input_node_directory());
    });

    auto locateChunksResult = WaitFor(locateChunks
        .AsyncVia(GetConnection()->GetInvoker())
        .Run());

    if (!locateChunksResult.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to locate chunks used in job input")
            << TErrorAttribute("job_id", jobId);
    }

    auto jobSpecHelper = NJobProxy::CreateJobSpecHelper(jobSpec);

    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

    auto userJobReadController = CreateUserJobReadController(
        jobSpecHelper,
        MakeStrong(this),
        GetConnection()->GetInvoker(),
        TNodeDescriptor(),
        BIND([] { }) /* onNetworkRelease */,
        std::nullopt /* udfDirectory */,
        blockReadOptions,
        nullptr /* trafficMeter */,
        NConcurrency::GetUnlimitedThrottler() /* bandwidthThrottler */,
        NConcurrency::GetUnlimitedThrottler() /* rpsThrottler */);

    auto jobInputReader = New<TJobInputReader>(std::move(userJobReadController), GetConnection()->GetInvoker());
    jobInputReader->Open();
    return jobInputReader;
}

TYsonString TClient::DoGetJobInputPaths(
    TJobId jobId,
    const TGetJobInputPathsOptions& /*options*/)
{
    NJobTrackerClient::NProto::TJobSpec jobSpec;
    auto jobSpecFromProxyOrError = GetJobSpecFromJobNode(jobId, EPermissionSet(EPermission::Read));
    if (!jobSpecFromProxyOrError.IsOK() && !IsNoSuchJobOrOperationError(jobSpecFromProxyOrError)) {
        THROW_ERROR jobSpecFromProxyOrError;
    }
    if (jobSpecFromProxyOrError.IsOK()) {
        jobSpec = std::move(jobSpecFromProxyOrError.Value());
    } else {
        jobSpec = GetJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));
    }

    auto schedulerJobSpecExt = jobSpec.GetExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

    auto optionalDataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(schedulerJobSpecExt.extensions());
    if (!optionalDataSourceDirectoryExt) {
        THROW_ERROR_EXCEPTION("Cannot build job input paths; job is either too old or has intermediate input")
            << TErrorAttribute("job_id", jobId);
    }

    const auto& dataSourceDirectoryExt = *optionalDataSourceDirectoryExt;
    auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);

    for (const auto& dataSource : dataSourceDirectory->DataSources()) {
        if (!dataSource.GetPath()) {
            THROW_ERROR_EXCEPTION("Cannot build job input paths; job has intermediate input")
               << TErrorAttribute("job_id", jobId);
        }
    }

    std::vector<std::vector<TDataSliceDescriptor>> slicesByTable(dataSourceDirectory->DataSources().size());
    for (const auto& inputSpec : schedulerJobSpecExt.input_table_specs()) {
        auto dataSliceDescriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
        for (const auto& slice : dataSliceDescriptors) {
            slicesByTable[slice.GetDataSourceIndex()].push_back(slice);
        }
    }

    for (const auto& inputSpec : schedulerJobSpecExt.foreign_input_table_specs()) {
        auto dataSliceDescriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
        for (const auto& slice : dataSliceDescriptors) {
            slicesByTable[slice.GetDataSourceIndex()].push_back(slice);
        }
    }

    auto compareAbsoluteReadLimits = [] (const TReadLimit& lhs, const TReadLimit& rhs) -> bool {
        YT_VERIFY(lhs.HasRowIndex() == rhs.HasRowIndex());

        if (lhs.HasRowIndex() && lhs.GetRowIndex() != rhs.GetRowIndex()) {
            return lhs.GetRowIndex() < rhs.GetRowIndex();
        }

        if (lhs.HasKey() && rhs.HasKey()) {
            return lhs.GetKey() < rhs.GetKey();
        } else if (lhs.HasKey()) {
            // rhs is less
            return false;
        } else if (rhs.HasKey()) {
            // lhs is less
            return true;
        } else {
            // These read limits are effectively equal.
            return false;
        }
    };

    auto canMergeSlices = [] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs, bool versioned) {
        if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
            return false;
        }

        auto lhsUpperLimit = GetAbsoluteUpperReadLimit(lhs, versioned);
        auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned);

        YT_VERIFY(lhsUpperLimit.HasRowIndex() == rhsLowerLimit.HasRowIndex());
        if (lhsUpperLimit.HasRowIndex() && lhsUpperLimit.GetRowIndex() < rhsLowerLimit.GetRowIndex()) {
            return false;
        }

        if (lhsUpperLimit.HasKey() != rhsLowerLimit.HasKey()) {
            return false;
        }

        if (lhsUpperLimit.HasKey() && lhsUpperLimit.GetKey() < rhsLowerLimit.GetKey()) {
            return false;
        }

        return true;
    };

    std::vector<std::vector<std::pair<TDataSliceDescriptor, TDataSliceDescriptor>>> rangesByTable(dataSourceDirectory->DataSources().size());
    for (int tableIndex = 0; tableIndex < dataSourceDirectory->DataSources().size(); ++tableIndex) {
        bool versioned = dataSourceDirectory->DataSources()[tableIndex].GetType() == EDataSourceType::VersionedTable;
        auto& tableSlices = slicesByTable[tableIndex];
        std::sort(
            tableSlices.begin(),
            tableSlices.end(),
            [&] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs) {
                if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
                    return lhs.GetRangeIndex() < rhs.GetRangeIndex();
                }

                auto lhsLowerLimit = GetAbsoluteLowerReadLimit(lhs, versioned);
                auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned);

                return compareAbsoluteReadLimits(lhsLowerLimit, rhsLowerLimit);
            });

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (!canMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice], versioned)) {
                    break;
                }
                ++lastSlice;
            }
            rangesByTable[tableIndex].emplace_back(
                tableSlices[firstSlice],
                tableSlices[lastSlice - 1]);

            firstSlice = lastSlice;
        }
    }

    auto buildSliceLimit = [](const TReadLimit& limit, TFluentAny fluent) {
        fluent.BeginMap()
              .DoIf(limit.HasRowIndex(), [&] (TFluentMap fluent) {
                  fluent
                      .Item("row_index").Value(limit.GetRowIndex());
              })
              .DoIf(limit.HasKey(), [&] (TFluentMap fluent) {
                  fluent
                      .Item("key").Value(limit.GetKey());
              })
              .EndMap();
    };

    return BuildYsonStringFluently(EYsonFormat::Pretty)
        .DoListFor(rangesByTable, [&] (TFluentList fluent, const std::vector<std::pair<TDataSliceDescriptor, TDataSliceDescriptor>>& tableRanges) {
            fluent
                .DoIf(!tableRanges.empty(), [&] (TFluentList fluent) {
                    int dataSourceIndex = tableRanges[0].first.GetDataSourceIndex();
                    const auto& dataSource =  dataSourceDirectory->DataSources()[dataSourceIndex];
                    bool versioned = dataSource.GetType() == EDataSourceType::VersionedTable;
                    fluent
                        .Item()
                            .BeginAttributes()
                        .DoIf(dataSource.GetForeign(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("foreign").Value(true);
                        })
                        .Item("ranges")
                        .DoListFor(tableRanges, [&] (TFluentList fluent, const std::pair<TDataSliceDescriptor, TDataSliceDescriptor>& range) {
                            fluent
                                .Item()
                                .BeginMap()
                                .Item("lower_limit").Do(BIND(
                                    buildSliceLimit,
                                    GetAbsoluteLowerReadLimit(range.first, versioned)))
                                .Item("upper_limit").Do(BIND(
                                    buildSliceLimit,
                                    GetAbsoluteUpperReadLimit(range.second, versioned)))
                                .EndMap();
                        })
                        .EndAttributes()
                        .Value(dataSource.GetPath());
                });
        });
}

TSharedRef TClient::DoGetJobStderrFromNode(
    TOperationId operationId,
    TJobId jobId)
{
    auto nodeChannel = TryCreateChannelToJobNode(operationId, jobId, EPermissionSet(EPermission::Read));

    if (!nodeChannel) {
        return TSharedRef();
    }

    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);
    auto req = jobProberServiceProxy.GetStderr();
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    ToProto(req->mutable_job_id(), jobId);
    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        if (IsNoSuchJobOrOperationError(rspOrError) ||
            rspOrError.FindMatching(NJobProberClient::EErrorCode::JobIsNotRunning))
        {
            return TSharedRef();
        }
        THROW_ERROR_EXCEPTION("Failed to get job stderr from job proxy")
            << TErrorAttribute("operation_id", operationId)
            << TErrorAttribute("job_id", jobId)
            << std::move(rspOrError);
    }
    auto rsp = rspOrError.Value();
    return TSharedRef::FromString(rsp->stderr_data());
}

TSharedRef TClient::DoGetJobStderrFromCypress(
    TOperationId operationId,
    TJobId jobId)
{
    auto createFileReader = [&] (const NYPath::TYPath& path) {
        return WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path));
    };

    try {
        auto fileReader = createFileReader(NScheduler::GetStderrPath(operationId, jobId))
            .ValueOrThrow();

        std::vector<TSharedRef> blocks;
        while (true) {
            auto block = WaitFor(fileReader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

            blocks.push_back(std::move(block));
        }

        i64 size = GetByteSize(blocks);
        YT_VERIFY(size);
        auto stderrFile = TSharedMutableRef::Allocate(size);
        auto memoryOutput = TMemoryOutput(stderrFile.Begin(), size);

        for (const auto& block : blocks) {
            memoryOutput.Write(block.Begin(), block.Size());
        }

        return stderrFile;
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job stderr from Cypress")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobStderrFromArchive(
    TOperationId operationId,
    TJobId jobId)
{
    // Check permissions.
    GetJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));

    try {
        TJobStderrTableDescriptor tableDescriptor;

        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(4);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Index.OperationIdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Index.OperationIdLo);
        key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], tableDescriptor.Index.JobIdHi);
        key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], tableDescriptor.Index.JobIdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({tableDescriptor.Index.Stderr});
        lookupOptions.KeepMissingRows = true;

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobStderrsPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(keys, rowBuffer),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YT_VERIFY(!rows.Empty());

        if (rows[0]) {
            auto value = rows[0][0];

            YT_VERIFY(value.Type == EValueType::String);
            return TSharedRef::MakeCopy<char>(TRef(value.Data.String, value.Length));
        }
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job stderr from archive")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobStderr(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobStderrOptions& /*options*/)
{
    auto stderrRef = DoGetJobStderrFromNode(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    stderrRef = DoGetJobStderrFromCypress(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    stderrRef = DoGetJobStderrFromArchive(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::NoSuchJob, "Job stderr is not found")
        << TErrorAttribute("operation_id", operationId)
        << TErrorAttribute("job_id", jobId);
}

TSharedRef TClient::DoGetJobFailContextFromArchive(
    TOperationId operationId,
    TJobId jobId)
{
    // Check permissions.
    GetJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));

    try {
        TJobFailContextTableDescriptor tableDescriptor;

        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(4);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Index.OperationIdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Index.OperationIdLo);
        key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], tableDescriptor.Index.JobIdHi);
        key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], tableDescriptor.Index.JobIdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({tableDescriptor.Index.FailContext});
        lookupOptions.KeepMissingRows = true;

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobFailContextsPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(keys, rowBuffer),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YT_VERIFY(!rows.Empty());

        if (rows[0]) {
            auto value = rows[0][0];

            YT_VERIFY(value.Type == EValueType::String);
            return TSharedRef::MakeCopy<char>(TRef(value.Data.String, value.Length));
        }
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job fail_context from archive")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobFailContextFromCypress(
    TOperationId operationId,
    TJobId jobId)
{
    auto createFileReader = [&] (const NYPath::TYPath& path) {
        return WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path));
    };

    try {
        auto fileReader = createFileReader(NScheduler::GetFailContextPath(operationId, jobId))
            .ValueOrThrow();

        std::vector<TSharedRef> blocks;
        while (true) {
            auto block = WaitFor(fileReader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

            blocks.push_back(std::move(block));
        }

        i64 size = GetByteSize(blocks);
        YT_VERIFY(size);
        auto failContextFile = TSharedMutableRef::Allocate(size);
        auto memoryOutput = TMemoryOutput(failContextFile.Begin(), size);

        for (const auto& block : blocks) {
            memoryOutput.Write(block.Begin(), block.Size());
        }

        return failContextFile;
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job fail context from Cypress")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobFailContext(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobFailContextOptions& /*options*/)
{
    if (auto failContextRef = DoGetJobFailContextFromCypress(operationId, jobId)) {
        return failContextRef;
    }
    if (auto failContextRef = DoGetJobFailContextFromArchive(operationId, jobId)) {
        return failContextRef;
    }
    THROW_ERROR_EXCEPTION(
        NScheduler::EErrorCode::NoSuchJob,
        "Job fail context is not found")
        << TErrorAttribute("operation_id", operationId)
        << TErrorAttribute("job_id", jobId);
}

TString TClient::ExtractTextFactorForCypressItem(const TOperation& operation)
{
    std::vector<TString> textFactors;

    if (operation.Id) {
        textFactors.push_back(ToString(*operation.Id));
    }
    if (operation.AuthenticatedUser) {
        textFactors.push_back(*operation.AuthenticatedUser);
    }
    if (operation.State) {
        textFactors.push_back(ToString(*operation.State));
    }
    if (operation.Type) {
        textFactors.push_back(ToString(*operation.Type));
    }
    if (operation.Annotations) {
        textFactors.push_back(ConvertToYsonString(operation.Annotations, EYsonFormat::Text).GetData());
    }

    if (operation.BriefSpec) {
        auto briefSpecMapNode = ConvertToNode(operation.BriefSpec)->AsMap();
        if (briefSpecMapNode->FindChild("title")) {
            textFactors.push_back(briefSpecMapNode->GetChild("title")->AsString()->GetValue());
        }
        if (briefSpecMapNode->FindChild("input_table_paths")) {
            auto inputTablesNode = briefSpecMapNode->GetChild("input_table_paths")->AsList();
            if (inputTablesNode->GetChildCount() > 0) {
                textFactors.push_back(inputTablesNode->GetChildren()[0]->AsString()->GetValue());
            }
        }
        if (briefSpecMapNode->FindChild("output_table_paths")) {
            auto outputTablesNode = briefSpecMapNode->GetChild("output_table_paths")->AsList();
            if (outputTablesNode->GetChildCount() > 0) {
                textFactors.push_back(outputTablesNode->GetChildren()[0]->AsString()->GetValue());
            }
        }
    }

    if (operation.RuntimeParameters) {
        auto pools = GetPoolsFromRuntimeParameters(ConvertToNode(operation.RuntimeParameters));
        textFactors.insert(textFactors.end(), pools.begin(), pools.end());
    }

    return to_lower(JoinToString(textFactors, AsStringBuf(" ")));
}

std::vector<TString> TClient::GetPoolsFromRuntimeParameters(const INodePtr& runtimeParameters)
{
    YT_VERIFY(runtimeParameters);

    std::vector<TString> result;
    if (auto schedulingOptionsNode = runtimeParameters->AsMap()->FindChild("scheduling_options_per_pool_tree")) {
        for (const auto& entry : schedulingOptionsNode->AsMap()->GetChildren()) {
            if (auto poolNode = entry.second->AsMap()->FindChild("pool")) {
                result.push_back(poolNode->GetValue<TString>());
            }
        }
    }
    return result;
}

TOperation TClient::CreateOperationFromNode(
    const INodePtr& node,
    const std::optional<THashSet<TString>>& attributes)
{
    const auto& nodeAttributes = node->Attributes();

    TOperation operation;

    if (!attributes || attributes->contains("id")) {
        operation.Id = nodeAttributes.Find<TGuid>("key");
    }
    if (!attributes || attributes->contains("type")) {
        operation.Type = nodeAttributes.Find<NScheduler::EOperationType>("operation_type");
    }
    if (!attributes || attributes->contains("state")) {
        operation.State = nodeAttributes.Find<NScheduler::EOperationState>("state");
    }
    if (!attributes || attributes->contains("start_time")) {
        operation.StartTime = nodeAttributes.Find<TInstant>("start_time");
    }
    if (!attributes || attributes->contains("finish_time")) {
        operation.FinishTime = nodeAttributes.Find<TInstant>("finish_time");
    }
    if (!attributes || attributes->contains("authenticated_user")) {
        operation.AuthenticatedUser = nodeAttributes.Find<TString>("authenticated_user");
    }

    if (!attributes || attributes->contains("brief_spec")) {
        operation.BriefSpec = nodeAttributes.FindYson("brief_spec");
    }
    if (!attributes || attributes->contains("spec")) {
        operation.Spec = nodeAttributes.FindYson("spec");
    }
    if (!attributes || attributes->contains("full_spec")) {
        operation.FullSpec = nodeAttributes.FindYson("full_spec");
    }
    if (!attributes || attributes->contains("unrecognized_spec")) {
        operation.UnrecognizedSpec = nodeAttributes.FindYson("unrecognized_spec");
    }

    if (!attributes || attributes->contains("brief_progress")) {
        operation.BriefProgress = nodeAttributes.FindYson("brief_progress");
    }
    if (!attributes || attributes->contains("progress")) {
        operation.Progress = nodeAttributes.FindYson("progress");
    }

    if (!attributes || attributes->contains("runtime_parameters")) {
        operation.RuntimeParameters = nodeAttributes.FindYson("runtime_parameters");

        if (operation.RuntimeParameters) {
            auto runtimeParametersNode = ConvertToNode(operation.RuntimeParameters);
            operation.Pools = GetPoolsFromRuntimeParameters(runtimeParametersNode);
            operation.Acl = runtimeParametersNode->AsMap()->FindChild("acl");
        }
    }

    if (!attributes || attributes->contains("suspended")) {
        operation.Suspended = nodeAttributes.Find<bool>("suspended");
    }

    if (!attributes || attributes->contains("events")) {
        operation.Events = nodeAttributes.FindYson("events");
    }
    if (!attributes || attributes->contains("result")) {
        operation.Result = nodeAttributes.FindYson("result");
    }

    if (!attributes || attributes->contains("slot_index_per_pool_tree")) {
        operation.SlotIndexPerPoolTree = nodeAttributes.FindYson("slot_index_per_pool_tree");
    }

    if (!attributes || attributes->contains("alerts")) {
        operation.Alerts = nodeAttributes.FindYson("alerts");
    }

    if (!attributes || attributes->contains("annotations")) {
        operation.Annotations = nodeAttributes.FindYson("annotations");
    }

    return operation;
}

THashSet<TString> TClient::MakeFinalAttributeSet(
    const std::optional<THashSet<TString>>& originalAttributes,
    const THashSet<TString>& requiredAttributes,
    const THashSet<TString>& defaultAttributes,
    const THashSet<TString>& ignoredAttributes)
{
    auto attributes = originalAttributes.value_or(defaultAttributes);
    attributes.insert(requiredAttributes.begin(), requiredAttributes.end());
    for (const auto& attribute : ignoredAttributes) {
        attributes.erase(attribute);
    }
    return attributes;
}

// Searches in Cypress for operations satisfying given filters.
// Adds found operations to |idToOperation| map.
// The operations are returned with requested fields plus necessarily "start_time" and "id".
void TClient::DoListOperationsFromCypress(
    TInstant deadline,
    TCountingFilter& countingFilter,
    const TListOperationsAccessFilterPtr& accessFilter,
    const std::optional<THashSet<TString>>& transitiveClosureOfSubject,
    const TListOperationsOptions& options,
    THashMap<NScheduler::TOperationId, TOperation>* idToOperation)
{
    // These attributes will be requested for every operation in Cypress.
    // All the other attributes are considered heavy and if they are present in
    // the set of requested attributes an extra batch of "get" requests
    // (one for each operation satisfying filters) will be issued, so:
    // XXX(levysotsky): maintain this list up-to-date.
    const THashSet<TString> LightAttributes = {
        "authenticated_user",
        "brief_progress",
        "brief_spec",
        "events",
        "finish_time",
        "id",
        "type",
        "result",
        "runtime_parameters",
        "start_time",
        "state",
        "suspended",
    };

    const THashSet<TString> RequiredAttributes = {"id", "start_time"};

    const THashSet<TString> DefaultAttributes = {
        "authenticated_user",
        "brief_progress",
        "brief_spec",
        "finish_time",
        "id",
        "type",
        "runtime_parameters",
        "start_time",
        "state",
        "suspended",
    };

    const THashSet<TString> IgnoredAttributes = {};

    auto requestedAttributes = MakeFinalAttributeSet(options.Attributes, RequiredAttributes, DefaultAttributes, IgnoredAttributes);

    bool areAllRequestedAttributesLight = std::all_of(
        requestedAttributes.begin(),
        requestedAttributes.end(),
        [&] (const TString& attribute) {
            return LightAttributes.contains(attribute);
        });

    TObjectServiceProxy proxy(GetOperationArchiveChannel(options.ReadFrom));
    auto listBatchReq = proxy.ExecuteBatch();
    SetBalancingHeader(listBatchReq, options);

    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto hashStr = Format("%02x", hash);
        auto req = TYPathProxy::List("//sys/operations/" + hashStr);
        SetCachingHeader(req, options);
        auto attributes = LightAttributes;
        if (options.SubstrFilter) {
            attributes.emplace("annotations");
        }
        ToProto(req->mutable_attributes()->mutable_keys(), MakeCypressOperationAttributes(attributes));
        listBatchReq->AddRequest(req, "list_operations_" + hashStr);
    }

    auto listBatchRsp = WaitFor(listBatchReq->Invoke())
        .ValueOrThrow();

    auto substrFilter = options.SubstrFilter;
    if (substrFilter) {
        *substrFilter = to_lower(*substrFilter);
    }

    std::vector<TOperation> filteredOperations;
    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto rspOrError = listBatchRsp->GetResponse<TYPathProxy::TRspList>(Format("list_operations_%02x", hash));

        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            continue;
        }

        auto rsp = rspOrError.ValueOrThrow();
        auto operationNodes = ConvertToNode(TYsonString(rsp->value()))->AsList();

        for (const auto& operationNode : operationNodes->GetChildren()) {
            auto operation = CreateOperationFromNode(operationNode);

            if (options.FromTime && *operation.StartTime < *options.FromTime ||
                options.ToTime && *operation.StartTime >= *options.ToTime)
            {
                continue;
            }

            if (accessFilter) {
                YT_VERIFY(transitiveClosureOfSubject);
                if (!operation.Acl) {
                    continue;
                }
                auto action = CheckPermissionsByAclAndSubjectClosure(
                    ConvertTo<TSerializableAccessControlList>(operation.Acl),
                    *transitiveClosureOfSubject,
                    accessFilter->Permissions);
                if (action != ESecurityAction::Allow) {
                    continue;
                }
            }

            auto textFactor = ExtractTextFactorForCypressItem(operation);
            if (substrFilter && textFactor.find(*substrFilter) == std::string::npos) {
                continue;
            }

            EOperationState state = *operation.State;
            if (state != EOperationState::Pending && IsOperationInProgress(state)) {
                state = EOperationState::Running;
            }

            if (!countingFilter.Filter(operation.Pools, *operation.AuthenticatedUser, state, *operation.Type, 1)) {
                continue;
            }

            if (areAllRequestedAttributesLight) {
                filteredOperations.push_back(CreateOperationFromNode(operationNode, requestedAttributes));
            } else {
                filteredOperations.push_back(std::move(operation));
            }
        }
    }

    // Lookup all operations with certain ids, add their brief progress.
    if (DoesOperationsArchiveExist()) {
        std::vector<TUnversionedRow> keys;

        TOrderedByIdTableDescriptor tableDescriptor;
        auto rowBuffer = New<TRowBuffer>();
        for (const auto& operation : filteredOperations) {
            keys.push_back(CreateOperationKey(*operation.Id, tableDescriptor.Index, rowBuffer));
        }

        std::vector<int> columnIndexes = {tableDescriptor.NameTable->GetIdOrThrow("brief_progress")};

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
        lookupOptions.Timeout = options.ArchiveFetchingTimeout;
        lookupOptions.KeepMissingRows = true;
        auto rowsetOrError = WaitFor(LookupRows(
            GetOperationsArchiveOrderedByIdPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions));

        if (!rowsetOrError.IsOK()) {
            YT_LOG_DEBUG(rowsetOrError, "Failed to get information about operations' brief_progress from Archive");
        } else {
            const auto& rows = rowsetOrError.Value()->GetRows();

            for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
                const auto& row = rows[rowIndex];
                if (!row) {
                    continue;
                }

                auto& operation = filteredOperations[rowIndex];
                if (auto briefProgressPosition = lookupOptions.ColumnFilter.FindPosition(tableDescriptor.Index.BriefProgress)) {
                    auto briefProgressValue = row[*briefProgressPosition];
                    if (briefProgressValue.Type != EValueType::Null) {
                        auto briefProgressYsonString = FromUnversionedValue<TYsonString>(briefProgressValue);
                        operation.BriefProgress = GetLatestProgress(operation.BriefProgress, briefProgressYsonString);
                    }
                }
            }
        }
    }

    auto shouldRemove = [&] (const TOperation& operation) {
        if (!countingFilter.FilterByFailedJobs(operation.BriefProgress)) {
            return true;
        }
        return options.CursorTime &&
            ((options.CursorDirection == EOperationSortDirection::Past && *operation.StartTime >= *options.CursorTime) ||
            (options.CursorDirection == EOperationSortDirection::Future && *operation.StartTime <= *options.CursorTime));
    };

    filteredOperations.erase(
        std::remove_if(
            filteredOperations.begin(),
            filteredOperations.end(),
            shouldRemove),
        filteredOperations.end());

    // Retain more operations than limit to track (in)completeness of the response.
    auto operationsToRetain = options.Limit + 1;
    if (filteredOperations.size() > operationsToRetain) {
        std::nth_element(
            filteredOperations.begin(),
            filteredOperations.begin() + operationsToRetain,
            filteredOperations.end(),
            [&] (const TOperation& lhs, const TOperation& rhs) {
                // Leave only |operationsToRetain| operations:
                // either oldest (cursor_direction == "future") or newest (cursor_direction == "past").
                return (options.CursorDirection == EOperationSortDirection::Future) && (*lhs.StartTime < *rhs.StartTime) ||
                       (options.CursorDirection == EOperationSortDirection::Past  ) && (*lhs.StartTime > *rhs.StartTime);
            });
        filteredOperations.resize(operationsToRetain);
    }

    idToOperation->reserve(idToOperation->size() + filteredOperations.size());
    if (areAllRequestedAttributesLight) {
        for (auto& operation : filteredOperations) {
            (*idToOperation)[*operation.Id] = std::move(operation);
        }
    } else {
        auto getBatchReq = proxy.ExecuteBatch();
        SetBalancingHeader(getBatchReq, options);

        for (const auto& operation: filteredOperations) {
            auto req = TYPathProxy::Get(GetOperationPath(*operation.Id));
            SetCachingHeader(req, options);
            ToProto(req->mutable_attributes()->mutable_keys(), MakeCypressOperationAttributes(requestedAttributes));
            getBatchReq->AddRequest(req);
        }

        auto getBatchRsp = WaitFor(getBatchReq->Invoke())
            .ValueOrThrow();

        for (const auto& rspOrError : getBatchRsp->GetResponses<TYPathProxy::TRspGet>()) {
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                continue;
            }
            auto node = ConvertToNode(TYsonString(rspOrError.ValueOrThrow()->value()));
            auto operation = CreateOperationFromNode(node);
            (*idToOperation)[*operation.Id] = std::move(operation);
        }
    }
}

// Searches in archive for operations satisfying given filters.
// Returns operations with requested fields plus necessarily "start_time" and "id".
THashMap<NScheduler::TOperationId, TOperation> TClient::DoListOperationsFromArchive(
    TInstant deadline,
    TCountingFilter& countingFilter,
    const TListOperationsAccessFilterPtr& accessFilter,
    const std::optional<THashSet<TString>>& transitiveClosureOfSubject,
    const TListOperationsOptions& options)
{
    if (!options.FromTime) {
        THROW_ERROR_EXCEPTION("Missing required parameter \"from_time\"");
    }

    if (!options.ToTime) {
        THROW_ERROR_EXCEPTION("Missing required parameter \"to_time\"");
    }

    if (accessFilter) {
        constexpr int requiredVersion = 30;
        if (DoGetOperationsArchiveVersion() < requiredVersion) {
            THROW_ERROR_EXCEPTION("\"access\" filter is not supported in operations archive of version < %v",
                requiredVersion);
        }
    }

    auto addCommonWhereConjuncts = [&] (NQueryClient::TQueryBuilder* builder) {
        builder->AddWhereConjunct(Format("start_time > %v AND start_time <= %v",
            (*options.FromTime).MicroSeconds(),
            (*options.ToTime).MicroSeconds()));

        if (options.SubstrFilter) {
            builder->AddWhereConjunct(
                Format("is_substr(%Qv, filter_factors)", to_lower(*options.SubstrFilter)));
        }

        if (accessFilter) {
            YT_VERIFY(transitiveClosureOfSubject);
            builder->AddWhereConjunct(Format("NOT is_null(acl) AND _yt_has_permissions(acl, %Qv, %Qv)",
                ConvertToYsonString(*transitiveClosureOfSubject, EYsonFormat::Text),
                ConvertToYsonString(accessFilter->Permissions, EYsonFormat::Text)));
        }
    };

    if (options.IncludeCounters) {
        NQueryClient::TQueryBuilder builder;
        builder.SetSource(GetOperationsArchiveOrderedByStartTimePath());

        auto poolsIndex = builder.AddSelectExpression("pools_str");
        auto authenticatedUserIndex = builder.AddSelectExpression("authenticated_user");
        auto stateIndex = builder.AddSelectExpression("state");
        auto operationTypeIndex = builder.AddSelectExpression("operation_type");
        auto poolIndex = builder.AddSelectExpression("pool");
        auto countIndex = builder.AddSelectExpression("sum(1)", "count");

        addCommonWhereConjuncts(&builder);

        builder.AddGroupByExpression("any_to_yson_string(pools)", "pools_str");
        builder.AddGroupByExpression("authenticated_user");
        builder.AddGroupByExpression("state");
        builder.AddGroupByExpression("operation_type");
        builder.AddGroupByExpression("pool");

        TSelectRowsOptions selectOptions;
        selectOptions.Timeout = deadline - Now();
        selectOptions.InputRowLimit = std::numeric_limits<i64>::max();
        selectOptions.MemoryLimitPerNode = 100_MB;

        auto resultCounts = WaitFor(SelectRows(builder.Build(), selectOptions))
            .ValueOrThrow();

        for (auto row : resultCounts.Rowset->GetRows()) {
            std::optional<std::vector<TString>> pools;
            if (row[poolsIndex].Type != EValueType::Null) {
                pools = ConvertTo<std::vector<TString>>(TYsonString(row[poolsIndex].Data.String, row[poolsIndex].Length));
            }
            auto user = FromUnversionedValue<TStringBuf>(row[authenticatedUserIndex]);
            auto state = ParseEnum<EOperationState>(FromUnversionedValue<TStringBuf>(row[stateIndex]));
            auto type = ParseEnum<EOperationType>(FromUnversionedValue<TStringBuf>(row[operationTypeIndex]));
            if (row[poolIndex].Type != EValueType::Null) {
                if (!pools) {
                    pools.emplace();
                }
                pools->push_back(FromUnversionedValue<TString>(row[poolIndex]));
            }
            auto count = FromUnversionedValue<i64>(row[countIndex]);

            countingFilter.Filter(pools, user, state, type, count);
        }
    }

    NQueryClient::TQueryBuilder builder;
    builder.SetSource(GetOperationsArchiveOrderedByStartTimePath());

    builder.AddSelectExpression("id_hi");
    builder.AddSelectExpression("id_lo");

    addCommonWhereConjuncts(&builder);

    std::optional<EOrderByDirection> orderByDirection;

    switch (options.CursorDirection) {
        case EOperationSortDirection::Past:
            if (options.CursorTime) {
                builder.AddWhereConjunct(Format("start_time <= %v", (*options.CursorTime).MicroSeconds()));
            }
            orderByDirection = EOrderByDirection::Descending;
            break;
        case EOperationSortDirection::Future:
            if (options.CursorTime) {
                builder.AddWhereConjunct(Format("start_time > %v", (*options.CursorTime).MicroSeconds()));
            }
            orderByDirection = EOrderByDirection::Ascending;
            break;
        case EOperationSortDirection::None:
            break;
        default:
            YT_ABORT();
    }

    builder.AddOrderByExpression("start_time", orderByDirection);
    builder.AddOrderByExpression("id_hi", orderByDirection);
    builder.AddOrderByExpression("id_lo", orderByDirection);

    if (options.Pool) {
        builder.AddWhereConjunct(Format("list_contains(pools, %Qv) OR pool = %Qv", *options.Pool, *options.Pool));
    }

    if (options.StateFilter) {
        builder.AddWhereConjunct(Format("state = %Qv", FormatEnum(*options.StateFilter)));
    }

    if (options.TypeFilter) {
        builder.AddWhereConjunct(Format("operation_type = %Qv", FormatEnum(*options.TypeFilter)));
    }

    if (options.UserFilter) {
        builder.AddWhereConjunct(Format("authenticated_user = %Qv", *options.UserFilter));
    }

    // Retain more operations than limit to track (in)completeness of the response.
    builder.SetLimit(1 + options.Limit);

    TSelectRowsOptions selectOptions;
    selectOptions.Timeout = deadline - Now();
    selectOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectOptions.MemoryLimitPerNode = 100_MB;

    auto rowsItemsId = WaitFor(SelectRows(builder.Build(), selectOptions))
        .ValueOrThrow();

    TOrderedByIdTableDescriptor tableDescriptor;
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> keys;

    keys.reserve(rowsItemsId.Rowset->GetRows().Size());
    for (auto row : rowsItemsId.Rowset->GetRows()) {
        auto id = TOperationId(FromUnversionedValue<ui64>(row[0]), FromUnversionedValue<ui64>(row[1]));
        keys.push_back(CreateOperationKey(id, tableDescriptor.Index, rowBuffer));
    }

    const THashSet<TString> RequiredAttributes = {"id", "start_time", "brief_progress"};
    const THashSet<TString> DefaultAttributes = {
        "authenticated_user",
        "brief_progress",
        "brief_spec",
        "finish_time",
        "id",
        "runtime_parameters",
        "start_time",
        "state",
        "type",
    };
    const THashSet<TString> IgnoredAttributes = {"suspended", "memory_usage"};

    auto attributesToRequest = MakeFinalAttributeSet(options.Attributes, RequiredAttributes, DefaultAttributes, IgnoredAttributes);
    bool needBriefProgress = !options.Attributes || options.Attributes->contains("brief_progress");

    std::vector<int> columns;
    for (const auto columnName : MakeArchiveOperationAttributes(attributesToRequest)) {
        columns.push_back(tableDescriptor.NameTable->GetIdOrThrow(columnName));
    }

    NTableClient::TColumnFilter columnFilter(columns);

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = columnFilter;
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timeout = deadline - Now();

    auto rowset = WaitFor(LookupRows(
        GetOperationsArchiveOrderedByIdPath(),
        tableDescriptor.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions))
        .ValueOrThrow();

    auto rows = rowset->GetRows();

    auto getYson = [&] (const TUnversionedValue& value) {
        return value.Type == EValueType::Null
            ? TYsonString()
            : TYsonString(value.Data.String, value.Length);
    };
    auto getString = [&] (const TUnversionedValue& value, TStringBuf name) {
        if (value.Type == EValueType::Null) {
            THROW_ERROR_EXCEPTION("Unexpected null value in column %Qv in job archive", name);
        }
        return TStringBuf(value.Data.String, value.Length);
    };

    THashMap<NScheduler::TOperationId, TOperation> idToOperation;

    auto& tableIndex = tableDescriptor.Index;
    for (auto row : rows) {
        if (!row) {
            continue;
        }

        auto briefProgress = getYson(row[columnFilter.GetPosition(tableIndex.BriefProgress)]);
        if (!countingFilter.FilterByFailedJobs(briefProgress)) {
            continue;
        }

        TOperation operation;

        TGuid operationId(
            row[columnFilter.GetPosition(tableIndex.IdHi)].Data.Uint64,
            row[columnFilter.GetPosition(tableIndex.IdLo)].Data.Uint64);

        operation.Id = operationId;

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.OperationType)) {
            operation.Type = ParseEnum<EOperationType>(getString(row[*indexOrNull], "operation_type"));
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.State)) {
            operation.State = ParseEnum<EOperationState>(getString(row[*indexOrNull], "state"));
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.AuthenticatedUser)) {
            operation.AuthenticatedUser = TString(getString(row[*indexOrNull], "authenticated_user"));
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.StartTime)) {
            auto value = row[*indexOrNull];
            if (value.Type == EValueType::Null) {
                THROW_ERROR_EXCEPTION("Unexpected null value in column start_time in operations archive");
            }
            operation.StartTime = TInstant::MicroSeconds(value.Data.Int64);
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.FinishTime)) {
            if (row[*indexOrNull].Type != EValueType::Null) {
                operation.FinishTime = TInstant::MicroSeconds(row[*indexOrNull].Data.Int64);
            }
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.BriefSpec)) {
            operation.BriefSpec = getYson(row[*indexOrNull]);
        }
        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.FullSpec)) {
            operation.FullSpec = getYson(row[*indexOrNull]);
        }
        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Spec)) {
            operation.Spec = getYson(row[*indexOrNull]);
        }
        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.UnrecognizedSpec)) {
            operation.UnrecognizedSpec = getYson(row[*indexOrNull]);
        }

        if (needBriefProgress) {
            operation.BriefProgress = std::move(briefProgress);
        }
        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Progress)) {
            operation.Progress = getYson(row[*indexOrNull]);
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.RuntimeParameters)) {
            operation.RuntimeParameters = getYson(row[*indexOrNull]);
        }

        if (operation.RuntimeParameters) {
            operation.Pools = GetPoolsFromRuntimeParameters(ConvertToNode(operation.RuntimeParameters));
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Events)) {
            operation.Events = getYson(row[*indexOrNull]);
        }
        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Result)) {
            operation.Result = getYson(row[*indexOrNull]);
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.SlotIndexPerPoolTree)) {
            operation.SlotIndexPerPoolTree = getYson(row[*indexOrNull]);
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Alerts)) {
            operation.Alerts = getYson(row[*indexOrNull]);
        }

        idToOperation.emplace(*operation.Id, std::move(operation));
    }

    return idToOperation;
}

THashSet<TString> TClient::GetSubjectClosure(
    const TString& subject,
    TObjectServiceProxy& proxy,
    const TMasterReadOptions& options)
{
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, options);
    for (const auto& path : {GetUserPath(subject), GetGroupPath(subject)}) {
        auto req = TYPathProxy::Get(path + "/@member_of_closure");
        SetCachingHeader(req, options);
        batchReq->AddRequest(req);
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    for (const auto& rspOrError : batchRsp->GetResponses<TYPathProxy::TRspGet>()) {
        if (rspOrError.IsOK()) {
            auto res = ConvertTo<THashSet<TString>>(TYsonString(rspOrError.Value()->value()));
            res.insert(subject);
            return res;
        } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION(
                "Failed to get \"member_of_closure\" attribute for subject %Qv",
                subject)
                << rspOrError;
        }
    }
    THROW_ERROR_EXCEPTION(
        "Unrecognized subject %Qv",
        subject);
}

// XXX(levysotsky): The counters may be incorrect if |options.IncludeArchive| is |true|
// and an operation is in both Cypress and archive.
// XXX(levysotsky): The "failed_jobs_count" counter is incorrect if corresponding failed operations
// are in archive and outside of queried range.
TListOperationsResult TClient::DoListOperations(
    const TListOperationsOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultListOperationsTimeout);
    auto deadline = timeout.ToDeadLine();

    if (options.CursorTime && (
        options.ToTime && *options.CursorTime > *options.ToTime ||
        options.FromTime && *options.CursorTime < *options.FromTime))
    {
        THROW_ERROR_EXCEPTION("Time cursor (%v) is out of range [from_time (%v), to_time (%v)]",
            *options.CursorTime,
            *options.FromTime,
            *options.ToTime);
    }

    constexpr ui64 MaxLimit = 100;
    if (options.Limit > MaxLimit) {
        THROW_ERROR_EXCEPTION("Requested result limit (%v) exceeds maximum allowed limit (%v)",
            options.Limit,
            MaxLimit);
    }

    auto accessFilter = options.AccessFilter;
    std::optional<THashSet<TString>> transitiveClosureOfSubject;
    if (accessFilter) {
        TObjectServiceProxy proxy(GetOperationArchiveChannel(options.ReadFrom));
        transitiveClosureOfSubject = GetSubjectClosure(
            accessFilter->Subject,
            proxy,
            options);
        if (accessFilter->Subject == RootUserName || transitiveClosureOfSubject->contains(SuperusersGroupName)) {
            accessFilter.Reset();
        }
    }

    TCountingFilter countingFilter(options);

    THashMap<NScheduler::TOperationId, TOperation> idToOperation;
    if (options.IncludeArchive && DoesOperationsArchiveExist()) {
        idToOperation = DoListOperationsFromArchive(
            deadline,
            countingFilter,
            accessFilter,
            transitiveClosureOfSubject,
            options);
    }

    DoListOperationsFromCypress(
        deadline,
        countingFilter,
        accessFilter,
        transitiveClosureOfSubject,
        options,
        &idToOperation);

    std::vector<TOperation> operations;
    operations.reserve(idToOperation.size());
    for (auto& item : idToOperation) {
        operations.push_back(std::move(item.second));
    }

    std::sort(operations.begin(), operations.end(), [&] (const TOperation& lhs, const TOperation& rhs) {
        // Reverse order: most recent first.
        return
        std::tie(*lhs.StartTime, (*lhs.Id).Parts64[0], (*lhs.Id).Parts64[1])
        >
        std::tie(*rhs.StartTime, (*rhs.Id).Parts64[0], (*rhs.Id).Parts64[1]);
    });

    TListOperationsResult result;

    result.Operations = std::move(operations);
    if (result.Operations.size() > options.Limit) {
        if (options.CursorDirection == EOperationSortDirection::Past) {
            result.Operations.resize(options.Limit);
        } else {
            result.Operations.erase(result.Operations.begin(), result.Operations.end() - options.Limit);
        }
        result.Incomplete = true;
    }

    // Fetching progress for operations with mentioned ids.
    if (DoesOperationsArchiveExist()) {
        std::vector<TUnversionedRow> keys;

        TOrderedByIdTableDescriptor tableDescriptor;
        auto rowBuffer = New<TRowBuffer>();
        for (const auto& operation: result.Operations) {
            keys.push_back(CreateOperationKey(*operation.Id, tableDescriptor.Index, rowBuffer));
        }

        bool needBriefProgress = !options.Attributes || options.Attributes->contains("brief_progress");
        bool needProgress = options.Attributes && options.Attributes->contains("progress");

        std::vector<TString> fields;
        if (needBriefProgress) {
            fields.emplace_back("brief_progress");
        }
        if (needProgress) {
            fields.emplace_back("progress");
        }
        std::vector<int> columnIndexes;
        for (const auto& field : fields) {
            columnIndexes.push_back(tableDescriptor.NameTable->GetIdOrThrow(field));
        }

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
        lookupOptions.Timeout = options.ArchiveFetchingTimeout;
        lookupOptions.KeepMissingRows = true;
        auto rowsetOrError = WaitFor(LookupRows(
            GetOperationsArchiveOrderedByIdPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions));

        if (!rowsetOrError.IsOK()) {
            YT_LOG_DEBUG(rowsetOrError, "Failed to get information about operations' progress and brief_progress from Archive");
        } else {
            const auto& rows = rowsetOrError.Value()->GetRows();

            for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
                const auto& row = rows[rowIndex];
                if (!row) {
                    continue;
                }

                auto& operation = result.Operations[rowIndex];
                if (auto briefProgressPosition = lookupOptions.ColumnFilter.FindPosition(tableDescriptor.Index.BriefProgress)) {
                    auto briefProgressValue = row[*briefProgressPosition];
                    if (briefProgressValue.Type != EValueType::Null) {
                        auto briefProgressYsonString = FromUnversionedValue<TYsonString>(briefProgressValue);
                        operation.BriefProgress = GetLatestProgress(operation.BriefProgress, briefProgressYsonString);
                    }
                }
                if (auto progressPosition = lookupOptions.ColumnFilter.FindPosition(tableDescriptor.Index.Progress)) {
                    auto progressValue = row[*progressPosition];
                    if (progressValue.Type != EValueType::Null) {
                        auto progressYsonString = FromUnversionedValue<TYsonString>(progressValue);
                        operation.Progress = GetLatestProgress(operation.Progress, progressYsonString);
                    }
                }
            }
        }
    }

    if (options.IncludeCounters) {
        result.PoolCounts = std::move(countingFilter.PoolCounts);
        result.UserCounts = std::move(countingFilter.UserCounts);
        result.StateCounts = std::move(countingFilter.StateCounts);
        result.TypeCounts = std::move(countingFilter.TypeCounts);
        result.FailedJobsCount = countingFilter.FailedJobsCount;
    }

    return result;
}

void TClient::ValidateNotNull(
    const TUnversionedValue& value,
    TStringBuf name,
    TOperationId operationId,
    TJobId jobId)
{
    if (Y_UNLIKELY(value.Type == EValueType::Null)) {
        auto error = TError("Unexpected null value in column %Qv in job archive", name)
            << TErrorAttribute("operation_id", operationId);
        if (jobId) {
            error = error << TErrorAttribute("job_id", jobId);
        }
        THROW_ERROR error;
    }
};

NQueryClient::TQueryBuilder TClient::GetListJobsQueryBuilder(
    TOperationId operationId,
    const std::vector<EJobState>& states,
    const TListJobsOptions& options)
{
    NQueryClient::TQueryBuilder builder;
    builder.SetSource(GetOperationsArchiveJobsPath());

    builder.AddWhereConjunct(Format(
        "(operation_id_hi, operation_id_lo) = (%vu, %vu)",
        operationId.Parts64[0],
        operationId.Parts64[1]));

    builder.AddWhereConjunct(Format(
        R""(job_state IN ("aborted", "failed", "completed", "lost") )""
        "OR (NOT is_null(update_time) AND update_time >= %v)",
        (TInstant::Now() - options.RunningJobsLookbehindPeriod).MicroSeconds()));

    if (options.Address) {
        builder.AddWhereConjunct(Format("address = %Qv", *options.Address));
    }

    std::vector<TString> stateStrings;
    for (auto state : states) {
        stateStrings.push_back(Format("%Qv", FormatEnum(state)));
    }
    builder.AddWhereConjunct(Format("job_state IN (%v)", JoinToString(stateStrings, AsStringBuf(", "))));

    return builder;
}

// Asynchronously perform "select_rows" from job archive and parse result.
//
// |Offset| and |Limit| fields in |options| are ignored, |limit| is used instead.
// Jobs are additionally filtered by |states|.
TFuture<std::vector<TJob>> TClient::DoListJobsFromArchiveAsyncImpl(
    TOperationId operationId,
    const std::vector<EJobState>& states,
    i64 limit,
    const TSelectRowsOptions& selectRowsOptions,
    const TListJobsOptions& options)
{
    auto builder = GetListJobsQueryBuilder(operationId, states, options);

    builder.SetLimit(limit);

    auto jobIdHiIndex = builder.AddSelectExpression("job_id_hi");
    auto jobIdLoIndex = builder.AddSelectExpression("job_id_lo");
    auto typeIndex = builder.AddSelectExpression("type", "job_type");
    auto stateIndex = builder.AddSelectExpression("if(is_null(state), transient_state, state)", "job_state");
    auto startTimeIndex = builder.AddSelectExpression("start_time");
    auto finishTimeIndex = builder.AddSelectExpression("finish_time");
    auto addressIndex = builder.AddSelectExpression("address");
    auto errorIndex = builder.AddSelectExpression("error");
    auto statisticsIndex = builder.AddSelectExpression("statistics");
    auto stderrSizeIndex = builder.AddSelectExpression("stderr_size");
    auto hasSpecIndex = builder.AddSelectExpression("has_spec");
    auto failContextSizeIndex = builder.AddSelectExpression("fail_context_size");

    if (options.WithStderr) {
        if (*options.WithStderr) {
            builder.AddWhereConjunct("stderr_size != 0 AND NOT is_null(stderr_size)");
        } else {
            builder.AddWhereConjunct("stderr_size = 0 OR is_null(stderr_size)");
        }
    }

    if (options.WithSpec) {
        if (*options.WithSpec) {
            builder.AddWhereConjunct("has_spec AND NOT is_null(has_spec)");
        } else {
            builder.AddWhereConjunct("NOT has_spec OR is_null(has_spec)");
        }
    }

    if (options.WithFailContext) {
        if (*options.WithFailContext) {
            builder.AddWhereConjunct("fail_context_size != 0 AND NOT is_null(fail_context_size)");
        } else {
            builder.AddWhereConjunct("fail_context_size = 0 OR is_null(fail_context_size)");
        }
    }

    if (options.Type) {
        builder.AddWhereConjunct(Format("job_type = %Qv", FormatEnum(*options.Type)));
    }

    if (options.State) {
        builder.AddWhereConjunct(Format("job_state = %Qv", FormatEnum(*options.State)));
    }

    if (options.SortField != EJobSortField::None) {
        EOrderByDirection orderByDirection;
        switch (options.SortOrder) {
            case EJobSortDirection::Ascending:
                orderByDirection = EOrderByDirection::Ascending;
                break;
            case EJobSortDirection::Descending:
                orderByDirection = EOrderByDirection::Descending;
                break;
            default:
                YT_ABORT();
        }
        switch (options.SortField) {
            case EJobSortField::Type:
                builder.AddOrderByExpression("job_type", orderByDirection);
                break;
            case EJobSortField::State:
                builder.AddOrderByExpression("job_state", orderByDirection);
                break;
            case EJobSortField::StartTime:
                builder.AddOrderByExpression("start_time", orderByDirection);
                break;
            case EJobSortField::FinishTime:
                builder.AddOrderByExpression("finish_time", orderByDirection);
                break;
            case EJobSortField::Address:
                builder.AddOrderByExpression("address", orderByDirection);
                break;
            case EJobSortField::Duration:
                builder.AddOrderByExpression(
                    Format(
                        "if(is_null(finish_time), %v, finish_time) - start_time",
                        TInstant::Now().MicroSeconds()),
                    orderByDirection);
                break;
            case EJobSortField::Id:
                builder.AddOrderByExpression("format_guid(job_id_hi, job_id_lo)", orderByDirection);
                break;
            case EJobSortField::Progress:
                // XXX: progress is not present in archive table.
                break;
            default:
                YT_ABORT();
        }
    }

    return SelectRows(builder.Build(), selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
        std::vector<TJob> jobs;
        auto rows = result.Rowset->GetRows();
        jobs.reserve(rows.Size());
        for (auto row : rows) {
            ValidateNotNull(row[jobIdHiIndex], "job_id_hi", operationId, TJobId());
            ValidateNotNull(row[jobIdLoIndex], "job_id_lo", operationId, TJobId());

            TJobId jobId(row[jobIdHiIndex].Data.Uint64, row[jobIdLoIndex].Data.Uint64);

            jobs.emplace_back();
            auto& job = jobs.back();

            job.Id = jobId;

            ValidateNotNull(row[typeIndex], "type", operationId, jobId);
            job.Type = ParseEnum<EJobType>(TString(row[typeIndex].Data.String, row[typeIndex].Length));

            ValidateNotNull(row[stateIndex], "state", operationId, jobId);
            job.State = ParseEnum<EJobState>(TString(row[stateIndex].Data.String, row[stateIndex].Length));

            if (row[startTimeIndex].Type != EValueType::Null) {
                job.StartTime = TInstant::MicroSeconds(row[startTimeIndex].Data.Int64);
            }

            if (row[finishTimeIndex].Type != EValueType::Null) {
                job.FinishTime = TInstant::MicroSeconds(row[finishTimeIndex].Data.Int64);
            }

            if (row[addressIndex].Type != EValueType::Null) {
                job.Address = TString(row[addressIndex].Data.String, row[addressIndex].Length);
            }

            if (row[stderrSizeIndex].Type != EValueType::Null) {
                job.StderrSize = row[stderrSizeIndex].Data.Uint64;
            }

            if (row[failContextSizeIndex].Type != EValueType::Null) {
                job.FailContextSize = row[failContextSizeIndex].Data.Uint64;
            }

            if (row[hasSpecIndex].Type != EValueType::Null) {
                job.HasSpec = row[hasSpecIndex].Data.Boolean;
            }

            if (row[errorIndex].Type != EValueType::Null) {
                job.Error = TYsonString(TString(row[errorIndex].Data.String, row[errorIndex].Length));
            }

            if (row[statisticsIndex].Type != EValueType::Null) {
                auto briefStatisticsYson = TYsonString(TString(row[statisticsIndex].Data.String, row[statisticsIndex].Length));
                auto briefStatistics = ConvertToNode(briefStatisticsYson);

                // See BuildBriefStatistics.
                auto rowCount = FindNodeByYPath(briefStatistics, "/data/input/row_count/sum");
                auto uncompressedDataSize = FindNodeByYPath(briefStatistics, "/data/input/uncompressed_data_size/sum");
                auto compressedDataSize = FindNodeByYPath(briefStatistics, "/data/input/compressed_data_size/sum");
                auto dataWeight = FindNodeByYPath(briefStatistics, "/data/input/data_weight/sum");
                auto inputPipeIdleTime = FindNodeByYPath(briefStatistics, "/user_job/pipes/input/idle_time/sum");
                auto jobProxyCpuUsage = FindNodeByYPath(briefStatistics, "/job_proxy/cpu/user/sum");

                job.BriefStatistics = BuildYsonStringFluently()
                    .BeginMap()
                        .DoIf(static_cast<bool>(rowCount), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_row_count").Value(rowCount->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(uncompressedDataSize), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_uncompressed_data_size").Value(uncompressedDataSize->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(compressedDataSize), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_compressed_data_size").Value(compressedDataSize->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(dataWeight), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_data_weight").Value(dataWeight->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(inputPipeIdleTime), [&] (TFluentMap fluent) {
                            fluent.Item("input_pipe_idle_time").Value(inputPipeIdleTime->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(jobProxyCpuUsage), [&] (TFluentMap fluent) {
                            fluent.Item("job_proxy_cpu_usage").Value(jobProxyCpuUsage->AsInt64()->GetValue());
                        })
                    .EndMap();
            }
        }
        return jobs;
    }));
}

// Get statistics for jobs.
// Jobs are additionally filtered by |states|.
TFuture<TListJobsStatistics> TClient::ListJobsStatisticsFromArchiveAsync(
    TOperationId operationId,
    const std::vector<EJobState>& states,
    const TSelectRowsOptions& selectRowsOptions,
    const TListJobsOptions& options)
{
    auto builder = GetListJobsQueryBuilder(operationId, states, options);

    auto jobTypeIndex = builder.AddSelectExpression("type", "job_type");
    auto jobStateIndex = builder.AddSelectExpression("if(is_null(state), transient_state, state)", "job_state");
    auto countIndex = builder.AddSelectExpression("sum(1)", "count");

    builder.AddGroupByExpression("job_type");
    builder.AddGroupByExpression("job_state");

    return SelectRows(builder.Build(), selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
        TListJobsStatistics statistics;
        for (auto row : result.Rowset->GetRows()) {
            ValidateNotNull(row[jobTypeIndex], "type", operationId);
            auto jobType = ParseEnum<EJobType>(FromUnversionedValue<TStringBuf>(row[jobTypeIndex]));
            ValidateNotNull(row[jobStateIndex], "state", operationId);
            auto jobState = ParseEnum<EJobState>(FromUnversionedValue<TStringBuf>(row[jobStateIndex]));
            auto count = FromUnversionedValue<i64>(row[countIndex]);

            statistics.TypeCounts[jobType] += count;
            if (options.Type && *options.Type != jobType) {
                continue;
            }

            statistics.StateCounts[jobState] += count;
            if (options.State && *options.State != jobState) {
                continue;
            }
        }
        return statistics;
    }));
}

// Retrieves:
// 1) Filtered finished jobs (with limit).
// 2) All (non-filtered and without limit) in-progress jobs (if |includeInProgressJobs == true|).
// 3) Statistics for finished jobs.
TFuture<TClient::TListJobsFromArchiveResult> TClient::DoListJobsFromArchiveAsync(
    TOperationId operationId,
    TInstant deadline,
    bool includeInProgressJobs,
    const TListJobsOptions& options)
{
    std::vector<EJobState> inProgressJobStates;
    std::vector<EJobState> finishedJobStates;
    for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
        if (IsJobInProgress(state)) {
            inProgressJobStates.push_back(state);
        } else {
            finishedJobStates.push_back(state);
        }
    }

    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
    selectRowsOptions.Timeout = deadline - Now();
    selectRowsOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectRowsOptions.MemoryLimitPerNode = 100_MB;

    TFuture<std::vector<TJob>> jobsInProgressFuture;
    if (includeInProgressJobs) {
        jobsInProgressFuture = DoListJobsFromArchiveAsyncImpl(
            operationId,
            inProgressJobStates,
            ListJobsFromArchiveInProgressJobLimit,
            selectRowsOptions,
            options);
    } else {
        jobsInProgressFuture = MakeFuture(std::vector<TJob>{});
    }

    auto finishedJobsFuture = DoListJobsFromArchiveAsyncImpl(
        operationId,
        finishedJobStates,
        options.Limit + options.Offset,
        selectRowsOptions,
        options);

    auto finishedJobsStatisticsFuture = ListJobsStatisticsFromArchiveAsync(
        operationId,
        finishedJobStates,
        selectRowsOptions,
        options);

    return CombineAll(std::vector<TFuture<void>>{
            jobsInProgressFuture.As<void>(),
            finishedJobsFuture.As<void>(),
            finishedJobsStatisticsFuture.As<void>()})
        .Apply(BIND([jobsInProgressFuture, finishedJobsFuture, finishedJobsStatisticsFuture] (const std::vector<TError>&) {
            auto& jobsInProgressOrError = jobsInProgressFuture.Get();
            auto& finishedJobsOrError = finishedJobsFuture.Get();
            auto& statisticsOrError = finishedJobsStatisticsFuture.Get();

            if (!jobsInProgressOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get jobs in progress from the operation archive")
                    << jobsInProgressOrError;
            }
            if (!finishedJobsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get finished jobs from the operation archive")
                    << jobsInProgressOrError;
            }
            if (!statisticsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get finished job statistics from the operation archive")
                    << statisticsOrError;
            }

            auto difference = [] (std::vector<TJob> origin, const std::vector<TJob>& blacklist) {
                THashSet<TJobId> idBlacklist;
                for (const auto& job : blacklist) {
                    idBlacklist.emplace(job.Id);
                }
                origin.erase(
                    std::remove_if(
                        origin.begin(),
                        origin.end(),
                        [&idBlacklist] (const TJob& job) {
                            return idBlacklist.contains(job.Id);
                        }),
                    origin.end());
                return origin;
            };

            TListJobsFromArchiveResult result;
            result.FinishedJobs = std::move(finishedJobsOrError.Value());
            // If a job is present in both lists, we give prority
            // to |FinishedJobs| and remove it from |InProgressJobs|.
            result.InProgressJobs = difference(std::move(jobsInProgressOrError.Value()), result.FinishedJobs);
            result.FinishedJobsStatistics = std::move(statisticsOrError.Value());
            return result;
        }));
}

TFuture<std::pair<std::vector<TJob>, int>> TClient::DoListJobsFromCypressAsync(
    TOperationId operationId,
    TInstant deadline,
    const TListJobsOptions& options)
{
    TObjectServiceProxy proxy(GetOperationArchiveChannel(options.ReadFrom));

    auto attributeFilter = std::vector<TString>{
        "job_type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "error",
        "brief_statistics",
        "input_paths",
        "core_infos",
        "uncompressed_data_size"
    };

    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetTimeout(deadline - Now());

    {
        auto getReq = TYPathProxy::Get(GetJobsPath(operationId));
        ToProto(getReq->mutable_attributes()->mutable_keys(), attributeFilter);
        batchReq->AddRequest(getReq, "get_jobs");
    }

    return batchReq->Invoke().Apply(BIND([&options] (
        const TErrorOr<TObjectServiceProxy::TRspExecuteBatchPtr>& batchRspOrError)
    {
        const auto& batchRsp = batchRspOrError.ValueOrThrow();
        auto getReqRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");

        const auto& rsp = getReqRsp.ValueOrThrow();

        std::pair<std::vector<TJob>, int> result;
        auto& jobs = result.first;

        auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
        result.second = items->GetChildren().size();

        for (const auto& item : items->GetChildren()) {
            const auto& attributes = item.second->Attributes();
            auto children = item.second->AsMap();

            auto id = TGuid::FromString(item.first);

            auto type = ParseEnum<NJobTrackerClient::EJobType>(attributes.Get<TString>("job_type"));
            auto state = ParseEnum<NJobTrackerClient::EJobState>(attributes.Get<TString>("state"));
            auto address = attributes.Get<TString>("address");

            i64 stderrSize = -1;
            if (auto stderrNode = children->FindChild("stderr")) {
                stderrSize = stderrNode->Attributes().Get<i64>("uncompressed_data_size");
            }

            if (options.WithStderr) {
                if (*options.WithStderr && stderrSize <= 0) {
                    continue;
                }
                if (!(*options.WithStderr) && stderrSize > 0) {
                    continue;
                }
            }

            i64 failContextSize = -1;
            if (auto failContextNode = children->FindChild("fail_context")) {
                failContextSize = failContextNode->Attributes().Get<i64>("uncompressed_data_size");
            }

            if (options.WithFailContext) {
                if (*options.WithFailContext && failContextSize <= 0) {
                    continue;
                }
                if (!(*options.WithFailContext) && failContextSize > 0) {
                    continue;
                }
            }

            jobs.emplace_back();
            auto& job = jobs.back();

            job.Id = id;
            job.Type = type;
            job.State = state;
            job.StartTime = ConvertTo<TInstant>(attributes.Get<TString>("start_time"));
            job.FinishTime = ConvertTo<TInstant>(attributes.Get<TString>("finish_time"));
            job.Address = address;
            if (stderrSize >= 0) {
                job.StderrSize = stderrSize;
            }
            if (failContextSize >= 0) {
                job.FailContextSize = failContextSize;
            }
            job.HasSpec = true;
            job.Error = attributes.FindYson("error");
            job.BriefStatistics = attributes.FindYson("brief_statistics");
            job.InputPaths = attributes.FindYson("input_paths");
            job.CoreInfos = attributes.FindYson("core_infos");
        }

        return result;
    }));
}

TFuture<std::pair<std::vector<TJob>, int>> TClient::DoListJobsFromControllerAgentAsync(
    TOperationId operationId,
    const std::optional<TString>& controllerAgentAddress,
    TInstant deadline,
    const TListJobsOptions& options)
{
    TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    proxy.SetDefaultTimeout(deadline - Now());

    if (!controllerAgentAddress) {
        return MakeFuture(std::pair<std::vector<TJob>, int>{});
    }

    auto path = GetControllerAgentOrchidOperationPath(*controllerAgentAddress, operationId) + "/running_jobs";
    auto getReq = TYPathProxy::Get(path);

    return proxy.Execute(getReq).Apply(BIND([options] (const TYPathProxy::TRspGetPtr& rsp) {
        std::pair<std::vector<TJob>, int> result;
        auto& jobs = result.first;

        auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
        result.second = items->GetChildren().size();

        for (const auto& item : items->GetChildren()) {
            auto values = item.second->AsMap();

            auto id = TGuid::FromString(item.first);

            auto type = ParseEnum<NJobTrackerClient::EJobType>(
                values->GetChild("job_type")->AsString()->GetValue());
            auto state = ParseEnum<NJobTrackerClient::EJobState>(
                values->GetChild("state")->AsString()->GetValue());
            auto address = values->GetChild("address")->AsString()->GetValue();

            auto stderrSize = values->GetChild("stderr_size")->AsInt64()->GetValue();

            if (options.WithStderr && *options.WithStderr != (stderrSize > 0)) {
                continue;
            }

            if (options.WithFailContext && *options.WithFailContext) {
                continue;
            }

            jobs.emplace_back();
            auto& job = jobs.back();

            job.Id = id;
            job.Type = type;
            job.State = state;
            job.StartTime = ConvertTo<TInstant>(values->GetChild("start_time")->AsString()->GetValue());
            job.Address = address;
            job.HasSpec = true;
            job.Progress = values->GetChild("progress")->AsDouble()->GetValue();
            if (stderrSize > 0) {
                job.StderrSize = stderrSize;
            }
            job.BriefStatistics = ConvertToYsonString(values->GetChild("brief_statistics"));
        }

        return result;
    }));
}

std::function<bool(const TJob&, const TJob&)> TClient::GetJobsComparator(EJobSortField sortField, EJobSortDirection sortOrder)
{
    auto makeLessBy = [sortOrder] (auto transform) -> std::function<bool(const TJob&, const TJob&)> {
        switch (sortOrder) {
            case EJobSortDirection::Ascending:
                return [=] (const TJob& lhs, const TJob& rhs) {
                    return transform(lhs) < transform(rhs);
                };
            case EJobSortDirection::Descending:
                return [=] (const TJob& lhs, const TJob& rhs) {
                    return transform(rhs) < transform(lhs);
                };
            default:
                YT_ABORT();
        }
    };

    auto makeLessByField = [&] (auto TJob::* field) {
        return makeLessBy([field] (const TJob& job) {
            return job.*field;
        });
    };

    auto makeLessByFormattedEnumField = [&] (auto TJob::* field) {
        return makeLessBy([field] (const TJob& job) {
            return FormatEnum(job.*field);
        });
    };

    switch (sortField) {
        case EJobSortField::Type:
            return makeLessByFormattedEnumField(&TJob::Type);
        case EJobSortField::State:
            return makeLessByFormattedEnumField(&TJob::State);
        case EJobSortField::StartTime:
            return makeLessByField(&TJob::StartTime);
        case EJobSortField::FinishTime:
            return makeLessByField(&TJob::FinishTime);
        case EJobSortField::Address:
            return makeLessByField(&TJob::Address);
        case EJobSortField::Progress:
            return makeLessByField(&TJob::Progress);
        case EJobSortField::None:
            return makeLessByField(&TJob::Id);
        case EJobSortField::Id:
            return makeLessBy([] (const TJob& job) {
                return ToString(job.Id);
            });
        case EJobSortField::Duration:
            return makeLessBy([now = TInstant::Now()] (const TJob& job) {
                return (job.FinishTime ? *job.FinishTime : now) - job.StartTime;
            });
        default:
            YT_ABORT();
    }
}

void TClient::UpdateJobsList(std::vector<TJob> delta, std::vector<TJob>* origin, bool ignoreNewJobs)
{
    auto mergeJob = [] (TJob* target, TJob&& source) {
#define MERGE_FIELD(name) target->name = std::move(source.name)
#define MERGE_NULLABLE_FIELD(name) \
        if (source.name) { \
            target->name = std::move(source.name); \
        }
        MERGE_FIELD(Type);
        MERGE_FIELD(State);
        MERGE_FIELD(StartTime);
        MERGE_NULLABLE_FIELD(FinishTime);
        MERGE_FIELD(Address);
        MERGE_NULLABLE_FIELD(Progress);
        MERGE_NULLABLE_FIELD(StderrSize);
        MERGE_NULLABLE_FIELD(Error);
        MERGE_NULLABLE_FIELD(BriefStatistics);
        MERGE_NULLABLE_FIELD(InputPaths);
        MERGE_NULLABLE_FIELD(CoreInfos);
#undef MERGE_FIELD
#undef MERGE_NULLABLE_FIELD
    };

    THashMap<TJobId, TJob*> originMap;
    for (auto& job : *origin) {
        originMap.emplace(job.Id, &job);
    }
    // NB(levysotsky): We cannot insert directly into |origin|
    // as this can invalidate pointers stored in |originMap|.
    std::vector<TJob> actualDelta;
    for (auto& job : delta) {
        auto originMapIt = originMap.find(job.Id);
        if (originMapIt != originMap.end()) {
            mergeJob(originMapIt->second, std::move(job));
        } else if (!ignoreNewJobs) {
            actualDelta.push_back(std::move(job));
        }
    }
    origin->insert(
        origin->end(),
        std::make_move_iterator(actualDelta.begin()),
        std::make_move_iterator(actualDelta.end()));
}

TListJobsResult TClient::DoListJobs(
    TOperationId operationId,
    const TListJobsOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultListJobsTimeout);
    auto deadline = timeout.ToDeadLine();

    auto controllerAgentAddress = GetControllerAgentAddressFromCypress(
        operationId,
        GetMasterChannelOrThrow(EMasterChannelKind::Follower));

    auto dataSource = options.DataSource;
    if (dataSource == EDataSource::Auto) {
        if (controllerAgentAddress) {
            dataSource = EDataSource::Runtime;
        } else {
            dataSource = EDataSource::Archive;
        }
    }

    bool includeCypress;
    bool includeControllerAgent;
    bool includeArchive;

    switch (dataSource) {
        case EDataSource::Archive:
            includeCypress = false;
            includeControllerAgent = true;
            includeArchive = true;
            break;
        case EDataSource::Runtime:
            includeCypress = true;
            includeControllerAgent = true;
            includeArchive = false;
            break;
        case EDataSource::Manual:
            THROW_ERROR_EXCEPTION("\"manual\" mode is deprecated and forbidden");
        default:
            YT_ABORT();
    }

    YT_LOG_DEBUG("Starting list jobs (IncludeCypress: %v, IncludeControllerAgent: %v, IncludeArchive: %v)",
        includeCypress,
        includeControllerAgent,
        includeArchive);

    TFuture<std::pair<std::vector<TJob>, int>> cypressResultFuture;
    TFuture<std::pair<std::vector<TJob>, int>> controllerAgentResultFuture;
    TFuture<TListJobsFromArchiveResult> archiveResultFuture;

    // Issue the requests in parallel.

    if (includeArchive) {
        if (DoesOperationsArchiveExist()) {
            archiveResultFuture = DoListJobsFromArchiveAsync(
                operationId,
                deadline,
                /* includeInProgressJobs */ controllerAgentAddress.has_value(),
                options);
        } else {
            archiveResultFuture = MakeFuture(TListJobsFromArchiveResult{});
        }
    }

    if (includeCypress) {
        cypressResultFuture = DoListJobsFromCypressAsync(operationId, deadline, options);
    }

    if (includeControllerAgent) {
        controllerAgentResultFuture = DoListJobsFromControllerAgentAsync(
            operationId,
            controllerAgentAddress,
            deadline,
            options);
    }

    // Wait for results and combine them.

    TListJobsResult result;

    std::vector<TJob> controllerAgentJobs;

    if (includeControllerAgent) {
        auto controllerAgentResultOrError = WaitFor(controllerAgentResultFuture);
        if (controllerAgentResultOrError.IsOK()) {
            auto& [jobs, jobCount] = controllerAgentResultOrError.Value();
            result.ControllerAgentJobCount = jobCount;
            controllerAgentJobs = std::move(jobs);
        } else if (controllerAgentResultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No such operation in the controller agent.
            result.ControllerAgentJobCount = 0;
        } else {
            result.Errors.push_back(std::move(controllerAgentResultOrError));
        }
    }

    auto countAndFilterJobs = [&options] (std::vector<TJob> jobs, TListJobsStatistics* statistics) {
        std::vector<TJob> filteredJobs;
        for (auto& job : jobs) {
            if (options.Address && job.Address != *options.Address) {
                continue;
            }

            statistics->TypeCounts[job.Type] += 1;
            if (options.Type && job.Type != *options.Type) {
                continue;
            }

            statistics->StateCounts[job.State] += 1;
            if (options.State && job.State != *options.State) {
                continue;
            }

            filteredJobs.push_back(std::move(job));
        }
        return filteredJobs;
    };

    switch (dataSource) {
        case EDataSource::Archive: {
            TListJobsFromArchiveResult archiveResult;

            auto archiveResultOrError = WaitFor(archiveResultFuture);
            if (archiveResultOrError.IsOK()) {
                archiveResult = std::move(archiveResultOrError.Value());
                result.ArchiveJobCount = archiveResult.InProgressJobs.size();
                for (auto count : archiveResult.FinishedJobsStatistics.TypeCounts) {
                    *result.ArchiveJobCount += count;
                }
                result.Statistics = archiveResult.FinishedJobsStatistics;
            } else {
                result.Errors.push_back(TError(
                    EErrorCode::JobArchiveUnavailable,
                    "Job archive is unavailable")
                    << archiveResultOrError);
            }

            if (!controllerAgentAddress) {
                result.Jobs = std::move(archiveResult.FinishedJobs);
            } else {
                auto inProgressJobs = std::move(archiveResult.InProgressJobs);
                UpdateJobsList(std::move(controllerAgentJobs), &inProgressJobs, /* ignoreNewJobs */ !inProgressJobs.empty());
                auto filteredInProgressJobs = countAndFilterJobs(std::move(inProgressJobs), &result.Statistics);
                auto jobComparator = GetJobsComparator(options.SortField, options.SortOrder);
                std::sort(filteredInProgressJobs.begin(), filteredInProgressJobs.end(), jobComparator);
                result.Jobs.reserve(filteredInProgressJobs.size() + archiveResult.FinishedJobs.size());
                std::merge(
                    std::make_move_iterator(filteredInProgressJobs.begin()),
                    std::make_move_iterator(filteredInProgressJobs.end()),
                    std::make_move_iterator(archiveResult.FinishedJobs.begin()),
                    std::make_move_iterator(archiveResult.FinishedJobs.end()),
                    std::back_inserter(result.Jobs),
                    jobComparator);
            }
            break;
        }
        case EDataSource::Runtime: {
            auto cypressResultOrError = WaitFor(cypressResultFuture);
            std::vector<TJob> cypressJobs;
            if (cypressResultOrError.IsOK()) {
                auto& [jobs, cypressJobCount] = cypressResultOrError.Value();
                result.CypressJobCount = cypressJobCount;
                cypressJobs = std::move(jobs);
            } else if (cypressResultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in Cypress.
                result.CypressJobCount = 0;
            } else {
                result.Errors.push_back(TError("Failed to get jobs from Cypress") << cypressResultOrError);
            }

            UpdateJobsList(std::move(controllerAgentJobs), &cypressJobs, /* ignoreNewJobs */ false);
            result.Jobs = countAndFilterJobs(std::move(cypressJobs), &result.Statistics);
            auto jobComparator = GetJobsComparator(options.SortField, options.SortOrder);
            std::sort(result.Jobs.begin(), result.Jobs.end(), jobComparator);

            break;
        }
        default:
            YT_ABORT();
    }

    auto beginIt = std::min(result.Jobs.end(), result.Jobs.begin() + options.Offset);
    auto endIt = std::min(result.Jobs.end(), beginIt + options.Limit);
    result.Jobs = std::vector<TJob>(std::make_move_iterator(beginIt), std::make_move_iterator(endIt));
    return result;
}

template <typename TValue>
void TClient::TryAddFluentItem(
    TFluentMap fluent,
    TStringBuf key,
    TUnversionedRow row,
    const NTableClient::TColumnFilter& columnFilter,
    int columnIndex)
{
    auto valueIndexOrNull = columnFilter.FindPosition(columnIndex);
    if (valueIndexOrNull && row[*valueIndexOrNull].Type != EValueType::Null) {
        fluent.Item(key).Value(FromUnversionedValue<TValue>(row[*valueIndexOrNull]));
    }
}

std::vector<TString> TClient::MakeJobArchiveAttributes(const THashSet<TString>& attributes)
{
    std::vector<TString> result;
    result.reserve(attributes.size() + 2); // Plus 2 as operation_id and job_id are split into hi and lo.
    for (const auto& attribute : attributes) {
        if (!SupportedJobAttributes.contains(attribute)) {
            THROW_ERROR_EXCEPTION("Job attribute %Qv is not supported", attribute);
        }
        if (attribute.EndsWith("_id")) {
            result.push_back(attribute + "_hi");
            result.push_back(attribute + "_lo");
        } else if (attribute == "state") {
            result.push_back("state");
            result.push_back("transient_state");
        } else {
            result.push_back(attribute);
        }
    }
    return result;
}

TYsonString TClient::DoGetJob(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetJobTimeout);
    auto deadline = timeout.ToDeadLine();

    TJobTableDescriptor table;
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TUnversionedRow> keys;
    auto key = rowBuffer->AllocateUnversioned(4);
    key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], table.Index.OperationIdHi);
    key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], table.Index.OperationIdLo);
    key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], table.Index.JobIdHi);
    key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], table.Index.JobIdLo);
    keys.push_back(key);

    TLookupRowsOptions lookupOptions;

    const THashSet<TString> DefaultAttributes = {
        "operation_id",
        "job_id",
        "type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "error",
        "statistics",
        "events",
        "has_spec",
    };

    std::vector<int> columnIndexes;
    auto fields = MakeJobArchiveAttributes(options.Attributes.value_or(DefaultAttributes));
    for (const auto& field : fields) {
        columnIndexes.push_back(table.NameTable->GetIdOrThrow(field));
    }

    lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timeout = deadline - Now();

    auto rowset = WaitFor(LookupRows(
        GetOperationsArchiveJobsPath(),
        table.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions))
        .ValueOrThrow();

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.Empty());
    auto row = rows[0];

    if (!row) {
        THROW_ERROR_EXCEPTION("No such job %v or operation %v", jobId, operationId);
    }

    const auto& columnFilter = lookupOptions.ColumnFilter;

    TStringBuf state;
    {
        auto indexOrNull = columnFilter.FindPosition(table.Index.State);
        if (indexOrNull && row[*indexOrNull].Type != EValueType::Null) {
            state = FromUnversionedValue<TStringBuf>(row[*indexOrNull]);
        }
    }
    if (!state.IsInited()) {
        auto indexOrNull = columnFilter.FindPosition(table.Index.TransientState);
        if (indexOrNull && row[*indexOrNull].Type != EValueType::Null) {
            state = FromUnversionedValue<TStringBuf>(row[*indexOrNull]);
        }
    }

    // NB: We need a separate function for |TInstant| because it has type "int64" in table
    // but |FromUnversionedValue<TInstant>| expects it to be "uint64".
    auto tryAddInstantFluentItem = [&] (TFluentMap fluent, TStringBuf key, int columnIndex) {
        auto valueIndexOrNull = columnFilter.FindPosition(columnIndex);
        if (valueIndexOrNull && row[*valueIndexOrNull].Type != EValueType::Null) {
            fluent.Item(key).Value(TInstant::MicroSeconds(row[*valueIndexOrNull].Data.Int64));
        }
    };

    using std::placeholders::_1;
    return BuildYsonStringFluently()
        .BeginMap()
            .DoIf(columnFilter.ContainsIndex(table.Index.OperationIdHi), [&] (TFluentMap fluent) {
                fluent.Item("operation_id").Value(operationId);
            })
            .DoIf(columnFilter.ContainsIndex(table.Index.JobIdHi), [&] (TFluentMap fluent) {
                fluent.Item("job_id").Value(jobId);
            })
            .DoIf(state.IsInited(), [&] (TFluentMap fluent) {
                fluent.Item("state").Value(state);
            })

            .Do(std::bind(tryAddInstantFluentItem, _1, "start_time", table.Index.StartTime))
            .Do(std::bind(tryAddInstantFluentItem, _1, "finish_time", table.Index.FinishTime))

            .Do(std::bind(TryAddFluentItem<bool>,        _1, "has_spec",   row, columnFilter, table.Index.HasSpec))
            .Do(std::bind(TryAddFluentItem<TString>,     _1, "address",    row, columnFilter, table.Index.Address))
            .Do(std::bind(TryAddFluentItem<TString>,     _1, "type",       row, columnFilter, table.Index.Type))
            .Do(std::bind(TryAddFluentItem<TYsonString>, _1, "error",      row, columnFilter, table.Index.Error))
            .Do(std::bind(TryAddFluentItem<TYsonString>, _1, "statistics", row, columnFilter, table.Index.Statistics))
            .Do(std::bind(TryAddFluentItem<TYsonString>, _1, "events",     row, columnFilter, table.Index.Events))
        .EndMap();
}

TYsonString TClient::DoStraceJob(
    TJobId jobId,
    const TStraceJobOptions& /*options*/)
{
    auto req = JobProberProxy_->Strace();
    ToProto(req->mutable_job_id(), jobId);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return TYsonString(rsp->trace());
}

void TClient::DoSignalJob(
    TJobId jobId,
    const TString& signalName,
    const TSignalJobOptions& /*options*/)
{
    auto req = JobProberProxy_->SignalJob();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_signal_name(), signalName);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoAbandonJob(
    TJobId jobId,
    const TAbandonJobOptions& /*options*/)
{
    auto req = JobProberProxy_->AbandonJob();
    ToProto(req->mutable_job_id(), jobId);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

TYsonString TClient::DoPollJobShell(
    TJobId jobId,
    const TYsonString& parameters,
    const TPollJobShellOptions& options)
{
    auto jobNodeDescriptor = GetJobNodeDescriptor(jobId, EPermissionSet(EPermission::Manage | EPermission::Read))
        .ValueOrThrow();
    auto nodeChannel = ChannelFactory_->CreateChannel(jobNodeDescriptor);

    YT_LOG_DEBUG("Polling job shell (JobId: %v)", jobId);

    NJobProberClient::TJobProberServiceProxy proxy(nodeChannel);

    auto spec = GetJobSpecFromJobNode(jobId, proxy)
        .ValueOrThrow();

    auto req = proxy.PollJobShell();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_parameters(), parameters.GetData());

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << TErrorAttribute("job_id", jobId)
            << rspOrError;
    }

    const auto& rsp = rspOrError.Value();
    return TYsonString(rsp->result());
}

void TClient::DoAbortJob(
    TJobId jobId,
    const TAbortJobOptions& options)
{
    auto req = JobProberProxy_->AbortJob();
    ToProto(req->mutable_job_id(), jobId);
    if (options.InterruptTimeout) {
        req->set_interrupt_timeout(ToProto<i64>(*options.InterruptTimeout));
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

TClusterMeta TClient::DoGetClusterMeta(
    const TGetClusterMetaOptions& options)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    auto req = TMasterYPathProxy::GetClusterMeta();
    req->set_populate_node_directory(options.PopulateNodeDirectory);
    req->set_populate_cluster_directory(options.PopulateClusterDirectory);
    req->set_populate_medium_directory(options.PopulateMediumDirectory);
    SetCachingHeader(req, options);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspGetClusterMeta>(0)
        .ValueOrThrow();

    TClusterMeta meta;
    if (options.PopulateNodeDirectory) {
        meta.NodeDirectory = std::make_shared<NNodeTrackerClient::NProto::TNodeDirectory>();
        meta.NodeDirectory->Swap(rsp->mutable_node_directory());
    }
    if (options.PopulateClusterDirectory) {
        meta.ClusterDirectory = std::make_shared<NHiveClient::NProto::TClusterDirectory>();
        meta.ClusterDirectory->Swap(rsp->mutable_cluster_directory());
    }
    if (options.PopulateMediumDirectory) {
        meta.MediumDirectory = std::make_shared<NChunkClient::NProto::TMediumDirectory>();
        meta.MediumDirectory->Swap(rsp->mutable_medium_directory());
    }
    return meta;
}

bool TClient::TryParseObjectId(const TYPath& path, TObjectId* objectId)
{
    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() != NYPath::ETokenType::Literal) {
        return false;
    }

    auto token = tokenizer.GetToken();
    if (!token.StartsWith(ObjectIdPathPrefix)) {
        return false;
    }

    *objectId = TObjectId::FromString(token.SubString(ObjectIdPathPrefix.length(), token.length() - ObjectIdPathPrefix.length()));
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
