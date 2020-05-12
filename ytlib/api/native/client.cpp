#include "client_impl.h"
#include "box.h"
#include "connection.h"
#include "list_operations.h"
#include "rpc_helpers.h"
#include "transaction.h"
#include "private.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/operation_archive_schema.h>
#include <yt/client/api/file_reader.h>
#include <yt/client/api/file_writer.h>
#include <yt/client/api/journal_reader.h>
#include <yt/client/api/journal_writer.h>

#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/job_tracker_client/helpers.h>

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

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/ytlib/job_proxy/job_spec_helper.h>
#include <yt/ytlib/job_proxy/helpers.h>
#include <yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/ytlib/job_prober_client/job_node_descriptor_cache.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/query_client/functions_cache.h>
#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/coordinator.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>
#include <yt/client/scheduler/operation_id_or_alias.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/async_stream_pipe.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/rpc/helpers.h>

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

TUnversionedRow CreateOperationKey(
    const TOperationId& operationId,
    const TOrderedByIdTableDescriptor::TIndex& index,
    const TRowBufferPtr& rowBuffer)
{
    auto key = rowBuffer->AllocateUnversioned(2);
    key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], index.IdHi);
    key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], index.IdLo);
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

constexpr i64 ListJobsFromArchiveInProgressJobLimit = 100000;

TYPath GetControllerAgentOrchidRunningJobsPath(TStringBuf controllerAgentAddress, TOperationId operationId)
{
    return GetControllerAgentOrchidOperationPath(controllerAgentAddress, operationId) + "/running_jobs";
}

TYPath GetControllerAgentOrchidRetainedFinishedJobsPath(TStringBuf controllerAgentAddress, TOperationId operationId)
{
    return GetControllerAgentOrchidOperationPath(controllerAgentAddress, operationId) + "/retained_finished_jobs";
}

} // namespace

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
            TransferResultFuture_.Cancel(TError("Reader destroyed"));
        }
    }

    void Open()
    {
        auto transferClosure = UserJobReadController_->PrepareJobInputTransfer(AsyncStreamPipe_);
        TransferResultFuture_ = transferClosure
            .AsyncVia(Invoker_)
            .Run();

        TransferResultFuture_.Subscribe(BIND([pipe = AsyncStreamPipe_] (const TError& error) {
            if (!error.IsOK()) {
                pipe->Abort(TError("Failed to get job input") << error);
            }
        }));
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

    SchedulerProxy_ = std::make_unique<TSchedulerServiceProxy>(GetSchedulerChannel());
    JobProberProxy_ = std::make_unique<TJobProberServiceProxy>(GetSchedulerChannel());

    TransactionManager_ = New<TTransactionManager>(
        Connection_,
        Options_.GetUser());

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

void TClient::Terminate()
{
    TransactionManager_->AbortAll();

    auto error = TError("Client terminated");

    for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
        for (const auto& pair : MasterChannels_[kind]) {
            auto channel = pair.second;
            channel->Terminate(error);
        }
    }
    SchedulerChannel_->Terminate(error);
}

const IChannelPtr& TClient::GetOperationArchiveChannel(EMasterChannelKind kind)
{
    {
        auto guard = Guard(OperationsArchiveChannelsLock_);
        if (OperationsArchiveChannels_) {
            return (*OperationsArchiveChannels_)[kind];
        }
    }

    TEnumIndexedVector<EMasterChannelKind, NRpc::IChannelPtr> channels;
    for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
        // NOTE(asaitgalin): Cache is tied to user so to utilize cache properly all Cypress
        // requests for operations archive should be performed under the same user.
        channels[kind] = CreateAuthenticatedChannel(
            Connection_->GetMasterChannelOrThrow(kind, PrimaryMasterCellTag),
            OperationsClientUserName);
    }

    {
        auto guard = Guard(OperationsArchiveChannelsLock_);
        if (!OperationsArchiveChannels_) {
            OperationsArchiveChannels_ = std::move(channels);
        }
        return (*OperationsArchiveChannels_)[kind];
    }
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
            this,
            this_ = MakeWeak(this)
        ] (TAsyncSemaphoreGuard /*guard*/) mutable {
            auto client = this_.Lock();
            if (!client) {
                return;
            }

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
    NApi::NNative::SetCachingHeader(request, Connection_->GetConfig(), options);
}

void TClient::SetBalancingHeader(
    const IClientRequestPtr& request,
    const TMasterReadOptions& options)
{
    NApi::NNative::SetBalancingHeader(request, Connection_->GetConfig(), options);
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

template std::unique_ptr<TObjectServiceProxy> TClient::CreateWriteProxy<TObjectServiceProxy>(TCellTag cellTag);
template std::unique_ptr<TChunkServiceProxy> TClient::CreateWriteProxy<TChunkServiceProxy>(TCellTag cellTag);

IChannelPtr TClient::GetReadCellChannelOrThrow(TTabletCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(cellId);
    const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(cellDescriptor, EPeerKind::Leader);
    return ChannelFactory_->CreateChannel(primaryPeerDescriptor.GetAddressWithNetworkOrThrow(Connection_->GetNetworks()));
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
    // COMPAT(gritukan): Drop it.
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
    "job_competition_id",
    "has_competitors",
    "exec_attributes",
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
            result.emplace_back("key");
        } else if (attribute == "type") {
            result.emplace_back("operation_type");
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
            result.emplace_back("id_hi");
            result.emplace_back("id_lo");
        } else if (attribute == "type") {
            result.emplace_back("operation_type");
        } else if (attribute == "annotations") {
            // COMPAT(gritukan): This field is deprecated.
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
        .Run();
    getOperationFutures.push_back(cypressFuture);

    TFuture<TYsonString> archiveFuture = MakeFuture<TYsonString>(TYsonString());
    // We request state to distinguish controller agent's archive entries
    // from operation cleaner's ones (the latter must have "state" field).
    auto archiveOptions = options;
    if (archiveOptions.Attributes) {
        archiveOptions.Attributes->insert("state");
    }
    if (DoesOperationsArchiveExist()) {
        archiveFuture = BIND(&TClient::DoGetOperationFromArchive,
            MakeStrong(this),
            operationId,
            deadline,
            std::move(archiveOptions))
            .AsyncVia(Connection_->GetInvoker())
            .Run()
            .WithTimeout(options.ArchiveTimeout);
    }
    getOperationFutures.push_back(archiveFuture);

    auto getOperationResponses = WaitFor(CombineAll<TYsonString>(getOperationFutures))
        .ValueOrThrow();

    auto cypressResult = cypressFuture.Get()
        .ValueOrThrow();

    auto archiveResultOrError = archiveFuture.Get();
    TYsonString archiveResult;
    if (archiveResultOrError.IsOK()) {
        archiveResult = archiveResultOrError.Value();
    } else {
        YT_LOG_DEBUG("Failed to get information for operation from archive (OperationId: %v, Error: %v)",
            operationId,
            archiveResultOrError);
    }

    auto mergeResults = [] (const TYsonString& cypressResult, const TYsonString& archiveResult) {
        auto cypressResultNode = ConvertToNode(cypressResult);
        YT_VERIFY(cypressResultNode->GetType() == ENodeType::Map);
        const auto& cypressResultMap = cypressResultNode->AsMap();

        std::vector<TString> fieldNames = {"brief_progress", "progress"};
        for (const auto& fieldName : fieldNames) {
            auto cypressFieldNode = cypressResultMap->FindChild(fieldName);
            cypressResultMap->RemoveChild(fieldName);

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
                cypressResultMap->AddChild(fieldName, ConvertToNode(result));
            }
        }
        return ConvertToYsonString(cypressResultMap);
    };

    if (archiveResult && cypressResult) {
        return mergeResults(cypressResult, archiveResult);
    } else if (cypressResult) {
        return cypressResult;
    }

    // Check whether archive row was written by controller agent or operation cleaner.
    // Here we assume that controller agent does not write "state" field to the archive.
    auto isCompleteArchiveResult = [] (const TYsonString& archiveResult) {
        return TryGetString(archiveResult.GetData(), "/state").has_value();
    };

    if (archiveResult) {
        // We have a non-empty response from archive and an empty response from Cypress.
        // If the archive response is incomplete (i.e. written by controller agent),
        // we need to retry the archive request as there might be a race
        // between these two requests and operation archivation.
        //
        // ---------------------------------------------------> time
        //         |               |             |
        //    archive rsp.   archivation   cypress rsp.
        if (!isCompleteArchiveResult(archiveResult)) {
            YT_LOG_DEBUG("Got empty response from Cypress and incomplete response from archive, "
                "retrying (OperationId: %v)",
                operationId);
            archiveResult = DoGetOperationFromArchive(operationId, deadline, archiveOptions);
        }
    } else if (!archiveResultOrError.IsOK()) {
        // The operation is missing from Cypress and the archive request finished with errors.
        // If it is timeout error, we retry without timeout.
        // Otherwise we throw the error as there is no hope.
        if (!archiveResultOrError.FindMatching(NYT::EErrorCode::Timeout)) {
            archiveResultOrError.ThrowOnError();
        }
        archiveResult = DoGetOperationFromArchive(operationId, deadline, archiveOptions);
    }

    if (!archiveResult || !isCompleteArchiveResult(archiveResult)) {
        THROW_ERROR_EXCEPTION(
            NApi::EErrorCode::NoSuchOperation,
            "No such operation %v",
            operationId);
    }

    if (!options.Attributes || options.Attributes->contains("state")) {
        return archiveResult;
    }
    // Remove "state" field if it was not requested.
    auto archiveResultNode = ConvertToNode(archiveResult);
    YT_VERIFY(archiveResultNode->GetType() == ENodeType::Map);
    archiveResultNode->AsMap()->RemoveChild("state");
    return ConvertToYsonString(archiveResultNode);
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
TErrorOr<TNodeDescriptor> TClient::TryGetJobNodeDescriptor(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    const auto& cache = Connection_->GetJobNodeDescriptorCache();
    NJobProberClient::TJobNodeDescriptorKey key{
        .User = Options_.GetUser(),
        .JobId = jobId,
        .Permissions = requiredPermissions
    };
    return WaitFor(cache->Get(key));
}

IChannelPtr TClient::TryCreateChannelToJobNode(
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = TryGetJobNodeDescriptor(jobId, requiredPermissions);
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
        auto jobSpecOrError = TryGetJobSpecFromJobNode(jobId, nodeChannel);
        if (!jobSpecOrError.IsOK()) {
            return nullptr;
        }

        const auto& jobSpec = jobSpecOrError.Value();
        ValidateJobSpecVersion(jobId, jobSpec);
        ValidateOperationAccess(jobId, jobSpec, requiredPermissions);

        return nodeChannel;
    } catch (const TErrorException& ex) {
        YT_LOG_DEBUG(ex, "Failed create node channel to job using address from archive (JobId: %v)", jobId);
        return nullptr;
    }
}

TErrorOr<NJobTrackerClient::NProto::TJobSpec> TClient::TryGetJobSpecFromJobNode(
    TJobId jobId,
    const NRpc::IChannelPtr& nodeChannel)
{
    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);

    auto req = jobProberServiceProxy.GetSpec();
    ToProto(req->mutable_job_id(), jobId);

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        return TError("Failed to get job spec from job node")
            << std::move(rspOrError)
            << TErrorAttribute("job_id", jobId);
    }

    const auto& rsp = rspOrError.Value();
    const auto& spec = rsp->spec();
    ValidateJobSpecVersion(jobId, spec);
    return spec;
}

// Get job spec from node and check that user has |requiredPermissions|
// for accessing the corresponding operation.
TErrorOr<NJobTrackerClient::NProto::TJobSpec> TClient::TryGetJobSpecFromJobNode(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = TryGetJobNodeDescriptor(jobId, requiredPermissions);
    if (!jobNodeDescriptorOrError.IsOK()) {
        return TError(std::move(jobNodeDescriptorOrError));
    }

    const auto& nodeDescriptor = jobNodeDescriptorOrError.Value();
    auto nodeChannel = ChannelFactory_->CreateChannel(nodeDescriptor);
    return TryGetJobSpecFromJobNode(jobId, nodeChannel);
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
    auto jobSpecFromProxyOrError = TryGetJobSpecFromJobNode(jobId, EPermissionSet(EPermission::Read));
    if (!jobSpecFromProxyOrError.IsOK() && !IsNoSuchJobOrOperationError(jobSpecFromProxyOrError)) {
        THROW_ERROR jobSpecFromProxyOrError;
    }

    NJobTrackerClient::NProto::TJobSpec jobSpec;
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
    auto jobSpecFromProxyOrError = TryGetJobSpecFromJobNode(jobId, EPermissionSet(EPermission::Read));
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
    TListOperationsCountingFilter& countingFilter,
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

    TObjectServiceProxy proxy(GetOperationArchiveChannel(options.ReadFrom));
    auto listBatchReq = proxy.ExecuteBatch();
    SetBalancingHeader(listBatchReq, options);

    auto filteringAttributes = LightAttributes;
    if (options.SubstrFilter) {
        filteringAttributes.emplace("annotations");
    }
    auto filteringCypressAttributes = MakeCypressOperationAttributes(filteringAttributes);
    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto hashStr = Format("%02x", hash);
        auto req = TYPathProxy::List("//sys/operations/" + hashStr);
        SetCachingHeader(req, options);
        auto attributes = LightAttributes;
        if (options.SubstrFilter) {
            attributes.emplace("annotations");
        }
        ToProto(req->mutable_attributes()->mutable_keys(), filteringCypressAttributes);
        listBatchReq->AddRequest(req, "list_operations_" + hashStr);
    }

    auto listBatchRsp = WaitFor(listBatchReq->Invoke())
        .ValueOrThrow();

    std::vector<TYsonString> operationsYson;
    operationsYson.reserve(0xFF + 1);
    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto rspOrError = listBatchRsp->GetResponse<TYPathProxy::TRspList>(Format("list_operations_%02x", hash));
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            continue;
        }
        auto rsp = rspOrError.ValueOrThrow();
        operationsYson.emplace_back(rsp->value());
    }

    TListOperationsFilter filter(operationsYson, countingFilter, options);

    // Lookup all operations with currently filtered ids, add their brief progress.
    if (DoesOperationsArchiveExist()) {
        TOrderedByIdTableDescriptor tableDescriptor;
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> keys;
        keys.reserve(filter.GetCount());
        filter.ForEachOperationImmutable([&] (int index, const TListOperationsFilter::TLightOperation& lightOperation) {
            keys.push_back(CreateOperationKey(lightOperation.GetId(), tableDescriptor.Index, rowBuffer));
        });

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
            auto rows = rowsetOrError.ValueOrThrow()->GetRows();
            YT_VERIFY(rows.Size() == filter.GetCount());

            filter.ForEachOperationMutable([&] (int index, TListOperationsFilter::TLightOperation& lightOperation) {
                auto row = rows[index];
                if (!row) {
                    return;
                }
                auto position = lookupOptions.ColumnFilter.FindPosition(tableDescriptor.Index.BriefProgress);
                if (!position) {
                    return;
                }
                auto value = row[*position];
                if (value.Type == EValueType::Null) {
                    return;
                }
                YT_VERIFY(value.Type == EValueType::Any);
                lightOperation.UpdateBriefProgress(TStringBuf(value.Data.String, value.Length));
            });
        }
    }

    filter.OnBriefProgressFinished();

    auto areAllRequestedAttributesLight = std::all_of(
        requestedAttributes.begin(),
        requestedAttributes.end(),
        [&] (const TString& attribute) {
            return LightAttributes.contains(attribute);
        });
    if (!areAllRequestedAttributesLight) {
        auto getBatchReq = proxy.ExecuteBatch();
        SetBalancingHeader(getBatchReq, options);

        const auto cypressRequestedAttributes = MakeCypressOperationAttributes(requestedAttributes);
        filter.ForEachOperationImmutable([&] (int index, const TListOperationsFilter::TLightOperation& lightOperation) {
            auto req = TYPathProxy::Get(GetOperationPath(lightOperation.GetId()));
            SetCachingHeader(req, options);
            ToProto(req->mutable_attributes()->mutable_keys(), cypressRequestedAttributes);
            getBatchReq->AddRequest(req);
        });

        auto getBatchRsp = WaitFor(getBatchReq->Invoke())
            .ValueOrThrow();
        auto responses = getBatchRsp->GetResponses<TYPathProxy::TRspGet>();
        filter.ForEachOperationMutable([&] (int index, TListOperationsFilter::TLightOperation& lightOperation) {
            const auto& rspOrError = responses[index];
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                return;
            }
            lightOperation.SetYson(rspOrError.ValueOrThrow()->value());
        });
    }

    auto operations = filter.BuildOperations(requestedAttributes);
    idToOperation->reserve(idToOperation->size() + operations.size());
    for (auto& operation : operations) {
        (*idToOperation)[*operation.Id] = std::move(operation);
    }
}

// Searches in archive for operations satisfying given filters.
// Returns operations with requested fields plus necessarily "start_time" and "id".
THashMap<NScheduler::TOperationId, TOperation> TClient::DoListOperationsFromArchive(
    TInstant deadline,
    TListOperationsCountingFilter& countingFilter,
    const TListOperationsOptions& options)
{
    if (!options.FromTime) {
        THROW_ERROR_EXCEPTION("Missing required parameter \"from_time\"");
    }

    if (!options.ToTime) {
        THROW_ERROR_EXCEPTION("Missing required parameter \"to_time\"");
    }

    if (options.AccessFilter) {
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
                Format("is_substr(%Qv, filter_factors)", *options.SubstrFilter));
        }

        if (options.AccessFilter) {
            builder->AddWhereConjunct(Format("NOT is_null(acl) AND _yt_has_permissions(acl, %Qv, %Qv)",
                ConvertToYsonString(options.AccessFilter->SubjectTransitiveClosure, EYsonFormat::Text),
                ConvertToYsonString(options.AccessFilter->Permissions, EYsonFormat::Text)));
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
        auto hasFailedJobsIndex = builder.AddSelectExpression("has_failed_jobs");
        auto countIndex = builder.AddSelectExpression("sum(1)", "count");

        addCommonWhereConjuncts(&builder);

        builder.AddGroupByExpression("any_to_yson_string(pools)", "pools_str");
        builder.AddGroupByExpression("authenticated_user");
        builder.AddGroupByExpression("state");
        builder.AddGroupByExpression("operation_type");
        builder.AddGroupByExpression("pool");
        builder.AddGroupByExpression("has_failed_jobs");

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
            if (!countingFilter.Filter(pools, user, state, type, count)) {
                continue;
            }

            bool hasFailedJobs = false;
            if (row[hasFailedJobsIndex].Type != EValueType::Null) {
                hasFailedJobs = FromUnversionedValue<bool>(row[hasFailedJobsIndex]);
            }
            countingFilter.FilterByFailedJobs(hasFailedJobs, count);
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

    if (options.WithFailedJobs) {
        if (*options.WithFailedJobs) {
            builder.AddWhereConjunct("has_failed_jobs");
        } else {
            builder.AddWhereConjunct("not has_failed_jobs");
        }
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

    const THashSet<TString> RequiredAttributes = {"id", "start_time"};
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

    auto attributesToRequest = MakeFinalAttributeSet(
        options.Attributes,
        RequiredAttributes,
        DefaultAttributes,
        IgnoredAttributes);

    std::vector<int> columns;
    for (const auto& columnName : MakeArchiveOperationAttributes(attributesToRequest)) {
        columns.push_back(tableDescriptor.NameTable->GetIdOrThrow(columnName));
    }

    const NTableClient::TColumnFilter columnFilter(columns);

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

    auto getYson = [&] (TUnversionedValue value) {
        return value.Type == EValueType::Null
            ? TYsonString()
            : TYsonString(value.Data.String, value.Length);
    };
    auto getString = [&] (TUnversionedValue value, TStringBuf name) {
        if (value.Type == EValueType::Null) {
            THROW_ERROR_EXCEPTION("Unexpected null value in column %Qv in job archive", name);
        }
        return TStringBuf(value.Data.String, value.Length);
    };

    THashMap<NScheduler::TOperationId, TOperation> idToOperation;

    const auto& tableIndex = tableDescriptor.Index;
    for (auto row : rowset->GetRows()) {
        if (!row) {
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

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.BriefProgress)) {
            operation.BriefProgress = getYson(row[*indexOrNull]);
        }
        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Progress)) {
            operation.Progress = getYson(row[*indexOrNull]);
        }

        if (auto indexOrNull = columnFilter.FindPosition(tableIndex.RuntimeParameters)) {
            operation.RuntimeParameters = getYson(row[*indexOrNull]);
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
TListOperationsResult TClient::DoListOperations(
    const TListOperationsOptions& oldOptions)
{
    auto options = oldOptions;

    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultListOperationsTimeout);
    auto deadline = timeout.ToDeadLine();

    if (options.CursorTime && (
        (options.ToTime && *options.CursorTime > *options.ToTime) ||
        (options.FromTime && *options.CursorTime < *options.FromTime)))
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

    if (options.SubstrFilter) {
        options.SubstrFilter = to_lower(*options.SubstrFilter);
    }

    if (options.AccessFilter) {
        TObjectServiceProxy proxy(GetOperationArchiveChannel(options.ReadFrom));
        options.AccessFilter->SubjectTransitiveClosure = GetSubjectClosure(
            options.AccessFilter->Subject,
            proxy,
            options);
        if (options.AccessFilter->Subject == RootUserName ||
            options.AccessFilter->SubjectTransitiveClosure.contains(SuperusersGroupName))
        {
            options.AccessFilter.Reset();
        }
    }

    TListOperationsCountingFilter countingFilter(options);

    THashMap<NScheduler::TOperationId, TOperation> idToOperation;
    if (options.IncludeArchive && DoesOperationsArchiveExist()) {
        idToOperation = DoListOperationsFromArchive(
            deadline,
            countingFilter,
            options);
    }

    DoListOperationsFromCypress(
        deadline,
        countingFilter,
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
}

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
    auto jobCompetitionIdIndex = builder.AddSelectExpression("job_competition_id");
    auto hasCompetitorsIndex = builder.AddSelectExpression("has_competitors");
    auto execAttributesIndex = builder.AddSelectExpression("exec_attributes");

    int coreInfosIndex = -1;
    {
        constexpr int requiredVersion = 31;
        if (DoGetOperationsArchiveVersion() >= requiredVersion) {
            coreInfosIndex = builder.AddSelectExpression("core_infos");
        }
    }

    if (options.WithStderr) {
        if (*options.WithStderr) {
            builder.AddWhereConjunct("stderr_size != 0 AND NOT is_null(stderr_size)");
        } else {
            builder.AddWhereConjunct("stderr_size = 0 OR is_null(stderr_size)");
        }
    }

    if (options.WithSpec) {
        if (*options.WithSpec) {
            builder.AddWhereConjunct("has_spec");
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

    if (options.JobCompetitionId) {
        builder.AddWhereConjunct(Format("job_competition_id = %Qv", options.JobCompetitionId));
    }

    if (options.WithCompetitors) {
        if (*options.WithCompetitors) {
            builder.AddWhereConjunct("has_competitors");
        } else {
            builder.AddWhereConjunct("is_null(has_competitors) OR NOT has_competitors");
        }
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
            job.ArchiveState = job.State;

            if (row[startTimeIndex].Type != EValueType::Null) {
                job.StartTime = TInstant::MicroSeconds(row[startTimeIndex].Data.Int64);
            } else {
                // This field previously was non-optional.
                job.StartTime.emplace();
            }

            if (row[finishTimeIndex].Type != EValueType::Null) {
                job.FinishTime = TInstant::MicroSeconds(row[finishTimeIndex].Data.Int64);
            }

            if (row[addressIndex].Type != EValueType::Null) {
                job.Address = TString(row[addressIndex].Data.String, row[addressIndex].Length);
            } else {
                // This field previously was non-optional.
                job.Address.emplace();
            }

            if (row[stderrSizeIndex].Type != EValueType::Null) {
                job.StderrSize = row[stderrSizeIndex].Data.Uint64;
            }

            if (row[failContextSizeIndex].Type != EValueType::Null) {
                job.FailContextSize = row[failContextSizeIndex].Data.Uint64;
            }

            if (row[jobCompetitionIdIndex].Type != EValueType::Null) {
                job.JobCompetitionId = TJobId::FromString(FromUnversionedValue<TStringBuf>(row[jobCompetitionIdIndex]));
            }

            if (row[hasCompetitorsIndex].Type != EValueType::Null) {
                job.HasCompetitors = row[hasCompetitorsIndex].Data.Boolean;
            } else {
                job.HasCompetitors = false;
            }

            if (row[hasSpecIndex].Type != EValueType::Null) {
                job.HasSpec = row[hasSpecIndex].Data.Boolean;
            } else {
                // This field previously was non-optional.
                job.HasSpec = false;
            }

            if (row[errorIndex].Type != EValueType::Null) {
                job.Error = TYsonString(TString(row[errorIndex].Data.String, row[errorIndex].Length));
            }

            if (coreInfosIndex != -1 && row[coreInfosIndex].Type != EValueType::Null) {
                job.CoreInfos = TYsonString(TString(row[coreInfosIndex].Data.String, row[coreInfosIndex].Length));
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

            if (row[execAttributesIndex].Type != EValueType::Null) {
                job.ExecAttributes = TYsonString(TString(row[execAttributesIndex].Data.String, row[execAttributesIndex].Length));
            }

            // We intentionally mark stderr as missing if job has no spec since
            // it is impossible to check permissions without spec.
            if (job.State && NJobTrackerClient::IsJobFinished(*job.State) && !job.HasSpec) {
                job.StderrSize = std::nullopt;
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
        .Apply(BIND([jobsInProgressFuture, finishedJobsFuture, finishedJobsStatisticsFuture, this, this_=MakeStrong(this)] (const std::vector<TError>&) {
            const auto& jobsInProgressOrError = jobsInProgressFuture.Get();
            const auto& finishedJobsOrError = finishedJobsFuture.Get();
            const auto& statisticsOrError = finishedJobsStatisticsFuture.Get();

            if (!jobsInProgressOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get jobs in progress from the operation archive")
                    << jobsInProgressOrError;
            }
            if (!finishedJobsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get finished jobs from the operation archive")
                    << finishedJobsOrError;
            }
            if (!statisticsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get finished job statistics from the operation archive")
                    << statisticsOrError;
            }

            int filteredOutAsFinished = 0;
            auto difference = [&filteredOutAsFinished] (std::vector<TJob> origin, const std::vector<TJob>& blacklist) {
                THashSet<TJobId> idBlacklist;
                for (const auto& job : blacklist) {
                    YT_VERIFY(job.Id);
                    idBlacklist.emplace(job.Id);
                }
                origin.erase(
                    std::remove_if(
                        origin.begin(),
                        origin.end(),
                        [&idBlacklist, &filteredOutAsFinished] (const TJob& job) {
                            YT_VERIFY(job.Id);

                            if (idBlacklist.contains(job.Id)) {
                                ++filteredOutAsFinished;
                                return true;
                            }
                            return false;
                        }),
                    origin.end());
                return origin;
            };

            TListJobsFromArchiveResult result;
            result.FinishedJobs = finishedJobsOrError.Value();
            // If a job is present in both lists, we give priority
            // to |FinishedJobs| and remove it from |InProgressJobs|.
            result.InProgressJobs = difference(jobsInProgressOrError.Value(), result.FinishedJobs);
            result.FinishedJobsStatistics = statisticsOrError.Value();

            YT_LOG_DEBUG("Received finished jobs from archive (Count: %v)", result.FinishedJobs.size());
            YT_LOG_DEBUG("Received in-progress jobs from archive (Count: %v, FilteredOutAsFinished: %v)", result.InProgressJobs.size(), filteredOutAsFinished);

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
        "uncompressed_data_size",
        "job_competition_id",
        "has_competitors",
    };

    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetTimeout(deadline - Now());

    {
        auto getReq = TYPathProxy::Get(GetJobsPath(operationId));
        ToProto(getReq->mutable_attributes()->mutable_keys(), attributeFilter);
        batchReq->AddRequest(getReq, "get_jobs");
    }

    return batchReq->Invoke().Apply(BIND([&options, this, this_=MakeStrong(this)] (
        const TErrorOr<TObjectServiceProxy::TRspExecuteBatchPtr>& batchRspOrError)
    {
        const auto& batchRsp = batchRspOrError.ValueOrThrow();
        auto getReqRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");

        const auto& rsp = getReqRsp.ValueOrThrow();

        std::pair<std::vector<TJob>, int> result;
        auto& jobs = result.first;

        auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
        result.second = items->GetChildren().size();

        YT_LOG_DEBUG("Received jobs from cypress (Count: %v)", items->GetChildren().size());

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

            if (options.JobCompetitionId) {
                if (options.JobCompetitionId != attributes.Find<TJobId>("job_competition_id")) {
                    continue;
                }
            }

            if (options.WithCompetitors) {
                if (options.WithCompetitors != attributes.Find<bool>("has_competitors")) {
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
            job.JobCompetitionId = attributes.Find<TJobId>("job_competition_id").value_or(TJobId());
            job.HasCompetitors = attributes.Find<bool>("has_competitors").value_or(false);
        }

        return result;
    }));
}

static void ParseJobsFromControllerAgentResponse(
    TOperationId operationId,
    const std::vector<std::pair<TString, INodePtr>>& jobNodes,
    const std::function<bool(const INodePtr&)>& filter,
    const THashSet<TString>& attributes,
    std::vector<TJob>* jobs)
{
    auto needJobId = attributes.contains("job_id");
    auto needOperationId = attributes.contains("operation_id");
    auto needType = attributes.contains("type");
    auto needState = attributes.contains("state");
    auto needStartTime = attributes.contains("start_time");
    auto needFinishTime = attributes.contains("finish_time");
    auto needAddress = attributes.contains("address");
    auto needHasSpec = attributes.contains("has_spec");
    auto needProgress = attributes.contains("progress");
    auto needStderrSize = attributes.contains("stderr_size");
    auto needBriefStatistics = attributes.contains("brief_statistics");
    auto needJobCompetitionId = attributes.contains("job_competition_id");
    auto needHasCompetitors = attributes.contains("has_competitors");
    auto needError = attributes.contains("error");

    for (const auto& [jobIdString, jobNode] : jobNodes) {
        if (!filter(jobNode)) {
            continue;
        }

        const auto& jobMapNode = jobNode->AsMap();
        auto& job = jobs->emplace_back();
        if (needJobId) {
            job.Id = TJobId::FromString(jobIdString);
        }
        if (needOperationId) {
            job.OperationId = operationId;
        }
        if (needType) {
            job.Type = ParseEnum<EJobType>(jobMapNode->GetChild("job_type")->GetValue<TString>());
        }
        if (needState) {
            job.State = ParseEnum<EJobState>(jobMapNode->GetChild("state")->GetValue<TString>());
            job.ControllerAgentState = job.State;
        }
        if (needStartTime) {
            job.StartTime = ConvertTo<TInstant>(jobMapNode->GetChild("start_time")->GetValue<TString>());
        }
        if (needFinishTime) {
            if (auto child = jobMapNode->FindChild("finish_time")) {
                job.FinishTime = ConvertTo<TInstant>(child->GetValue<TString>());
            }
        }
        if (needAddress) {
            job.Address = jobMapNode->GetChild("address")->GetValue<TString>();
        }
        if (needHasSpec) {
            job.HasSpec = true;
        }
        if (needProgress) {
            job.Progress = jobMapNode->GetChild("progress")->GetValue<double>();
        }

        auto stderrSize = jobMapNode->GetChild("stderr_size")->GetValue<i64>();
        if (stderrSize > 0 && needStderrSize) {
            job.StderrSize = stderrSize;
        }

        if (needBriefStatistics) {
            job.BriefStatistics = ConvertToYsonString(jobMapNode->GetChild("brief_statistics"));
        }
        if (needJobCompetitionId) {
            //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            if (auto child = jobMapNode->FindChild("job_competition_id")) {
                job.JobCompetitionId = ConvertTo<TJobId>(child);
            }
        }
        if (needHasCompetitors) {
            //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            if (auto child = jobMapNode->FindChild("has_competitors")) {
                job.HasCompetitors = ConvertTo<bool>(child);
            }
        }
        if (needError) {
            if (auto child = jobMapNode->FindChild("error")) {
                job.Error = ConvertToYsonString(ConvertTo<TError>(child));
            }
        }
    }
}

static void ParseJobsFromControllerAgentResponse(
    TOperationId operationId,
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp,
    const TString& key,
    const THashSet<TString>& attributes,
    const TListJobsOptions& options,
    std::vector<TJob>* jobs,
    int* totalCount,
    NLogging::TLogger Logger)
{
    auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(key);
    if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return;
    }
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Cannot get %Qv from controller agent", key)
            << rspOrError;
    }

    auto rsp = rspOrError.Value();
    auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
    *totalCount += items->GetChildren().size();

    YT_LOG_DEBUG("Received %v jobs from controller agent (Count: %v)", key, items->GetChildren().size());

    auto filter = [&] (const INodePtr& jobNode) -> bool {
        auto stderrSize = jobNode->AsMap()->GetChild("stderr_size")->GetValue<i64>();
        auto failContextSizeNode = jobNode->AsMap()->FindChild("fail_context_size");
        auto failContextSize = failContextSizeNode
            ? failContextSizeNode->GetValue<i64>()
            : 0;
        auto jobCompetitionIdNode = jobNode->AsMap()->FindChild("job_competition_id");
        auto jobCompetitionId = jobCompetitionIdNode  //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            ? ConvertTo<TJobId>(jobCompetitionIdNode)
            : TJobId();
        auto hasCompetitorsNode = jobNode->AsMap()->FindChild("has_competitors");
        auto hasCompetitors = hasCompetitorsNode  //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            ? ConvertTo<bool>(hasCompetitorsNode)
            : false;
        return
            (!options.WithStderr || *options.WithStderr == (stderrSize > 0)) &&
            (!options.WithFailContext || *options.WithFailContext == (failContextSize > 0)) &&
            (!options.JobCompetitionId || options.JobCompetitionId == jobCompetitionId) &&
            (!options.WithCompetitors || options.WithCompetitors == hasCompetitors);
    };

    ParseJobsFromControllerAgentResponse(
        operationId,
        items->GetChildren(),
        filter,
        attributes,
        jobs);
}

TFuture<TClient::TListJobsFromControllerAgentResult> TClient::DoListJobsFromControllerAgentAsync(
    TOperationId operationId,
    const std::optional<TString>& controllerAgentAddress,
    TInstant deadline,
    const TListJobsOptions& options)
{
    if (!controllerAgentAddress) {
        return MakeFuture(TListJobsFromControllerAgentResult{});
    }

    // TODO(levysotskiy): extract this list to some common place.
    static const THashSet<TString> DefaultAttributes = {
        "job_id",
        "type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "has_spec",
        "progress",
        "stderr_size",
        "error",
        "brief_statistics",
        "job_competition_id",
        "has_competitors",
    };

    TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    proxy.SetDefaultTimeout(deadline - Now());
    auto batchReq = proxy.ExecuteBatch();

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidRunningJobsPath(*controllerAgentAddress, operationId)),
        "running_jobs");

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidRetainedFinishedJobsPath(*controllerAgentAddress, operationId)),
        "retained_finished_jobs");

    return batchReq->Invoke().Apply(BIND([operationId, options, this, this_=MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
        TListJobsFromControllerAgentResult result;
        ParseJobsFromControllerAgentResponse(
            operationId,
            batchRsp,
            "running_jobs",
            DefaultAttributes,
            options,
            &result.InProgressJobs,
            &result.TotalInProgressJobCount,
            Logger);
        ParseJobsFromControllerAgentResponse(
            operationId,
            batchRsp,
            "retained_finished_jobs",
            DefaultAttributes,
            options,
            &result.FinishedJobs,
            &result.TotalFinishedJobCount,
            Logger);
        return result;
    }));
}

static std::function<bool(const TJob&, const TJob&)> GetJobsComparator(
    EJobSortField sortField,
    EJobSortDirection sortOrder)
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
        }
        YT_ABORT();
    };

    auto makeLessByField = [&] (auto TJob::* field) {
        return makeLessBy([field] (const TJob& job) {
            return job.*field;
        });
    };

    auto makeLessByFormattedEnumField = [&] (auto TJob::* field) {
        return makeLessBy([field] (const TJob& job) -> std::optional<TString> {
            if (auto value = job.*field) {
                return FormatEnum(*value);
            } else {
                return std::nullopt;
            }
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
            return makeLessBy([now = TInstant::Now()] (const TJob& job) -> std::optional<TDuration> {
                if (job.StartTime) {
                    return (job.FinishTime ? *job.FinishTime : now) - *job.StartTime;
                } else {
                    return std::nullopt;
                }
            });
        default:
            YT_ABORT();
    }
}

template <typename TSourceJob>
static void MergeJob(TSourceJob&& source, TJob* target)
{
#define MERGE_NULLABLE_FIELD(name) \
    if (source.name) { \
        target->name = std::forward<decltype(std::forward<TSourceJob>(source).name)>(source.name); \
    }
    MERGE_NULLABLE_FIELD(Type);
    MERGE_NULLABLE_FIELD(State);
    MERGE_NULLABLE_FIELD(ControllerAgentState);
    MERGE_NULLABLE_FIELD(ArchiveState);
    MERGE_NULLABLE_FIELD(StartTime);
    MERGE_NULLABLE_FIELD(FinishTime);
    MERGE_NULLABLE_FIELD(Address);
    MERGE_NULLABLE_FIELD(Progress);
    MERGE_NULLABLE_FIELD(Error);
    MERGE_NULLABLE_FIELD(BriefStatistics);
    MERGE_NULLABLE_FIELD(InputPaths);
    MERGE_NULLABLE_FIELD(CoreInfos);
    MERGE_NULLABLE_FIELD(JobCompetitionId);
    MERGE_NULLABLE_FIELD(HasCompetitors);
    MERGE_NULLABLE_FIELD(ExecAttributes);
#undef MERGE_NULLABLE_FIELD
    if (source.StderrSize && target->StderrSize.value_or(0) < source.StderrSize) {
        target->StderrSize = source.StderrSize;
    }
}

static THashMap<TJobId, TJob*> CreateJobIdToJobMap(std::vector<TJob>& jobs)
{
    THashMap<TJobId, TJob*> result;
    for (auto& job : jobs) {
        YT_VERIFY(job.Id);
        result.emplace(job.Id, &job);
    }
    return result;
}

template <typename TJobs>
static void UpdateJobs(TJobs&& patch, std::vector<TJob>* origin)
{
    auto originMap = CreateJobIdToJobMap(*origin);
    for (auto& job : patch) {
        YT_VERIFY(job.Id);
        if (auto originMapIt = originMap.find(job.Id); originMapIt != originMap.end()) {
            if constexpr (std::is_rvalue_reference_v<TJobs>) {
                MergeJob(std::move(job), originMapIt->second);
            } else {
                MergeJob(job, originMapIt->second);
            }
        }
    }
}

static void UpdateJobsAndAddMissing(std::vector<TJob>&& delta, std::vector<TJob>* origin)
{
    auto originMap = CreateJobIdToJobMap(*origin);
    std::vector<TJob> newJobs;
    for (auto& job : delta) {
        YT_VERIFY(job.Id);
        if (auto originMapIt = originMap.find(job.Id); originMapIt != originMap.end()) {
            MergeJob(std::move(job), originMapIt->second);
        } else {
            newJobs.push_back(std::move(job));
        }
    }
    origin->insert(
        origin->end(),
        std::make_move_iterator(newJobs.begin()),
        std::make_move_iterator(newJobs.end()));
}

template <typename T, typename TComparator>
static void MergeThreeVectors(
    std::vector<T>&& first,
    std::vector<T>&& second,
    std::vector<T>&& third,
    std::vector<T>* result,
    TComparator comparator)
{
    YT_VERIFY(result);
    std::vector<T> firstAndSecond;
    firstAndSecond.reserve(first.size() + second.size());
    std::merge(
        std::make_move_iterator(first.begin()),
        std::make_move_iterator(first.end()),
        std::make_move_iterator(second.begin()),
        std::make_move_iterator(second.end()),
        std::back_inserter(firstAndSecond),
        comparator);
    result->reserve(firstAndSecond.size() + third.size());
    std::merge(
        std::make_move_iterator(firstAndSecond.begin()),
        std::make_move_iterator(firstAndSecond.end()),
        std::make_move_iterator(third.begin()),
        std::make_move_iterator(third.end()),
        std::back_inserter(*result),
        comparator);
}

void FillIsJobStale(TJob* job)
{
    job->IsStale = (!job->ControllerAgentState && job->ArchiveState == EJobState::Running);
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

    TFuture<std::pair<std::vector<TJob>, int>> cypressResultFuture;
    TFuture<TListJobsFromControllerAgentResult> controllerAgentResultFuture;
    TFuture<TListJobsFromArchiveResult> archiveResultFuture;

    // Issue the requests in parallel.

    auto tryListJobsFromArchiveAsync = [&] {
        if (DoesOperationsArchiveExist()) {
            return DoListJobsFromArchiveAsync(
                operationId,
                deadline,
                /* includeInProgressJobs */ controllerAgentAddress.has_value(),
                options);
        }
        return MakeFuture(TListJobsFromArchiveResult{});
    };

    if (includeArchive) {
        archiveResultFuture = tryListJobsFromArchiveAsync();
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

    TListJobsFromControllerAgentResult controllerAgentResult;
    if (includeControllerAgent) {
        auto controllerAgentResultOrError = WaitFor(controllerAgentResultFuture);
        if (controllerAgentResultOrError.IsOK()) {
            controllerAgentResult = std::move(controllerAgentResultOrError.Value());
            result.ControllerAgentJobCount =
                controllerAgentResult.TotalFinishedJobCount + controllerAgentResult.TotalInProgressJobCount;
        } else if (controllerAgentResultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No such operation in the controller agent.
            result.ControllerAgentJobCount = 0;
        } else {
            result.Errors.push_back(std::move(controllerAgentResultOrError));
        }
    }

    auto countAndFilterJobs = [&options] (std::vector<TJob>&& jobs, TListJobsStatistics* statistics) {
        std::vector<TJob> filteredJobs;
        for (auto& job : jobs) {
            if (options.Address && job.Address != *options.Address) {
                continue;
            }

            if (job.Type) {
                statistics->TypeCounts[*job.Type] += 1;
            }
            if (options.Type && job.Type != *options.Type) {
                continue;
            }

            if (job.State) {
                statistics->StateCounts[*job.State] += 1;
            }
            if (options.State && job.State != *options.State) {
                continue;
            }

            filteredJobs.push_back(std::move(job));
        }
        return filteredJobs;
    };

    auto mergeWithArchiveJobs = [&] (
        TListJobsResult& result,
        TListJobsFromControllerAgentResult&& controllerAgentResult,
        const TFuture<TListJobsFromArchiveResult>& archiveResultFuture)
    {
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

        if (!controllerAgentAddress && archiveResult.InProgressJobs.empty()) {
            result.Jobs = std::move(archiveResult.FinishedJobs);
            return;
        }

        UpdateJobs(controllerAgentResult.InProgressJobs, &archiveResult.InProgressJobs);
        UpdateJobs(controllerAgentResult.FinishedJobs, &archiveResult.InProgressJobs);
        UpdateJobs(controllerAgentResult.InProgressJobs, &archiveResult.FinishedJobs);
        UpdateJobs(controllerAgentResult.FinishedJobs, &archiveResult.FinishedJobs);

        THashSet<TJobId> archiveJobIds;
        for (const auto& job : archiveResult.InProgressJobs) {
            YT_VERIFY(job.Id);
            archiveJobIds.insert(job.Id);
        }
        THashSet<TJobId> archiveFinishedJobIds;
        for (const auto& job : archiveResult.FinishedJobs) {
            YT_VERIFY(job.Id);
            archiveJobIds.insert(job.Id);
        }

        int filteredOutAsReceivedFromArchive = 0;
        controllerAgentResult.FinishedJobs.erase(
            std::remove_if(
                controllerAgentResult.FinishedJobs.begin(),
                controllerAgentResult.FinishedJobs.end(),
                [&] (const TJob& job) {
                    if (archiveJobIds.contains(job.Id)) {
                        ++filteredOutAsReceivedFromArchive;
                        return true;
                    }
                    return false;
                }),
            controllerAgentResult.FinishedJobs.end());

        YT_LOG_DEBUG("Finished jobs from controller agent filtered (FilteredOutAsReceivedFromArchive: %v)",
            filteredOutAsReceivedFromArchive);

        auto jobComparator = GetJobsComparator(options.SortField, options.SortOrder);
        auto countFilterSort = [&] (std::vector<TJob>& jobs) {
            jobs = countAndFilterJobs(std::move(jobs), &result.Statistics);
            std::sort(jobs.begin(), jobs.end(), jobComparator);
        };

        countFilterSort(controllerAgentResult.FinishedJobs);
        countFilterSort(archiveResult.InProgressJobs);
        std::sort(archiveResult.FinishedJobs.begin(), archiveResult.FinishedJobs.end(), jobComparator);

        MergeThreeVectors(
            std::move(controllerAgentResult.FinishedJobs),
            std::move(archiveResult.InProgressJobs),
            std::move(archiveResult.FinishedJobs),
            &result.Jobs,
            jobComparator);
    };

    switch (dataSource) {
        case EDataSource::Archive: {
            mergeWithArchiveJobs(result, std::move(controllerAgentResult), archiveResultFuture);
            break;
        }
        case EDataSource::Runtime: {
            auto cypressResultOrError = WaitFor(cypressResultFuture);
            std::vector<TJob> jobs;
            if (cypressResultOrError.IsOK()) {
                auto& [cypressJobs, cypressJobCount] = cypressResultOrError.Value();
                result.CypressJobCount = cypressJobCount;
                jobs = std::move(cypressJobs);
            } else if (cypressResultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in Cypress.
                result.CypressJobCount = 0;
            } else {
                result.Errors.push_back(TError("Failed to get jobs from Cypress") << cypressResultOrError);
            }

            // No jobs fetched from Cypress, try to get them from archive.
            // (There might be no jobs in Cypress because we don't store them there in recent versions).
            if (result.CypressJobCount == 0 && options.DataSource == EDataSource::Auto) {
                mergeWithArchiveJobs(result, std::move(controllerAgentResult), tryListJobsFromArchiveAsync());
                break;
            }

            UpdateJobsAndAddMissing(std::move(controllerAgentResult.InProgressJobs), &jobs);
            UpdateJobsAndAddMissing(std::move(controllerAgentResult.FinishedJobs), &jobs);

            jobs = countAndFilterJobs(std::move(jobs), &result.Statistics);
            std::sort(jobs.begin(), jobs.end(), GetJobsComparator(options.SortField, options.SortOrder));
            result.Jobs = std::move(jobs);

            break;
        }
        default:
            YT_ABORT();
    }

    auto beginIt = std::min(result.Jobs.end(), result.Jobs.begin() + options.Offset);
    auto endIt = std::min(result.Jobs.end(), beginIt + options.Limit);
    result.Jobs = std::vector<TJob>(std::make_move_iterator(beginIt), std::make_move_iterator(endIt));

    // If controller agent was queried, we can fill |IsStale| field correctly.
    if (includeControllerAgent) {
        for (auto& job : result.Jobs) {
            FillIsJobStale(&job);
        }
    }

    return result;
}

template <typename T>
static std::optional<T> FindValue(
    TUnversionedRow row,
    const NTableClient::TColumnFilter& columnFilter,
    int columnIndex)
{
    auto maybeIndex = columnFilter.FindPosition(columnIndex);
    if (maybeIndex && row[*maybeIndex].Type != EValueType::Null) {
        return FromUnversionedValue<T>(row[*maybeIndex]);
    }
    return {};
}

template <typename TValue>
static void TryAddFluentItem(
    TFluentMap fluent,
    TStringBuf key,
    TUnversionedRow row,
    const NTableClient::TColumnFilter& columnFilter,
    int columnIndex)
{
    if (auto value = FindValue<TValue>(row, columnFilter, columnIndex)) {
        fluent.Item(key).Value(*value);
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
        if (attribute == "operation_id" || attribute == "job_id") {
            result.push_back(attribute + "_hi");
            result.push_back(attribute + "_lo");
        } else if (attribute == "state") {
            result.emplace_back("state");
            result.emplace_back("transient_state");
        } else {
            result.push_back(attribute);
        }
    }
    return result;
}

std::optional<TJob> TClient::DoGetJobFromArchive(
    TOperationId operationId,
    TJobId jobId,
    TInstant deadline,
    const THashSet<TString>& attributes,
    const TGetJobOptions& options)
{
    TJobTableDescriptor table;
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TUnversionedRow> keys;
    auto key = rowBuffer->AllocateUnversioned(4);
    key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], table.Index.OperationIdHi);
    key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], table.Index.OperationIdLo);
    key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], table.Index.JobIdHi);
    key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], table.Index.JobIdLo);
    keys.push_back(key);


    std::vector<int> columnIndexes;
    auto fields = MakeJobArchiveAttributes(attributes);
    for (const auto& field : fields) {
        columnIndexes.push_back(table.NameTable->GetIdOrThrow(field));
    }

    TLookupRowsOptions lookupOptions;
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
        return {};
    }

    const auto& columnFilter = lookupOptions.ColumnFilter;

    TJob job;

    if (columnFilter.ContainsIndex(table.Index.JobIdHi)) {
        job.Id = jobId;
    }
    if (columnFilter.ContainsIndex(table.Index.OperationIdHi)) {
        job.OperationId = operationId;
    }

    auto state = FindValue<TStringBuf>(row, columnFilter, table.Index.State);
    if (!state) {
        state = FindValue<TStringBuf>(row, columnFilter, table.Index.TransientState);
    }
    if (state) {
        job.State = ParseEnum<EJobState>(*state);
        job.ArchiveState = job.State;
    }

    // NB: We need a separate function for |TInstant| because it has type "int64" in table
    // but |FromUnversionedValue<TInstant>| expects it to be "uint64".
    auto findInstant = [&] (int columnIndex) -> std::optional<TInstant> {
        if (auto microseconds = FindValue<i64>(row, columnFilter, columnIndex)) {
            return TInstant::MicroSeconds(*microseconds);
        }
        return {};
    };

    if (auto startTime = findInstant(table.Index.StartTime)) {
        job.StartTime = *startTime;
    }
    if (auto finishTime = findInstant(table.Index.FinishTime)) {
        job.FinishTime = *finishTime;
    }
    if (auto hasSpec = FindValue<bool>(row, columnFilter, table.Index.HasSpec)) {
        job.HasSpec = *hasSpec;
    }
    if (auto address = FindValue<TStringBuf>(row, columnFilter, table.Index.Address)) {
        job.Address = *address;
    }
    if (auto type = FindValue<TStringBuf>(row, columnFilter, table.Index.Type)) {
        job.Type = ParseEnum<EJobType>(*type);
    }
    if (auto error = FindValue<TYsonString>(row, columnFilter, table.Index.Error)) {
        job.Error = std::move(*error);
    }
    if (auto statistics = FindValue<TYsonString>(row, columnFilter, table.Index.Statistics)) {
        job.Statistics = std::move(*statistics);
    }
    if (auto events = FindValue<TYsonString>(row, columnFilter, table.Index.Events)) {
        job.Events = std::move(*events);
    }
    if (auto jobCompetitionId = FindValue<TJobId>(row, columnFilter, table.Index.JobCompetitionId)) {
        job.JobCompetitionId = *jobCompetitionId;
    }
    if (columnFilter.FindPosition(table.Index.HasCompetitors)) {
        if (auto hasCompetitors = FindValue<bool>(row, columnFilter, table.Index.HasCompetitors)) {
            job.HasCompetitors = *hasCompetitors;
        } else {
            job.HasCompetitors = false;
        }
    }

    return job;
}

std::optional<TJob> TClient::DoGetJobFromControllerAgent(
    TOperationId operationId,
    TJobId jobId,
    TInstant deadline,
    const THashSet<TString>& attributes,
    const TGetJobOptions& options)
{
    auto controllerAgentAddress = GetControllerAgentAddressFromCypress(
        operationId,
        GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    if (!controllerAgentAddress) {
        return {};
    }

    TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    proxy.SetDefaultTimeout(deadline - Now());
    auto batchReq = proxy.ExecuteBatch();

    auto runningJobPath =
        GetControllerAgentOrchidRunningJobsPath(*controllerAgentAddress, operationId) + "/" + ToString(jobId);
    batchReq->AddRequest(TYPathProxy::Get(runningJobPath));

    auto finishedJobPath =
        GetControllerAgentOrchidRetainedFinishedJobsPath(*controllerAgentAddress, operationId) + "/" + ToString(jobId);
    batchReq->AddRequest(TYPathProxy::Get(finishedJobPath));

    auto batchRspOrError = WaitFor(batchReq->Invoke());

    if (!batchRspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Cannot get jobs from controller agent")
            << batchRspOrError;
    }

    for (const auto& rspOrError : batchRspOrError.Value()->GetResponses<TYPathProxy::TRspGet>()) {
        if (rspOrError.IsOK()) {
            std::vector<TJob> jobs;
            ParseJobsFromControllerAgentResponse(
                operationId,
                {{ToString(jobId), ConvertToNode(TYsonString(rspOrError.Value()->value()))}},
                [] (const INodePtr&) {
                    return true;
                },
                attributes,
                &jobs);
            YT_VERIFY(jobs.size() == 1);
            return jobs[0];
        } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION("Cannot get jobs from controller agent")
                << rspOrError;
        }
    }

    return {};
}

TYsonString TClient::DoGetJob(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetJobTimeout);
    auto deadline = timeout.ToDeadLine();

    static const THashSet<TString> DefaultAttributes = {
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
        "job_competition_id",
        "has_competitors",
    };

    const auto& attributes = options.Attributes.value_or(DefaultAttributes);

    auto job = DoGetJobFromArchive(operationId, jobId, deadline, attributes, options);
    if (!job) {
        job = DoGetJobFromControllerAgent(operationId, jobId, deadline, attributes, options);
    }

    if (!job) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchJob,
            "Job %v or operation %v not found neither in archive nor in controller agent",
            jobId,
            operationId);
    }

    return BuildYsonStringFluently()
        .Do([&] (TFluentAny fluent) {
            Serialize(*job, fluent.GetConsumer(), "job_id");
        });
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
    req->set_populate_master_cache_node_addresses(options.PopulateMasterCacheNodeAddresses);
    req->set_populate_timestamp_provider_node_addresses(options.PopulateTimestampProviderAddresses);
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
    if (options.PopulateMasterCacheNodeAddresses) {
        meta.MasterCacheNodeAddresses = FromProto<std::vector<TString>>(rsp->master_cache_node_addresses());
    }
    if (options.PopulateTimestampProviderAddresses) {
        meta.TimestampProviderAddresses = FromProto<std::vector<TString>>(rsp->timestamp_provider_node_addresses());
    }
    return meta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
