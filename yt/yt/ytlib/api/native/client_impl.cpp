#include "client_impl.h"

#include "box.h"
#include "config.h"
#include "connection.h"
#include "private.h"
#include "rpc_helpers.h"
#include "default_type_handler.h"
#include "pipeline_type_handler.h"
#include "replicated_table_replica_type_handler.h"
#include "replication_card_type_handler.h"
#include "replication_card_collocation_type_handler.h"
#include "chaos_table_replica_type_handler.h"
#include "secondary_index_type_handler.h"
#include "table_collocation_type_handler.h"
#include "tablet_action_type_handler.h"
#include "chaos_replicated_table_type_handler.h"

#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/client/security_client/access_control.h>
#include <yt/yt/client/security_client/public.h>
#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NProfiling;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NHiveClient;
using namespace NScheduler;
using namespace NHydra;

using NNodeTrackerClient::CreateNodeChannelFactory;
using NNodeTrackerClient::INodeChannelFactoryPtr;

////////////////////////////////////////////////////////////////////////////////

TTransactionCounters::TTransactionCounters(const TProfiler& profiler)
{
    #define XX(name) \
        name ## Counter = profiler.Counter("/" + CamelCaseToUnderscoreCase(#name) + "s");

        XX(Transaction)
        XX(CommittedTransaction)
        XX(AbortedTransaction)
        XX(TabletSessionCommit)
        XX(SuccessfulTabletSessionCommit)
        XX(RetriedSuccessfulTabletSessionCommit)
        XX(FailedTabletSessionCommit)

    #undef XX
}

TOperationApiCounters::TOperationApiCounters(const TProfiler& profiler)
{
    #define XX(name) \
        name ## Counter = profiler.Counter("/" + CamelCaseToUnderscoreCase(#name) + "_count");

        XX(GetOperationFromArchiveTimeout)
        XX(GetOperationFromArchiveFailure)
        XX(GetOperationFromArchiveSuccess)

    #undef XX
}

TClientCounters::TClientCounters(const TProfiler& profiler)
    : TransactionCounters(profiler)
    , OperationApiCounters(profiler)
{ }

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options,
    INodeMemoryTrackerPtr memoryTracker)
{
    YT_VERIFY(connection);

    return New<TClient>(std::move(connection), options, std::move(memoryTracker));
}

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    IConnectionPtr connection,
    const TClientOptions& options,
    INodeMemoryTrackerPtr memoryTracker)
    : Connection_(std::move(connection))
    , Options_(options)
    , Logger(ApiLogger.WithTag("ClientId: %v, AuthenticatedUser: %v",
        TGuid::Create(),
        Options_.GetAuthenticatedUser()))
    , Profiler_(TProfiler("/native_client").WithTag("connection_name", Connection_->GetStaticConfig()->ConnectionName))
    , Counters_(Profiler_)
    , TypeHandlers_{
        CreatePipelineTypeHandler(this),
        CreateReplicatedTableReplicaTypeHandler(this),
        CreateSecondaryIndexTypeHandler(this),
        CreateReplicationCardTypeHandler(this),
        CreateReplicationCardCollocationTypeHandler(this),
        CreateChaosReplicatedTableTypeHandler(this),
        CreateChaosTableReplicaTypeHandler(this),
        CreateTableCollocationTypeHandler(this),
        CreateTabletActionTypeHandler(this),
        CreateDefaultTypeHandler(this)
    }
    , FunctionImplCache_(BIND(CreateFunctionImplCache,
        Connection_->GetConfig()->FunctionImplCache,
        MakeWeak(this)))
    , FunctionRegistry_ (BIND(CreateFunctionRegistryCache,
        Connection_->GetConfig()->FunctionRegistryCache,
        MakeWeak(this),
        Connection_->GetInvoker()))
    , LookupMemoryTracker_(WithCategory(memoryTracker, EMemoryCategory::Lookup))
    , QueryMemoryTracker_(WithCategory(memoryTracker, EMemoryCategory::Query))
{
    if (!Options_.User) {
        THROW_ERROR_EXCEPTION("Native connection requires non-null \"user\" parameter");
    }

    auto wrapChannel = [&] (IChannelPtr channel) {
        channel = CreateAuthenticatedChannel(std::move(channel), options.GetAuthenticationIdentity());
        return channel;
    };
    auto wrapChannelFactory = [&] (IChannelFactoryPtr factory) {
        factory = CreateAuthenticatedChannelFactory(std::move(factory), options.GetAuthenticationIdentity());
        return factory;
    };

    auto initMasterChannel = [&] (EMasterChannelKind kind, TCellTag cellTag) {
        MasterChannels_[kind][cellTag] = wrapChannel(Connection_->GetMasterChannelOrThrow(kind, cellTag));
    };
    auto initCypressChannel = [&] (EMasterChannelKind kind, TCellTag cellTag) {
        CypressChannels_[kind][cellTag] = wrapChannel(Connection_->GetCypressChannelOrThrow(kind, cellTag));
    };
    for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
        initMasterChannel(kind, Connection_->GetPrimaryMasterCellTag());
        initCypressChannel(kind, Connection_->GetPrimaryMasterCellTag());
        for (auto cellTag : Connection_->GetSecondaryMasterCellTags()) {
            initMasterChannel(kind, cellTag);
            initCypressChannel(kind, cellTag);
        }
    }

    SchedulerChannel_ = wrapChannel(Connection_->GetSchedulerChannel());

    ChannelFactory_ = CreateNodeChannelFactory(
        wrapChannelFactory(Connection_->GetChannelFactory()),
        Connection_->GetNetworks());

    SchedulerOperationProxy_ = std::make_unique<TOperationServiceProxy>(GetSchedulerChannel());
    BundleControllerProxy_ = std::make_unique<NBundleController::TBundleControllerServiceProxy>(Connection_->GetBundleControllerChannel());

    TransactionManager_ = New<TTransactionManager>(
        Connection_,
        Options_.GetAuthenticatedUser());
}

NApi::IConnectionPtr TClient::GetConnection()
{
    return Connection_;
}

const ITableMountCachePtr& TClient::GetTableMountCache()
{
    return Connection_->GetTableMountCache();
}

const NChaosClient::IReplicationCardCachePtr& TClient::GetReplicationCardCache()
{
    return Connection_->GetReplicationCardCache();
}

const ITimestampProviderPtr& TClient::GetTimestampProvider()
{
    return Connection_->GetTimestampProvider();
}

const IConnectionPtr& TClient::GetNativeConnection()
{
    return Connection_;
}

const TTransactionManagerPtr& TClient::GetTransactionManager()
{
    return TransactionManager_;
}

const TClientCounters& TClient::GetCounters() const
{
    return Counters_;
}

IFunctionRegistryPtr TClient::GetFunctionRegistry()
{
    return FunctionRegistry_.Get();
}

TFunctionImplCachePtr TClient::GetFunctionImplCache()
{
    return FunctionImplCache_.Get();
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
    auto it = channels.find(cellTag == PrimaryMasterCellTagSentinel ? Connection_->GetPrimaryMasterCellTag() : cellTag);
    if (it == channels.end()) {
        THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
            cellTag);
    }
    return it->second;
}

IChannelPtr TClient::GetCypressChannelOrThrow(
    EMasterChannelKind kind,
    TCellTag cellTag)
{
    const auto& channels = CypressChannels_[kind];
    auto it = channels.find(cellTag == PrimaryMasterCellTagSentinel ? Connection_->GetPrimaryMasterCellTag() : cellTag);
    if (it == channels.end()) {
        THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
            cellTag);
    }
    return it->second;
}

IChannelPtr TClient::GetCellChannelOrThrow(TCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    auto channel = cellDirectory->GetChannelByCellIdOrThrow(cellId);
    return CreateAuthenticatedChannel(std::move(channel), Options_.GetAuthenticationIdentity());
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
        for (const auto& [cellTag, channel] : MasterChannels_[kind]) {
            channel->Terminate(error);
        }
        for (const auto& [cellTag, channel] : CypressChannels_[kind]) {
            channel->Terminate(error);
        }
    }
    SchedulerChannel_->Terminate(error);
}

const IClientPtr& TClient::GetOperationsArchiveClient()
{
    {
        auto guard = Guard(OperationsArchiveClientLock_);
        if (OperationsArchiveClient_) {
            return OperationsArchiveClient_;
        }
    }

    auto options = TClientOptions::FromUser(NSecurityClient::OperationsClientUserName);
    auto client = Connection_->CreateNativeClient(options);

    {
        auto guard = Guard(OperationsArchiveClientLock_);
        if (!OperationsArchiveClient_) {
            OperationsArchiveClient_ = std::move(client);
        }
        return OperationsArchiveClient_;
    }
}

template <class T>
TFuture<T> TClient::Execute(
    TStringBuf commandName,
    const TTimeoutOptions& options,
    TCallback<T()> callback)
{
    auto promise = NewPromise<T>();
    BIND([
        commandName,
        promise,
        callback = std::move(callback),
        this,
        this_ = MakeWeak(this)
    ] () mutable {
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
    })
        .Via(Connection_->GetInvoker())
        .Run();

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
    if (commandOptions.SuppressExpirationTimeoutRenewal) {
        NCypressClient::SetSuppressExpirationTimeoutRenewal(request, true);
    }
}

void TClient::SetCachingHeader(
    const IClientRequestPtr& request,
    const TMasterReadOptions& options)
{
    NApi::NNative::SetCachingHeader(request, Connection_, options);
}

void TClient::SetBalancingHeader(
    const TObjectServiceProxy::TReqExecuteBatchPtr& request,
    const TMasterReadOptions& options)
{
    NApi::NNative::SetBalancingHeader(request, Connection_, options);
}

TObjectServiceProxy TClient::CreateObjectServiceReadProxy(
    const TMasterReadOptions& options,
    TCellTag cellTag)
{
    return NObjectClient::CreateObjectServiceReadProxy(
        MakeStrong(this),
        options.ReadFrom,
        cellTag,
        Connection_->GetStickyGroupSizeCache());
}

TObjectServiceProxy TClient::CreateObjectServiceWriteProxy(
    TCellTag cellTag)
{
    return NObjectClient::CreateObjectServiceWriteProxy(
        MakeStrong(this),
        cellTag);
}

template <class TProxy>
TProxy TClient::CreateWriteProxy(
    TCellTag cellTag)
{
    auto channel = GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    return TProxy(std::move(channel));
}

template TChunkServiceProxy TClient::CreateWriteProxy<TChunkServiceProxy>(TCellTag cellTag);

template TObjectServiceProxy TClient::CreateWriteProxy<TObjectServiceProxy>(TCellTag cellTag);

NRpc::IChannelPtr TClient::GetReadCellChannelOrThrow(const NHiveClient::TCellDescriptorPtr& cellDescriptor)
{
    const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(*cellDescriptor, NHydra::EPeerKind::Leader);
    return ChannelFactory_->CreateChannel(primaryPeerDescriptor.GetAddressOrThrow(Connection_->GetNetworks()));
}

IChannelPtr TClient::GetReadCellChannelOrThrow(TTabletCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    auto cellDescriptor = cellDirectory->GetDescriptorByCellIdOrThrow(cellId);
    return GetReadCellChannelOrThrow(cellDescriptor);
}

IChannelPtr TClient::GetHydraAdminChannelOrThrow(TCellId cellId)
{
    const auto& clockServerConfig = Connection_->GetStaticConfig()->ClockServers;
    const auto& cellDirectory = Connection_->GetCellDirectory();

    auto isClockServerCellId = [&] (TCellId cellId) {
        return clockServerConfig && clockServerConfig->CellId == cellId;
    };

    auto isHiveCellId = [&] (TCellId cellId) {
        return cellDirectory->IsCellRegistered(cellId);
    };

    auto maybeSyncCellDirectory = [&] {
        try {
            SyncCellsIfNeeded({cellId});
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NHydra::EErrorCode::ReadOnly)) {
                YT_LOG_WARNING(ex, "Skipping cell directory synchronization");
            } else {
                throw;
            }
        }
    };

    // Currently, clock servers are configured statically and thus don't require
    // cell directory synchronization. Let's get them out of the way first.
    if (isClockServerCellId(cellId)) {
        YT_VERIFY(clockServerConfig);
        if (!clockServerConfig->Addresses) {
            THROW_ERROR_EXCEPTION("Clock server addresses are empty");
        }
        auto channel = CreatePeerChannel(clockServerConfig, Connection_->GetChannelFactory(), EPeerKind::Leader);
        return CreateRetryingChannel(clockServerConfig, std::move(channel));
    }

    maybeSyncCellDirectory();

    if (isHiveCellId(cellId)) {
        auto channel = cellDirectory->GetChannelByCellIdOrThrow(cellId);
        return CreateRetryingChannel(Connection_->GetConfig()->HydraAdminChannel, std::move(channel));
    }

    THROW_ERROR_EXCEPTION("Unknown cell %v",
        cellId);
}

TCellDescriptorPtr TClient::GetCellDescriptorOrThrow(TCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    if (auto cellDescriptor = cellDirectory->FindDescriptorByCellId(cellId)) {
        return cellDescriptor;
    }

    WaitFor(Connection_->GetCellDirectorySynchronizer()->Sync())
        .ThrowOnError();

    return cellDirectory->GetDescriptorByCellIdOrThrow(cellId);
}

std::vector<TString> TClient::GetCellAddressesOrThrow(NObjectClient::TCellId cellId)
{
    const auto& cellDirectory = Connection_->GetCellDirectory();
    if (cellDirectory->IsCellRegistered(cellId)) {
        std::vector<TString> addresses;
        auto cellDescriptor = GetCellDescriptorOrThrow(cellId);
        for (const auto& peerDescriptor : cellDescriptor->Peers) {
            addresses.push_back(peerDescriptor.GetDefaultAddress());
        }
        return addresses;
    }

    auto config = Connection_->GetStaticConfig()->ClockServers;
    if (config && config->CellId == cellId) {
        if (!config->Addresses) {
            THROW_ERROR_EXCEPTION("Clock server addresses are empty");
        }
        return *config->Addresses;
    }

    THROW_ERROR_EXCEPTION("Unknown cell %v",
        cellId);
}

NApi::IClientPtr TClient::CreateRootClient()
{
    auto options = TClientOptions::FromAuthenticationIdentity(GetRootAuthenticationIdentity());
    return Connection_->CreateClient(options);
}

void TClient::ValidateSuperuserPermissions()
{
    if (Options_.User == NSecurityClient::RootUserName) {
        return;
    }

    auto pathToGroupYsonList = NSecurityClient::GetUserPath(*Options_.User) + "/@member_of_closure";

    TGetNodeOptions options;
    options.SuppressTransactionCoordinatorSync = true;
    options.SuppressUpstreamSync = true;
    auto groupYsonList = WaitFor(GetNode(pathToGroupYsonList, options))
        .ValueOrThrow();

    auto groups = ConvertTo<THashSet<TString>>(groupYsonList);
    YT_LOG_DEBUG("User group membership info received (Name: %v, Groups: %v)",
        Options_.User,
        groups);

    if (!groups.contains(NSecurityClient::SuperusersGroupName)) {
        THROW_ERROR_EXCEPTION("Superuser permissions required");
    }
}

void TClient::ValidatePermissionsWithAcn(
    NSecurityClient::EAccessControlObject accessControlObjectName,
    EPermission permission)
{
    TErrorOr<TCheckPermissionResponse> response;

    auto aco = GetAccessControlObjectDescriptor(accessControlObjectName);
    auto objectPath = aco.GetPrincipalPath();

    try {
        response = DoCheckPermission(
            Options_.GetAuthenticatedUser(),
            objectPath,
            permission,
            TCheckPermissionOptions());
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Check permission failed");
        response = TError(ex);
    }

    if (!(response.IsOK() && response.Value().Action == NSecurityClient::ESecurityAction::Allow)) {
        ValidateSuperuserPermissions();
        YT_LOG_WARNING("There is no access control object with the necessary permissions (Name: %v, Path: %v, Permission: %v)",
            Options_.User,
            objectPath,
            ToString(permission));
    }
}

TObjectId TClient::CreateObjectImpl(
    EObjectType type,
    TCellTag cellTag,
    const IAttributeDictionary& attributes,
    const TCreateObjectOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy(cellTag);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetSuppressTransactionCoordinatorSync(true);
    SetPrerequisites(batchReq, options);

    auto req = TMasterYPathProxy::CreateObject();
    SetMutationId(req, options);
    req->set_type(ToProto<int>(type));
    req->set_ignore_existing(options.IgnoreExisting);
    ToProto(req->mutable_object_attributes(), attributes);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>(0)
        .ValueOrThrow();

    auto objectId = FromProto<TObjectId>(rsp->object_id());

    if (rsp->two_phase_creation() && options.Sync) {
        const auto& config = Connection_->GetConfig();
        bool ok = false;
        NProfiling::TWallTimer timer;
        int retryIndex = 0;
        auto lifeStage = EObjectLifeStage::CreationStarted; // just a guess
        while (true) {
            if (retryIndex > config->ObjectLifeStageCheckRetryCount) {
                break;
            }
            if (retryIndex > 0 && timer.GetElapsedTime() > config->ObjectLifeStageCheckTimeout) {
                break;
            }

            ++retryIndex;

            YT_LOG_DEBUG("Retrieving object life stage (ObjectId: %v)",
                objectId);

            auto lifeStageYson = WaitFor(GetNode(FromObjectId(objectId) + "/@life_stage", /*options*/ {}))
                .ValueOrThrow();

            lifeStage = ConvertTo<EObjectLifeStage>(lifeStageYson);

            YT_LOG_DEBUG("Object life stage retrieved (ObjectId: %v, LifeStage: %v)",
                objectId,
                lifeStage);

            if (lifeStage == EObjectLifeStage::CreationCommitted) {
                ok = true;
                break;
            }

            TDelayedExecutor::WaitForDuration(config->ObjectLifeStageCheckPeriod);
        }

        if (!ok) {
            THROW_ERROR_EXCEPTION("Failed to wait until object %v creation is committed",
                objectId)
                << TErrorAttribute("retry_count", retryIndex)
                << TErrorAttribute("retry_time", timer.GetElapsedTime())
                << TErrorAttribute("last_seen_life_stage", lifeStage);
        }
    }

    return objectId;
}

TClusterMeta TClient::DoGetClusterMeta(
    const TGetClusterMetaOptions& options)
{
    auto proxy = CreateObjectServiceReadProxy(options);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetSuppressTransactionCoordinatorSync(true);
    SetBalancingHeader(batchReq, options);

    auto masterReq = TMasterYPathProxy::GetClusterMeta();
    masterReq->set_populate_node_directory(options.PopulateNodeDirectory);
    masterReq->set_populate_cluster_directory(options.PopulateClusterDirectory);
    masterReq->set_populate_medium_directory(options.PopulateMediumDirectory);
    masterReq->set_populate_master_cache_node_addresses(options.PopulateMasterCacheNodeAddresses);
    masterReq->set_populate_timestamp_provider_node_addresses(options.PopulateTimestampProviderAddresses);
    masterReq->set_populate_features(options.PopulateFeatures);
    SetCachingHeader(masterReq, options);
    batchReq->AddRequest(masterReq, "cluster_meta");

    if (options.PopulateFeatures) {
        auto schedulerOrchidReq = TYPathProxy::Get("//sys/scheduler/orchid/scheduler/supported_features");
        SetCachingHeader(schedulerOrchidReq, options);
        batchReq->AddRequest(schedulerOrchidReq, "scheduler_features");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto masterRsp = batchRsp->GetResponse<TMasterYPathProxy::TRspGetClusterMeta>("cluster_meta")
        .ValueOrThrow();

    TClusterMeta meta;
    if (options.PopulateNodeDirectory) {
        meta.NodeDirectory = std::make_shared<NNodeTrackerClient::NProto::TNodeDirectory>();
        meta.NodeDirectory->Swap(masterRsp->mutable_node_directory());
    }
    if (options.PopulateClusterDirectory) {
        meta.ClusterDirectory = std::make_shared<NHiveClient::NProto::TClusterDirectory>();
        meta.ClusterDirectory->Swap(masterRsp->mutable_cluster_directory());
    }
    if (options.PopulateMediumDirectory) {
        meta.MediumDirectory = std::make_shared<NChunkClient::NProto::TMediumDirectory>();
        meta.MediumDirectory->Swap(masterRsp->mutable_medium_directory());
    }
    if (options.PopulateMasterCacheNodeAddresses) {
        meta.MasterCacheNodeAddresses = FromProto<std::vector<TString>>(masterRsp->master_cache_node_addresses());
    }
    if (options.PopulateTimestampProviderAddresses) {
        meta.TimestampProviderAddresses = FromProto<std::vector<TString>>(masterRsp->timestamp_provider_node_addresses());
    }
    if (options.PopulateFeatures) {
        if (masterRsp->has_features()) {
            meta.Features = ConvertTo<IMapNodePtr>(TYsonStringBuf(masterRsp->features()));
        }
        auto schedulerRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("scheduler_features");
        if (schedulerRspOrError.IsOK() && schedulerRspOrError.Value()->has_value()) {
            if (!meta.Features) {
                meta.Features = GetEphemeralNodeFactory()->CreateMap();
            }
            auto schedulerFeatures = ConvertTo<IMapNodePtr>(TYsonStringBuf(schedulerRspOrError.Value()->value()));
            meta.Features = PatchNode(meta.Features, schedulerFeatures)->AsMap();
        }
    }
    return meta;
}

void TClient::DoCheckClusterLiveness(
    const TCheckClusterLivenessOptions& options)
{
    if (options.IsCheckTrivial()) {
        THROW_ERROR_EXCEPTION("No liveness check methods specified");
    }

    TMasterReadOptions masterReadOptions;
    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> futures;

    std::optional<int> bundleCheckSubrequestIndex;

    // Requests to primary master.
    if (options.CheckCypressRoot || options.CheckTabletCellBundle) {
        auto proxy = CreateObjectServiceReadProxy(masterReadOptions);
        auto batchReq = proxy.ExecuteBatch();

        if (options.CheckCypressRoot) {
            auto req = TYPathProxy::List("/");
            req->set_limit(1);
            batchReq->AddRequest(req);
        }

        if (auto bundleName = options.CheckTabletCellBundle) {
            bundleCheckSubrequestIndex = batchReq->GetSize();
            auto req = TYPathProxy::Get("//sys/tablet_cell_bundles/" + ToYPathLiteral(*bundleName) + "/@health");
            batchReq->AddRequest(req);
        }

        futures.push_back(batchReq->Invoke());
    }

    // Requests to secondary masters.
    if (options.CheckSecondaryMasterCells) {
        for (auto secondaryCellTag : Connection_->GetSecondaryMasterCellTags()) {
            auto proxy = CreateObjectServiceReadProxy(masterReadOptions, secondaryCellTag);
            auto batchReq = proxy.ExecuteBatch();

            auto req = TYPathProxy::List("/");
            req->set_limit(1);
            batchReq->AddRequest(req);

            futures.push_back(batchReq->Invoke());
        }
    }

    auto batchResponsesOrError = WaitFor(AllSucceeded(std::move(futures))
        .WithTimeout(Connection_->GetConfig()->ClusterLivenessCheckTimeout));
    if (!batchResponsesOrError.IsOK()) {
        if (batchResponsesOrError.FindMatching(NYT::EErrorCode::Timeout)) {
            THROW_ERROR_EXCEPTION(NApi::EErrorCode::ClusterLivenessCheckFailed,
                "Cluster liveness request timed out")
                << TError(batchResponsesOrError);
        } else {
            batchResponsesOrError.ThrowOnError();
        }
    }

    const auto& batchResponses = batchResponsesOrError.Value();
    for (const auto& batchResponse : batchResponses) {
        for (int responseIndex = 0; responseIndex < batchResponse->GetResponseCount(); ++responseIndex) {
            const auto& responseOrError = batchResponse->GetResponse(responseIndex);
            if (!responseOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(NApi::EErrorCode::ClusterLivenessCheckFailed,
                    "Cluster liveness subrequest failed")
                    << TError(responseOrError);
            }
        }
    }

    if (bundleCheckSubrequestIndex) {
        auto bundleCheckSubresponse = batchResponses[0]->GetResponse<TYPathProxy::TRspGet>(*bundleCheckSubrequestIndex)
            .Value();
        auto health = ConvertTo<ETabletCellHealth>(TYsonString(bundleCheckSubresponse->value()));
        if (health != ETabletCellHealth::Good && health != ETabletCellHealth::Degraded) {
            THROW_ERROR_EXCEPTION(NApi::EErrorCode::ClusterLivenessCheckFailed,
                "Tablet cell bundle health subrequest failed")
                << TErrorAttribute("bundle_name", *options.CheckTabletCellBundle)
                << TErrorAttribute("bundle_health", health);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
