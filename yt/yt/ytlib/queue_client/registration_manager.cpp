#include "registration_manager.h"
#include "config.h"
#include "dynamic_state.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NTabletClient;
using namespace NThreading;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* OptionalToPointer(std::optional<T>& optionalValue)
{
    if (optionalValue) {
        return &(*optionalValue);
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTable>
TIntrusivePtr<TTable> CreateStateTableClientOrThrow(
    const TWeakPtr<NApi::NNative::IConnection>& connection,
    const std::optional<TString>& cluster,
    const TYPath& path,
    const TString& user)
{
    auto localConnection = connection.Lock();
    if (!localConnection) {
        THROW_ERROR_EXCEPTION("Queue consumer registration cache owning connection expired");
    }

    IClientPtr client;
    auto clientOptions = TClientOptions::FromUser(user);
    if (cluster) {
        auto remoteConnection = localConnection->GetClusterDirectory()->GetConnectionOrThrow(*cluster);
        client = remoteConnection->CreateClient(clientOptions);
    } else {
        client = localConnection->CreateClient(clientOptions);
    }

    return New<TTable>(path, std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

void HandleTableMountInfoError(
    const TRichYPath& objectPath,
    const TErrorOr<TTableMountInfoPtr>& tableMountInfoOrError,
    bool throwOnFailure,
    const NLogging::TLogger& Logger)
{
    if (!tableMountInfoOrError.IsOK()) {
        YT_LOG_DEBUG(
            tableMountInfoOrError,
            "Failed to get table mount info to perform registration manager resolutions (Object: %v)",
            objectPath);

        if (throwOnFailure) {
            THROW_ERROR_EXCEPTION(
                "Failed to get table mount info to perform registration manager resolutions for object %Qv",
                objectPath)
            << tableMountInfoOrError;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TString NormalizeClusterName(TStringBuf clusterName)
{
    clusterName.ChopSuffix(".yt.yandex.net");
    return TString(clusterName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManager::TQueueConsumerRegistrationManager(
    TQueueConsumerRegistrationManagerConfigPtr config,
    NNative::IConnection* connection,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger)
    : Config_(std::move(config))
    , Connection_(connection)
    , Invoker_(std::move(invoker))
    , ClusterName_(connection->GetClusterName())
    , ConfigurationRefreshExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TQueueConsumerRegistrationManager::RefreshConfiguration, MakeWeak(this)),
        Config_->ConfigurationRefreshPeriod))
    , CacheRefreshExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TQueueConsumerRegistrationManager::RefreshCache, MakeWeak(this)),
        Config_->CacheRefreshPeriod))
    , Logger(logger)
    , DynamicConfig_(Config_)
{ }

void TQueueConsumerRegistrationManager::StartSync() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting queue consumer registration manager sync");
    ConfigurationRefreshExecutor_->Start();
    CacheRefreshExecutor_->Start();
}

void TQueueConsumerRegistrationManager::StopSync() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Stopping queue consumer registration manager sync");
    YT_UNUSED_FUTURE(CacheRefreshExecutor_->Stop());
    YT_UNUSED_FUTURE(ConfigurationRefreshExecutor_->Stop());
}

TQueueConsumerRegistrationManager::TGetRegistrationResult TQueueConsumerRegistrationManager::GetRegistration(
    TRichYPath queue,
    TRichYPath consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();
    YT_VERIFY(config);

    if (config->BypassCaching) {
        RefreshCache();
    }

    Resolve(config, &queue, &consumer, /*throwOnFailure*/ true);

    TGetRegistrationResult result{.ResolvedQueue = queue, .ResolvedConsumer = consumer};

    auto guard = ReaderGuard(CacheSpinLock_);

    if (auto it = Registrations_.find(std::pair{queue, consumer}); it != Registrations_.end()) {
        result.Registration = it->second;
    }

    return result;
}

TRichYPath TQueueConsumerRegistrationManager::ResolveObjectPhysicalPath(
    const TRichYPath& objectPath,
    const TTableMountInfoPtr& tableMountInfo) const
{
    auto resolvedObjectPath = objectPath;
    resolvedObjectPath.SetPath(tableMountInfo->PhysicalPath);

    if (resolvedObjectPath.GetPath() != objectPath.GetPath()) {
        YT_LOG_DEBUG(
            "Using corresponding physical path instead of symlinked path (SymlinkedPath: %v, PhysicalPath: %v)",
            objectPath,
            resolvedObjectPath);
    }

    return resolvedObjectPath;
}

TRichYPath TQueueConsumerRegistrationManager::ResolveReplica(
    const TRichYPath& objectPath,
    const TTableMountInfoPtr& tableMountInfo,
    bool throwOnFailure) const
{
    auto guard = ReaderGuard(CacheSpinLock_);

    auto replicaToReplicatedTableIt = ReplicaToReplicatedTable_.find(objectPath);
    if (replicaToReplicatedTableIt != ReplicaToReplicatedTable_.end()) {
        YT_LOG_DEBUG(
            "Using corresponding replicated table path in request instead of replica path (ReplicaPath: %v, ReplicatedTablePath: %v)",
            objectPath,
            replicaToReplicatedTableIt->second);
        return replicaToReplicatedTableIt->second;
    } else if (tableMountInfo->UpstreamReplicaId != NullObjectId && throwOnFailure) {
        THROW_ERROR_EXCEPTION(
            "Cannot perform request for replica %Qv with unknown [chaos_]replicated_table; please contact YT support,"
            " unless you specifically understand what this error means",
            objectPath);
    }

    return objectPath;
}

void TQueueConsumerRegistrationManager::Resolve(
    const TQueueConsumerRegistrationManagerConfigPtr& config,
    NYPath::TRichYPath* queuePath,
    NYPath::TRichYPath* consumerPath,
    bool throwOnFailure)
{
    ValidateClusterNameConfigured();

    auto connection = Connection_.Lock();
    if (!connection) {
        // NB: This error indicates that we are in the process of stopping the connection or something went wrong
        // completely. In both cases it is OK to throw even when `throwOnFailure` is false.
        THROW_ERROR_EXCEPTION("Error perform path resolution for queue and consumer due to expired connection");
    }

    auto pathHandler = [&](NYPath::TRichYPath* path) {
        auto tableMountInfoOrError = WaitFor(GetTableMountInfo(*path, connection));
        HandleTableMountInfoError(*queuePath, tableMountInfoOrError, throwOnFailure, Logger);

        if (!path->GetCluster()) {
            path->SetCluster(*ClusterName_);
        }
        path->SetCluster(NormalizeClusterName(*path->GetCluster()));

        if (config->ResolveSymlinks && tableMountInfoOrError.IsOK()) {
            *path = ResolveObjectPhysicalPath(*path, tableMountInfoOrError.Value());
        }

        if (config->ResolveReplicas && tableMountInfoOrError.IsOK()) {
            *path = ResolveReplica(*path, tableMountInfoOrError.Value(), throwOnFailure);
        }
    };

    if (queuePath) {
        pathHandler(queuePath);
    }

    if (consumerPath) {
        pathHandler(consumerPath);
    }
}

std::vector<TConsumerRegistrationTableRow> TQueueConsumerRegistrationManager::ListRegistrations(
    std::optional<TRichYPath> queue,
    std::optional<TRichYPath> consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();
    YT_VERIFY(config);

    if (config->BypassCaching) {
        RefreshCache();
    }

    // NB: We want to return an empty list if the provided queue/consumer does not exist,
    // thus we ignore resolution failures.
    Resolve(config, OptionalToPointer(queue), OptionalToPointer(consumer), /*throwOnFailure*/ false);

    auto guard = ReaderGuard(CacheSpinLock_);

    auto comparePaths = [](const TRichYPath& lhs, const TRichYPath& rhs) {
        return lhs.GetPath() == rhs.GetPath() && lhs.GetCluster() == rhs.GetCluster();
    };

    std::vector<TConsumerRegistrationTableRow> result;
    for (const auto& [key, registration] : Registrations_) {
        const auto& [keyQueue, keyConsumer] = key;
        if (queue && !comparePaths(*queue, keyQueue)) {
            continue;
        }
        if (consumer && !comparePaths(*consumer, keyConsumer)) {
            continue;
        }

        result.push_back(registration);
    }

    return result;
}

void TQueueConsumerRegistrationManager::RegisterQueueConsumer(
    TRichYPath queue,
    TRichYPath consumer,
    bool vital,
    const std::optional<std::vector<int>>& partitions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();
    Resolve(config, &queue, &consumer, /*throwOnFailure*/ true);

    auto registrationTableClient = CreateRegistrationTableWriteClientOrThrow();
    WaitFor(registrationTableClient->Insert(std::vector{TConsumerRegistrationTableRow{
        .Queue = TCrossClusterReference::FromRichYPath(queue),
        .Consumer = TCrossClusterReference::FromRichYPath(consumer),
        .Vital = vital,
        .Partitions = partitions,
    }}))
        .ValueOrThrow();
}

void TQueueConsumerRegistrationManager::UnregisterQueueConsumer(
    TRichYPath queue,
    TRichYPath consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();
    // NB: We want to allow to delete registrations with nonexistent queues/consumers, therefore we don't throw exceptions.
    Resolve(config, &queue, &consumer, /*throwOnFailure*/ false);

    auto registrationTableClient = CreateRegistrationTableWriteClientOrThrow();
    WaitFor(registrationTableClient->Delete(std::vector{TConsumerRegistrationTableRow{
        .Queue = TCrossClusterReference::FromRichYPath(queue),
        .Consumer = TCrossClusterReference::FromRichYPath(consumer),
    }}))
        .ValueOrThrow();
}

void TQueueConsumerRegistrationManager::Clear()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(CacheSpinLock_);
    Registrations_.clear();
    ReplicaToReplicatedTable_.clear();
}

void TQueueConsumerRegistrationManager::RefreshCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    try {
        GuardedRefreshCache();
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Could not refresh queue consumer registration cache");
    }

    try {
        GuardedRefreshReplicationTableMappingCache();
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Could not refresh queue consumer replication table mapping cache");
    }
}

void TQueueConsumerRegistrationManager::GuardedRefreshCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Refreshing queue consumer registration cache");

    auto config = GetDynamicConfig();
    auto registrations = FetchStateRowsOrThrow<TConsumerRegistrationTable>(
        config->StateReadPath,
        config->User);

    auto guard = WriterGuard(CacheSpinLock_);

    Registrations_.clear();

    for (const auto& registration : registrations) {
        Registrations_[std::pair{TRichYPath{registration.Queue}, TRichYPath{registration.Consumer}}] = registration;
    }

    YT_LOG_DEBUG("Queue consumer registration cache refreshed (RegistrationCount: %v)", Registrations_.size());
}

void TQueueConsumerRegistrationManager::GuardedRefreshReplicationTableMappingCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Refreshing queue consumer replication table mapping cache");

    auto config = GetDynamicConfig();
    auto replicatedTableMapping = FetchStateRowsOrThrow<TReplicatedTableMappingTable>(
        config->ReplicatedTableMappingReadPath,
        config->User);

    auto guard = WriterGuard(CacheSpinLock_);

    ReplicaToReplicatedTable_.clear();

    for (const auto& replicatedTableInfo : replicatedTableMapping) {
        for (const auto& replica : replicatedTableInfo.GetReplicas()) {
            ReplicaToReplicatedTable_[replica] = replicatedTableInfo.Ref;
        }
    }

    YT_LOG_DEBUG(
        "Queue consumer replication table mapping cache refreshed (MappingRows: %v, Replicas: %v)",
        replicatedTableMapping.size(),
        ReplicaToReplicatedTable_.size());
}

void TQueueConsumerRegistrationManager::RefreshConfiguration()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    try {
        GuardedRefreshConfiguration();
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Could not refresh queue consumer registration manager configuration");
    }
}

void TQueueConsumerRegistrationManager::GuardedRefreshConfiguration()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Refreshing queue consumer registration manager configuration");

    auto newConfig = Config_;

    if (ClusterName_) {
        if (auto localConnection = Connection_.Lock()) {
            if (auto remoteConnection = localConnection->GetClusterDirectory()->FindConnection(*ClusterName_)) {
                newConfig = remoteConnection->GetConfig()->QueueAgent->QueueConsumerRegistrationManager;
                YT_LOG_DEBUG(
                    "Retrieved queue consumer registration manager dynamic config (Config: %v)",
                    ConvertToYsonString(newConfig, EYsonFormat::Text));
            }
        }
    }

    auto oldConfig = GetDynamicConfig();

    YT_VERIFY(oldConfig);
    YT_VERIFY(newConfig);

    // NB: Should be safe to call inside the periodic executor, since it doesn't post any new callbacks while executing a callback.
    // This just sets the internal period to be used for scheduling the next invocation.
    if (newConfig->ConfigurationRefreshPeriod != oldConfig->ConfigurationRefreshPeriod) {
        YT_LOG_DEBUG(
            "Resetting queue consumer registration manager configuration refresh period (Period: %v -> %v)",
            oldConfig->ConfigurationRefreshPeriod,
            newConfig->ConfigurationRefreshPeriod);
        ConfigurationRefreshExecutor_->SetPeriod(newConfig->ConfigurationRefreshPeriod);
    }

    if (newConfig->CacheRefreshPeriod != oldConfig->CacheRefreshPeriod) {
        YT_LOG_DEBUG(
            "Resetting queue consumer registration manager cache refresh period (Period: %v -> %v)",
            oldConfig->CacheRefreshPeriod,
            newConfig->CacheRefreshPeriod);
        CacheRefreshExecutor_->SetPeriod(newConfig->CacheRefreshPeriod);
    }

    {
        auto guard = WriterGuard(ConfigurationSpinLock_);
        DynamicConfig_ = newConfig;
    }

    YT_LOG_DEBUG("Refreshed queue consumer registration manager configuration");
}

TConsumerRegistrationTablePtr TQueueConsumerRegistrationManager::CreateRegistrationTableWriteClientOrThrow() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();

    return CreateStateTableClientOrThrow<TConsumerRegistrationTable>(
        Connection_,
        config->StateWritePath.GetCluster(),
        config->StateWritePath.GetPath(),
        config->User);
}

template <class TTable>
std::vector<typename TTable::TRowType> TQueueConsumerRegistrationManager::FetchStateRowsOrThrow(
    const NYPath::TRichYPath& stateReadPath,
    const TString& user) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TIntrusivePtr<TTable>> readClients;

    auto readClusters = stateReadPath.GetClusters();
    if (readClusters && !readClusters->empty()) {
        readClients.reserve(readClusters->size());
        for (const auto& cluster : *readClusters) {
            auto remoteReadClient = CreateStateTableClientOrThrow<TTable>(
                Connection_,
                cluster,
                stateReadPath.GetPath(),
                user);
            readClients.push_back(std::move(remoteReadClient));
        }
    } else {
        auto localReadClient = CreateStateTableClientOrThrow<TTable>(
            Connection_,
            /*cluster*/ {},
            stateReadPath.GetPath(),
            user);
        readClients.push_back(std::move(localReadClient));
    }

    std::vector<TFuture<std::vector<typename TTable::TRowType>>> asyncRows;
    asyncRows.reserve(readClients.size());
    for (const auto& client : readClients) {
        asyncRows.push_back(client->Select());
    }

    // NB: It's hard to return a future here, since you would need to keep all the clients alive as well.
    return WaitFor(AnySucceeded(std::move(asyncRows)))
        .ValueOrThrow();
}

TQueueConsumerRegistrationManagerConfigPtr TQueueConsumerRegistrationManager::GetDynamicConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ConfigurationSpinLock_);
    // NB: Always non-null, since it is initialized from the static config in the constructor.
    return DynamicConfig_;
}

void TQueueConsumerRegistrationManager::BuildOrchid(TFluentAny fluent)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();
    YT_VERIFY(config);

    if (config->BypassCaching) {
        RefreshCache();
    }

    decltype(Registrations_) registrations;
    {
        auto guard = ReaderGuard(CacheSpinLock_);
        registrations = Registrations_;
    }

    fluent
        .BeginAttributes()
            .Item("opaque").Value(true)
        .EndAttributes()
        .BeginMap()
            .Item("effective_config").Value(config)
            .Item("registrations").DoListFor(registrations, [&] (TFluentList fluent, const auto& pair) {
                const TConsumerRegistrationTableRow& registration = pair.second;
                fluent
                    .Item()
                        .BeginMap()
                            .Item("queue").Value(registration.Queue)
                            .Item("consumer").Value(registration.Consumer)
                            .Item("vital").Value(registration.Vital)
                            .Item("partitions").Value(registration.Partitions)
                        .EndMap();
            })
        .EndMap();
}

void TQueueConsumerRegistrationManager::ValidateClusterNameConfigured() const
{
    if (!ClusterName_) {
        THROW_ERROR_EXCEPTION("Cannot serve request, queue consumer registration manager was not properly configured with a cluster name");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
