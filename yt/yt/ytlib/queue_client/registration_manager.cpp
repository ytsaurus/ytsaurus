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
    const std::optional<std::string>& cluster,
    const TYPath& path,
    const std::string& user)
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

std::string NormalizeClusterName(TStringBuf clusterName)
{
    clusterName.ChopSuffix(".yt.yandex.net");
    return std::string(clusterName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManager
    : public IQueueConsumerRegistrationManager
{
public:
    TQueueConsumerRegistrationManager(
        TQueueConsumerRegistrationManagerConfigPtr config,
        NApi::NNative::IConnection* connection,
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

    void StartSync() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Starting queue consumer registration manager sync");
        ConfigurationRefreshExecutor_->Start();
        CacheRefreshExecutor_->Start();
    }
    
    void StopSync() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Stopping queue consumer registration manager sync");
        YT_UNUSED_FUTURE(CacheRefreshExecutor_->Stop());
        YT_UNUSED_FUTURE(ConfigurationRefreshExecutor_->Stop());
    }

    TGetRegistrationResult GetRegistrationOrThrow(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto config = GetDynamicConfig();
        YT_VERIFY(config);

        if (config->BypassCaching) {
            RefreshCache();
        }

        auto rawQueue = queue;
        auto rawConsumer = consumer;

        Resolve(config, &queue, &consumer, /*throwOnFailure*/ true);

        TGetRegistrationResult result{.ResolvedQueue = queue, .ResolvedConsumer = consumer};

        auto guard = ReaderGuard(CacheSpinLock_);

        if (auto it = Registrations_.find(std::pair{queue, consumer}); it != Registrations_.end()) {
            result.Registration = it->second;
            return result;
        }

        THROW_ERROR_EXCEPTION(
            NYT::NSecurityClient::EErrorCode::AuthorizationError,
            "Consumer %v is not registered for queue %v",
            consumer,
            queue)
            << TErrorAttribute("raw_queue", rawQueue)
            << TErrorAttribute("raw_consumer", rawConsumer);
    }

    std::vector<TConsumerRegistrationTableRow> ListRegistrations(
        std::optional<NYPath::TRichYPath> queue,
        std::optional<NYPath::TRichYPath> consumer) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto config = GetDynamicConfig();
        YT_VERIFY(config);

        if (config->BypassCaching) {
            RefreshCache();
        }

        // NB: We want to return an empty list if the provided queue/consumer does not exist,
        // thus we ignore resolution failures.
        Resolve(config, OptionalToPointer(queue), OptionalToPointer(consumer), /*throwOnFailure*/ false);

        auto guard = ReaderGuard(CacheSpinLock_);

        auto comparePaths = [] (const TRichYPath& lhs, const TRichYPath& rhs) {
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

    void RegisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer,
        bool vital,
        const std::optional<std::vector<int>>& partitions = {}) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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

    void UnregisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(CacheSpinLock_);
        Registrations_.clear();
        ReplicaToReplicatedTable_.clear();
    }

    void BuildOrchid(NYTree::TFluentAny fluent) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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

private:
    const TQueueConsumerRegistrationManagerConfigPtr Config_;
    // The connection holds a strong reference to this object.
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const std::optional<std::string> ClusterName_;
    const NConcurrency::TPeriodicExecutorPtr ConfigurationRefreshExecutor_;
    const NConcurrency::TPeriodicExecutorPtr CacheRefreshExecutor_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConfigurationSpinLock_);
    TQueueConsumerRegistrationManagerConfigPtr DynamicConfig_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CacheSpinLock_);
    THashMap<std::pair<NYPath::TRichYPath, NYPath::TRichYPath>, TConsumerRegistrationTableRow> Registrations_;
    THashMap<NYPath::TRichYPath, NYPath::TRichYPath> ReplicaToReplicatedTable_;

    //! Polls the registration dynamic table and fills the cached map.
    void RefreshCache()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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

    void GuardedRefreshCache()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto config = GetDynamicConfig();

        YT_LOG_DEBUG("Refreshing queue consumer registration cache (StateReadPath: %v)", config->StateReadPath);

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

    void GuardedRefreshReplicationTableMappingCache()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto config = GetDynamicConfig();

        YT_LOG_DEBUG("Refreshing queue consumer replication table mapping cache (ReplicatedTableMappingReadPath: %v)", config->ReplicatedTableMappingReadPath);

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

    //! Retrieves the dynamic config from cluster directory, applies it and stores a local copy.
    void RefreshConfiguration()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        try {
            GuardedRefreshConfiguration();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Could not refresh queue consumer registration manager configuration");
        }
    }

    void GuardedRefreshConfiguration()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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

    //! Attempts to establish a connection to the registration table's (potentially remote) cluster
    //! and returns a client to be used for modifying the table.
    TConsumerRegistrationTablePtr CreateRegistrationTableWriteClientOrThrow() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto config = GetDynamicConfig();

        return CreateStateTableClientOrThrow<TConsumerRegistrationTable>(
            Connection_,
            config->StateWritePath.GetCluster(),
            config->StateWritePath.GetPath(),
            config->User);
    }

    //! Collects state rows from the clusters specified in state read path.
    //! The first successful response is returned.
    template <class TTable>
    std::vector<typename TTable::TRowType> FetchStateRowsOrThrow(
        const NYPath::TRichYPath& stateReadPath,
        const std::string& user) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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

    //! Returns the stored dynamic config.
    TQueueConsumerRegistrationManagerConfigPtr GetDynamicConfig() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(ConfigurationSpinLock_);
        // NB: Always non-null, since it is initialized from the static config in the constructor.
        return DynamicConfig_;
    }

    //! Resolves physical path for object.
    //! If throwOnFailure is false, errors during resolution are written to the log only and the path is returned unchanged.
    NYPath::TRichYPath ResolveObjectPhysicalPath(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo) const
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

    //! Resolves the corresponding chaos_replicated_table/replicated_table path for chaos/replicated table replicas
    //! via the replicated table mapping cache.
    NYPath::TRichYPath ResolveReplica(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo,
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

    //! Performs all necessary resolutions for a pair of queue and consumer paths in accordance with the provided config.
    //! Modifies the passed pointers, if not null.
    //! Effectively performs the following:
    //!     1. If cluster is not set, sets it to the statically configured cluster.
    //!     2. Resolves physical paths for potential symlinks.
    //!     3. Resolves [chaos] replicated table path for replicas.
    //! If throwOnFailure is false, errors during symlink resolution are written to the log only.
    void Resolve(
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

        auto pathResolver = [&](NYPath::TRichYPath* path) {
            auto tableMountInfoOrError = WaitFor(GetTableMountInfo(*path, connection));
            HandleTableMountInfoError(*path, tableMountInfoOrError, throwOnFailure, Logger);

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
            pathResolver(queuePath);
        }

        if (consumerPath) {
            pathResolver(consumerPath);
        }
    }

    //! Validates that a statically configured cluster name is set.
    void ValidateClusterNameConfigured() const
    {
        if (!ClusterName_) {
            THROW_ERROR_EXCEPTION("Cannot serve request, queue consumer registration manager was not properly configured with a cluster name");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IQueueConsumerRegistrationManagerPtr CreateQueueConsumerRegistrationManager(
    TQueueConsumerRegistrationManagerConfigPtr config,
    NApi::NNative::IConnection* connection,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger)
{
    return New<TQueueConsumerRegistrationManager>(
        std::move(config),
        connection,
        std::move(invoker),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
