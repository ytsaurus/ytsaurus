#include "registration_manager.h"

#include "registration_manager_base.h"
#include "registration_manager_new.h"
#include "config.h"
#include "dynamic_state.h"
#include "helpers.h"

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
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NSecurityClient;
using namespace NTabletClient;
using namespace NThreading;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManagerProfilingCounters::TQueueConsumerRegistrationManagerProfilingCounters(const TProfiler& profiler)
    : ListAllRegistrationsRequestCount(profiler.Counter("/list_all_registrations_request_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerOldImpl
    : public TQueueConsumerRegistrationManagerBase
{
public:
    TQueueConsumerRegistrationManagerOldImpl(
        TQueueConsumerRegistrationManagerConfigPtr config,
        TWeakPtr<NApi::NNative::IConnection> connection,
        std::optional<std::string> clusterName,
        IInvokerPtr invoker,
        TProfiler profiler,
        TLogger logger)
        : TQueueConsumerRegistrationManagerBase(
            std::move(config),
            std::move(connection),
            std::move(clusterName),
            std::move(invoker),
            std::move(profiler),
            std::move(logger))
        , CacheRefreshExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TQueueConsumerRegistrationManagerOldImpl::RefreshCache, MakeWeak(this)),
            Config_->CacheRefreshPeriod))
    { }

    void StartSync() override
    {
        TBase::StartSync();
        CacheRefreshExecutor_->Start();
    }

    void StopSync() override
    {
        TBase::StopSync();
        YT_UNUSED_FUTURE(CacheRefreshExecutor_->Stop());
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
                .Item("queue_consumer_registration_manager_implementation").Value(GetImplementationType())
            .EndAttributes()
            .BeginMap()
                .Item("effective_config").Value(config)
                .Item("unrecognized_config_options").Value(config->GetRecursiveUnrecognized())
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

protected:
    std::optional<TConsumerRegistrationTableRow> DoFindRegistration(
        NYPath::TRichYPath resolvedQueue,
        NYPath::TRichYPath resolvedConsumer) override
    {
        auto guard = ReaderGuard(CacheSpinLock_);

        auto it = Registrations_.find(std::pair{resolvedQueue, resolvedConsumer});

        if (it == Registrations_.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    std::vector<TConsumerRegistrationTableRow> DoListRegistrations(
        std::optional<NYPath::TRichYPath> resolvedQueue,
        std::optional<NYPath::TRichYPath> resolvedConsumer) override
    {
        auto guard = ReaderGuard(CacheSpinLock_);

        auto comparePaths = [] (const TRichYPath& lhs, const TRichYPath& rhs) {
            return lhs.GetPath() == rhs.GetPath() && lhs.GetCluster() == rhs.GetCluster();
        };

        std::vector<TConsumerRegistrationTableRow> result;
        for (const auto& [key, registration] : Registrations_) {
            const auto& [keyQueue, keyConsumer] = key;
            if (resolvedQueue && !comparePaths(*resolvedQueue, keyQueue)) {
                continue;
            }
            if (resolvedConsumer && !comparePaths(*resolvedConsumer, keyConsumer)) {
                continue;
            }

            result.push_back(registration);
        }

        return result;
    }

    //! Polls the registration dynamic table and fills the cached map.
    void RefreshCache() override
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

    EQueueConsumerRegistrationManagerImplementation GetImplementationType() const override
    {
        return EQueueConsumerRegistrationManagerImplementation::Legacy;
    }

    void Reconfigure(
        const TQueueConsumerRegistrationManagerConfigPtr& oldConfig,
        const TQueueConsumerRegistrationManagerConfigPtr& newConfig) override
    {
        TBase::Reconfigure(oldConfig, newConfig);

        if (newConfig->CacheRefreshPeriod != oldConfig->CacheRefreshPeriod) {
            YT_LOG_DEBUG(
                "Resetting queue consumer registration manager cache refresh period (Period: %v, %v)",
                oldConfig->CacheRefreshPeriod,
                newConfig->CacheRefreshPeriod);
            CacheRefreshExecutor_->SetPeriod(newConfig->CacheRefreshPeriod);
        }
    }

    NYPath::TRichYPath ResolveReplica(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo,
        bool throwOnFailure) const override
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

private:
    const NConcurrency::TPeriodicExecutorPtr CacheRefreshExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CacheSpinLock_);
    THashMap<std::pair<NYPath::TRichYPath, NYPath::TRichYPath>, TConsumerRegistrationTableRow> Registrations_;
    THashMap<NYPath::TRichYPath, NYPath::TRichYPath> ReplicaToReplicatedTable_;

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

private:
    using TBase = TQueueConsumerRegistrationManagerBase;
};

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManagerBasePtr CreateQueueConsumerRegistrationManagerOldImpl(
    TQueueConsumerRegistrationManagerConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> connection,
    std::optional<std::string> clusterName,
    IInvokerPtr invoker,
    TProfiler profiler,
    TLogger logger)
{
    auto result = New<TQueueConsumerRegistrationManagerOldImpl>(
        std::move(config),
        std::move(connection),
        std::move(clusterName),
        std::move(invoker),
        std::move(profiler),
        std::move(logger));

    result->Initialize();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TCallback<TQueueConsumerRegistrationManagerBasePtr(TQueueConsumerRegistrationManagerConfigPtr, TWeakPtr<NApi::NNative::IConnection>, std::optional<std::string>, IInvokerPtr, TProfiler, TLogger)> GetRegistrationManagerImplFactory(
    EQueueConsumerRegistrationManagerImplementation implementation)
{
    switch (implementation) {
        case EQueueConsumerRegistrationManagerImplementation::Legacy:
            return BIND_NO_PROPAGATE(&NDetail::CreateQueueConsumerRegistrationManagerOldImpl);
        case EQueueConsumerRegistrationManagerImplementation::AsyncExpiringCache:
            return BIND_NO_PROPAGATE(&NDetail::CreateQueueConsumerRegistrationManagerNewImpl);
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerWrapper
    : public IQueueConsumerRegistrationManager
{
public:
    TQueueConsumerRegistrationManagerWrapper(
        TQueueConsumerRegistrationManagerConfigPtr config,
        TWeakPtr<NApi::NNative::IConnection> connection,
        std::optional<std::string> clusterName,
        IInvokerPtr invoker,
        TProfiler profiler,
        TLogger logger)
        : Connection_(std::move(connection))
        , Invoker_(std::move(invoker))
        , Config_(std::move(config))
        , ClusterName_(std::move(clusterName))
        , ConfigurationRefreshExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TQueueConsumerRegistrationManagerWrapper::RefreshConfiguration, MakeWeak(this)),
            Config_->ConfigurationRefreshPeriod))
        , Profiler_(profiler)
        , Logger(logger)
        , AppliedConfig_(Config_)
    {
        // NB(apachee): Skip acquiring the lock in constructor.
        // NB(apachee): Created in constructor body, since this method relies on many parts of the object state.
        Impl_ = CreateRegistrationManagerGuarded(Config_);
    }

    void StartSync() override
    {
        auto guard = Guard(StateLock_);
        IsStarted_ = true;

        ConfigurationRefreshExecutor_->Start();
        Impl_.Acquire()->StartSync();
    }

    void StopSync() override
    {
        auto guard = Guard(StateLock_);
        IsStarted_ = false;

        YT_UNUSED_FUTURE(ConfigurationRefreshExecutor_->Stop());
        Impl_.Acquire()->StopSync();
    }

    TGetRegistrationResult GetRegistrationOrThrow(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) override
    {
        return Impl_.Acquire()->GetRegistrationOrThrow(std::move(queue), std::move(consumer));
    }

    std::vector<TConsumerRegistrationTableRow> ListRegistrations(
        std::optional<NYPath::TRichYPath> queue,
        std::optional<NYPath::TRichYPath> consumer) override
    {
        return Impl_.Acquire()->ListRegistrations(std::move(queue), std::move(consumer));
    }

    void RegisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer,
        bool vital,
        const std::optional<std::vector<int>>& partitions = {}) override
    {
        return Impl_.Acquire()->RegisterQueueConsumer(std::move(queue), std::move(consumer), vital, partitions);
    }

    void UnregisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) override
    {
        return Impl_.Acquire()->UnregisterQueueConsumer(std::move(queue), std::move(consumer));
    }

    void Clear() override
    {
        return Impl_.Acquire()->Clear();
    }

    //! Exports information about current applied config and cached registrations.
    void BuildOrchid(NYTree::TFluentAny fluent) override
    {
        Impl_.Acquire()->BuildOrchid(fluent);
    }

private:
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const TQueueConsumerRegistrationManagerConfigPtr Config_;
    const std::optional<std::string> ClusterName_;
    const NConcurrency::TPeriodicExecutorPtr ConfigurationRefreshExecutor_;
    const TProfiler Profiler_;
    const TLogger Logger;

    //! Last successfully applied configuration.
    //! \note Does not require synchronization, since it is only accessed from periodic executor.
    TQueueConsumerRegistrationManagerConfigPtr AppliedConfig_;

    using TQueueConsumerRegistrationManagerBaseAtomicPtr = TAtomicIntrusivePtr<TQueueConsumerRegistrationManagerBase>;
    TQueueConsumerRegistrationManagerBaseAtomicPtr Impl_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, StateLock_);
    bool IsStarted_ = false;

    TQueueConsumerRegistrationManagerBasePtr CreateRegistrationManager(TQueueConsumerRegistrationManagerConfigPtr config, TGuard<TSpinLock>& /*stateLockGuard*/)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(StateLock_);

        return CreateRegistrationManagerGuarded(std::move(config));
    }

    TQueueConsumerRegistrationManagerBasePtr CreateRegistrationManagerGuarded(TQueueConsumerRegistrationManagerConfigPtr config)
    {
        YT_LOG_INFO(
            "Creating new queue consumer registration manager implementation (Implementation: %v)",
            config->Implementation);

        auto registrationManagerImplFactory = GetRegistrationManagerImplFactory(config->Implementation);
        auto impl = registrationManagerImplFactory(std::move(config), Connection_, ClusterName_, Invoker_, Profiler_, Logger);

        if (IsStarted_) {
            impl->StartSync();
        }

        return impl;
    }

    //! Updates configuration from cluster connection and calls #Reconfigure
    //! to process the changes after a successful update.
    void RefreshConfiguration()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        try {
            GuardedRefreshConfiguration();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Could not refresh queue consumer registration manager configuration");
        }
    }

    void GuardedRefreshConfiguration()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_INFO("Refreshing queue consumer registration manager configuration");

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

        auto oldConfig = AppliedConfig_;

        YT_VERIFY(oldConfig);
        YT_VERIFY(newConfig);

        Reconfigure(oldConfig, newConfig);

        AppliedConfig_ = newConfig;

        YT_LOG_INFO("Refreshed queue consumer registration manager configuration");
    }

    void Reconfigure(const TQueueConsumerRegistrationManagerConfigPtr& oldConfig, const TQueueConsumerRegistrationManagerConfigPtr& newConfig)
    {
        if (*oldConfig == *newConfig) {
            YT_LOG_INFO("Skipping queue consumer registration manager reconfiguration since the configuration is unchanged");
            return;
        }

        if (newConfig->ConfigurationRefreshPeriod != oldConfig->ConfigurationRefreshPeriod) {
            YT_LOG_INFO(
                "Resetting queue consumer registration manager configuration refresh period (Period: %v, %v)",
                oldConfig->ConfigurationRefreshPeriod,
                newConfig->ConfigurationRefreshPeriod);
            ConfigurationRefreshExecutor_->SetPeriod(newConfig->ConfigurationRefreshPeriod);
        }

        // Implementation reconfiguration.
        if (oldConfig->Implementation == newConfig->Implementation) {
            Impl_.Acquire()->Reconfigure(oldConfig, newConfig);
        } else {
            // XXX(apachee): Add safeguard from constant constant implementation switches?
            // Creating old implementation causes full-scans. If reconfiguration fails we might end up stuck here.
            // This would lead to catastrophic dynamic state overload.
            // Maybe use in retrying periodic executor instead?
            YT_LOG_INFO(
                "Changing queue consumer registration manager implementation (OldImplementation: %v, NewImplementation: %v)",
                oldConfig->Implementation,
                newConfig->Implementation);

            auto guard = Guard(StateLock_);
            auto newImpl = CreateRegistrationManager(newConfig, guard);
            auto oldImpl = Impl_.Exchange(newImpl);
            oldImpl->StopSync();
        }

        YT_LOG_INFO(
            "Queue consumer registration manager reconfigured (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }
};

////////////////////////////////////////////////////////////////////////////////

IQueueConsumerRegistrationManagerPtr CreateQueueConsumerRegistrationManager(
    TQueueConsumerRegistrationManagerConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> connection,
    std::optional<std::string> clusterName,
    IInvokerPtr invoker,
    TProfiler profiler,
    TLogger logger)
{
    return New<TQueueConsumerRegistrationManagerWrapper>(
        std::move(config),
        std::move(connection),
        std::move(clusterName),
        std::move(invoker),
        std::move(profiler),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
