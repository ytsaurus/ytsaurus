#include "registration_manager.h"
#include "registration_manager_base.h"
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

class TQueueConsumerRegistrationManager
    : public TQueueConsumerRegistrationManagerBase
{
public:
    TQueueConsumerRegistrationManager(
        TQueueConsumerRegistrationManagerConfigPtr config,
        NApi::NNative::IConnection* connection,
        IInvokerPtr invoker,
        const NProfiling::TProfiler& profiler,
        const NLogging::TLogger& logger)
        : TQueueConsumerRegistrationManagerBase(
            std::move(config),
            connection,
            std::move(invoker),
            profiler,
            logger)
        , CacheRefreshExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TQueueConsumerRegistrationManager::RefreshCache, MakeWeak(this)),
            Config_->CacheRefreshPeriod))
    { }

    void StartSync() const override
    {
        TBase::StartSync();
        CacheRefreshExecutor_->Start();
    }

    void StopSync() const override
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

    void OnDynamicConfigChanged(
        const TQueueConsumerRegistrationManagerConfigPtr oldConfig,
        const TQueueConsumerRegistrationManagerConfigPtr newConfig) override
    {
        if (newConfig->CacheRefreshPeriod != oldConfig->CacheRefreshPeriod) {
            YT_LOG_DEBUG(
                "Resetting queue consumer registration manager cache refresh period (Period: %v -> %v)",
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

IQueueConsumerRegistrationManagerPtr CreateQueueConsumerRegistrationManager(
    TQueueConsumerRegistrationManagerConfigPtr config,
    NApi::NNative::IConnection* connection,
    IInvokerPtr invoker,
    const NProfiling::TProfiler& profiler,
    const NLogging::TLogger& logger)
{
    return New<TQueueConsumerRegistrationManager>(
        std::move(config),
        connection,
        std::move(invoker),
        profiler,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
