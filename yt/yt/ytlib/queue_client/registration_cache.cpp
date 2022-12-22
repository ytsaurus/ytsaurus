#include "registration_cache.h"
#include "config.h"
#include "dynamic_state.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NThreading;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TCrossClusterReference FillCrossClusterReferencesFromRichYPath(
    const TRichYPath& path,
    const std::optional<TString>& clusterName)
{
    if (!path.GetCluster() && !clusterName) {
        THROW_ERROR_EXCEPTION("Cluster name missing in path and not specified in cluster connection config")
            << TErrorAttribute("path", path);
    }

    return {
        .Cluster = path.GetCluster().value_or(*clusterName),
        .Path = path.GetPath(),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationCache::TQueueConsumerRegistrationCache(
    TQueueAgentRegistrationTableConfigPtr config,
    NNative::IConnection* connection,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger)
    : Config_(std::move(config))
    , Connection_(connection)
    , Invoker_(std::move(invoker))
    , ClusterName_(connection->GetConfig()->ClusterName)
    , RefreshExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TQueueConsumerRegistrationCache::Refresh, MakeWeak(this)),
        Config_->CacheRefreshPeriod))
    , OrchidService_(IYPathService::FromProducer(BIND(&TQueueConsumerRegistrationCache::BuildOrchid, MakeWeak(this)))->Via(Invoker_))
    , Logger(logger)
    , DynamicConfig_(Config_)
{ }

void TQueueConsumerRegistrationCache::StartSync() const
{
    YT_LOG_DEBUG("Starting queue agent registration cache sync");
    RefreshExecutor_->Start();
}

void TQueueConsumerRegistrationCache::StopSync() const
{
    YT_LOG_DEBUG("Stopping queue agent registration cache sync");
    RefreshExecutor_->Stop();
}

std::optional<TConsumerRegistrationTableRow> TQueueConsumerRegistrationCache::GetRegistration(
    const TRichYPath& queue,
    const TRichYPath& consumer) const
{
    if (!ClusterName_) {
        return {};
    }

    std::pair lookupKey{queue, consumer};
    if (!queue.GetCluster()) {
        lookupKey.first.SetCluster(*ClusterName_);
    }
    if (!consumer.GetCluster()) {
        lookupKey.second.SetCluster(*ClusterName_);
    }

    auto guard = ReaderGuard(CacheSpinLock_);

    if (auto it = Registrations_.find(lookupKey); it != Registrations_.end()) {
        return it->second;
    }

    return {};
}

void TQueueConsumerRegistrationCache::RegisterQueueConsumer(
    const TRichYPath& queue,
    const TRichYPath& consumer,
    bool vital)
{
    auto registrationTable = GetOrInitRegistrationTableOrThrow();

    WaitFor(registrationTable->Insert(std::vector{TConsumerRegistrationTableRow{
        .Queue = FillCrossClusterReferencesFromRichYPath(queue, ClusterName_),
        .Consumer = FillCrossClusterReferencesFromRichYPath(consumer, ClusterName_),
        .Vital = vital,
    }}))
        .ValueOrThrow();
}

void TQueueConsumerRegistrationCache::UnregisterQueueConsumer(
    const TRichYPath& queue,
    const TRichYPath& consumer)
{
    auto registrationTable = GetOrInitRegistrationTableOrThrow();

    WaitFor(registrationTable->Delete(std::vector{TConsumerRegistrationTableRow{
        .Queue = FillCrossClusterReferencesFromRichYPath(queue, ClusterName_),
        .Consumer = FillCrossClusterReferencesFromRichYPath(consumer, ClusterName_),
    }}))
        .ValueOrThrow();
}

void TQueueConsumerRegistrationCache::Clear()
{
    auto guard = WriterGuard(CacheSpinLock_);
    Registrations_.clear();
}

IYPathServicePtr TQueueConsumerRegistrationCache::GetOrchidService() const
{
    return OrchidService_;
}

void TQueueConsumerRegistrationCache::Refresh()
{
    try {
        GuardedRefresh();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Could not refresh queue agent registration cache");
        // Reset client just to be safe.
        auto guard = WriterGuard(ConfigurationSpinLock_);
        RegistrationTable_ = nullptr;
    }
}

void TQueueConsumerRegistrationCache::GuardedRefresh()
{
    auto config = RefreshDynamicConfig();

    YT_VERIFY(config);

    YT_LOG_DEBUG(
        "Refreshing queue agent registration cache (LocalCluster: %v, RegistrationTableCluster: %v, RegistrationTableRootPath: %Qv)",
        ClusterName_,
        config->Root.GetCluster(),
        config->Root.GetPath());

    auto registrationTable = GetOrInitRegistrationTableOrThrow();
    auto registrations = WaitFor(registrationTable->Select())
        .ValueOrThrow();

    auto guard = WriterGuard(CacheSpinLock_);

    Registrations_.clear();

    for (const auto& registration : registrations) {
        Registrations_[std::pair{TRichYPath{registration.Queue}, TRichYPath{registration.Consumer}}] = registration;
    }

    YT_LOG_DEBUG("Queue agent registration cache refreshed (RegistrationCount: %v)", Registrations_.size());
}

TConsumerRegistrationTablePtr TQueueConsumerRegistrationCache::GetOrInitRegistrationTableOrThrow()
{
    TConsumerRegistrationTablePtr newRegistrationTable;

    {
        auto guard = ReaderGuard(ConfigurationSpinLock_);

        if (RegistrationTable_) {
            return RegistrationTable_;
        }

        auto localConnection = Connection_.Lock();
        if (!localConnection) {
            THROW_ERROR_EXCEPTION("Queue agent registration cache owning connection expired");
        }

        IClientPtr client;
        auto clientOptions = TClientOptions::FromUser(DynamicConfig_->User);
        if (auto cluster = DynamicConfig_->Root.GetCluster()) {
            auto remoteConnection = localConnection->GetClusterDirectory()->GetConnectionOrThrow(*cluster);
            client = remoteConnection->CreateClient(clientOptions);
        } else {
            client = localConnection->CreateClient(clientOptions);
        }

        newRegistrationTable = New<TConsumerRegistrationTable>(DynamicConfig_->Root.GetPath(), client);
    }

    {
        auto guard = WriterGuard(ConfigurationSpinLock_);

        if (!RegistrationTable_) {
            RegistrationTable_ = std::move(newRegistrationTable);
            YT_LOG_DEBUG("Reset queue agent registration cache client");
        }

        return RegistrationTable_;
    }
}

TQueueAgentRegistrationTableConfigPtr TQueueConsumerRegistrationCache::RefreshDynamicConfig()
{
    TQueueAgentRegistrationTableConfigPtr config = Config_;

    if (ClusterName_) {
        if (auto localConnection = Connection_.Lock()) {
            if (auto remoteConnection = localConnection->GetClusterDirectory()->FindConnection(*ClusterName_)) {
                config = remoteConnection->GetConfig()->QueueAgent->RegistrationTable;
            }
        }
    }

    auto guard = WriterGuard(ConfigurationSpinLock_);

    //! NB: Should be safe to call inside the periodic executor, since it doesn't post any new callbacks while executing a callback.
    //! This just sets the internal period to be used for scheduling the next invocation.
    if (config->CacheRefreshPeriod != DynamicConfig_->CacheRefreshPeriod) {
        RefreshExecutor_->SetPeriod(config->CacheRefreshPeriod);
    }

    if (config->Root != DynamicConfig_->Root) {
        RegistrationTable_ = nullptr;
    }

    DynamicConfig_ = config;

    return config;
}

void TQueueConsumerRegistrationCache::BuildOrchid(IYsonConsumer* consumer) const
{
    TQueueAgentRegistrationTableConfigPtr config;
    {
        auto guard = ReaderGuard(ConfigurationSpinLock_);
        config = DynamicConfig_;
    }

    decltype(Registrations_) registrations;
    {
        auto guard = ReaderGuard(CacheSpinLock_);
        registrations = Registrations_;
    }

    BuildYsonFluently(consumer).BeginMap()
        .Item("effective_config").Value(config)
        .Item("registrations").DoListFor(registrations, [&] (TFluentList fluent, const auto& pair) {
            const auto& registration = pair.second;
            fluent
                .Item()
                    .BeginMap()
                        .Item("queue").Value(registration.Queue)
                        .Item("consumer").Value(registration.Consumer)
                        .Item("vital").Value(registration.Vital)
                    .EndMap();
        })
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
