#include "registration_manager.h"
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
    CacheRefreshExecutor_->Stop();
    ConfigurationRefreshExecutor_->Stop();
}

std::optional<TConsumerRegistrationTableRow> TQueueConsumerRegistrationManager::GetRegistration(
    const TRichYPath& queue,
    const TRichYPath& consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

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

    auto config = GetDynamicConfig();
    YT_VERIFY(config);

    if (config->BypassCaching) {
        RefreshCache();
    }

    auto guard = ReaderGuard(CacheSpinLock_);

    if (auto it = Registrations_.find(lookupKey); it != Registrations_.end()) {
        return it->second;
    }

    return {};
}

void TQueueConsumerRegistrationManager::RegisterQueueConsumer(
    const TRichYPath& queue,
    const TRichYPath& consumer,
    bool vital)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto registrationTableClient = CreateRegistrationTableClientOrThrow();

    WaitFor(registrationTableClient->Insert(std::vector{TConsumerRegistrationTableRow{
        .Queue = FillCrossClusterReferencesFromRichYPath(queue, ClusterName_),
        .Consumer = FillCrossClusterReferencesFromRichYPath(consumer, ClusterName_),
        .Vital = vital,
    }}))
        .ValueOrThrow();
}

void TQueueConsumerRegistrationManager::UnregisterQueueConsumer(
    const TRichYPath& queue,
    const TRichYPath& consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto registrationTableClient = CreateRegistrationTableClientOrThrow();

    WaitFor(registrationTableClient->Delete(std::vector{TConsumerRegistrationTableRow{
        .Queue = FillCrossClusterReferencesFromRichYPath(queue, ClusterName_),
        .Consumer = FillCrossClusterReferencesFromRichYPath(consumer, ClusterName_),
    }}))
        .ValueOrThrow();
}

void TQueueConsumerRegistrationManager::Clear()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(CacheSpinLock_);
    Registrations_.clear();
}

void TQueueConsumerRegistrationManager::RefreshCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    try {
        GuardedRefreshCache();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Could not refresh queue consumer registration cache");
    }
}

void TQueueConsumerRegistrationManager::GuardedRefreshCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Refreshing queue consumer registration cache");

    auto registrationTableClient = CreateRegistrationTableClientOrThrow();
    auto registrations = WaitFor(registrationTableClient->Select())
        .ValueOrThrow();

    auto guard = WriterGuard(CacheSpinLock_);

    Registrations_.clear();

    for (const auto& registration : registrations) {
        Registrations_[std::pair{TRichYPath{registration.Queue}, TRichYPath{registration.Consumer}}] = registration;
    }

    YT_LOG_DEBUG("Queue consumer registration cache refreshed (RegistrationCount: %v)", Registrations_.size());
}

void TQueueConsumerRegistrationManager::RefreshConfiguration()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    try {
        GuardedRefreshConfiguration();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Could not refresh queue consumer registration manager configuration");
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

TConsumerRegistrationTablePtr TQueueConsumerRegistrationManager::CreateRegistrationTableClientOrThrow() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto localConnection = Connection_.Lock();
    if (!localConnection) {
        THROW_ERROR_EXCEPTION("Queue consumer registration cache owning connection expired");
    }

    auto config = GetDynamicConfig();

    IClientPtr client;
    auto clientOptions = TClientOptions::FromUser(config->User);
    if (auto cluster = config->TablePath.GetCluster()) {
        auto remoteConnection = localConnection->GetClusterDirectory()->GetConnectionOrThrow(*cluster);
        client = remoteConnection->CreateClient(clientOptions);
    } else {
        client = localConnection->CreateClient(clientOptions);
    }

    return New<TConsumerRegistrationTable>(config->TablePath.GetPath(), client);
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
        .BeginMap()
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
