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

// TODO(apachee): This should be a method as the error thrown and logs are specific to the class.
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

struct TQueueConsumerRegistrationManagerProfilingCounters
{
    TCounter ListAllRegistrationsRequestCount;

    TQueueConsumerRegistrationManagerProfilingCounters(const TProfiler& profiler)
        : ListAllRegistrationsRequestCount(profiler.Counter("/list_all_registrations_request_count"))
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManagerBase::TQueueConsumerRegistrationManagerBase(
    TQueueConsumerRegistrationManagerConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> connection,
    std::optional<std::string> clusterName,
    IInvokerPtr invoker,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , Connection_(std::move(connection))
    , Invoker_(std::move(invoker))
    , ClusterName_(std::move(clusterName))
    , Logger(logger)
    , ProfilingCounters_(profiler)
    , DynamicConfig_(Config_)
{ }

void TQueueConsumerRegistrationManagerBase::Initialize()
{
    VerifyConfigImplementation(Config_);
}

void TQueueConsumerRegistrationManagerBase::StartSync()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting queue consumer registration manager sync");
}

void TQueueConsumerRegistrationManagerBase::StopSync()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Stopping queue consumer registration manager sync");
}

IQueueConsumerRegistrationManager::TGetRegistrationResult TQueueConsumerRegistrationManagerBase::GetRegistrationOrThrow(
    NYPath::TRichYPath queue,
    NYPath::TRichYPath consumer)
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

    if (auto registration = DoFindRegistration(queue, consumer); registration.has_value()) {
        result.Registration = *registration;
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

std::vector<TConsumerRegistrationTableRow> TQueueConsumerRegistrationManagerBase::ListRegistrations(
    std::optional<NYPath::TRichYPath> queue,
    std::optional<NYPath::TRichYPath> consumer)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();
    YT_VERIFY(config);

    if (config->BypassCaching) {
        RefreshCache();
    }

    // NB(apachee): This provides better diagnostics for finding bad requests.
    if (!queue && !consumer) {
        ProfilingCounters_.ListAllRegistrationsRequestCount.Increment();
        YT_LOG_DEBUG("List registrations request with both queue and consumer paths missing");

        THROW_ERROR_EXCEPTION_IF(
            config->DisableListAllRegistrations,
            "Listing all registrations is disabled by current cluster configuration "
            "and will be disabled entirely in the near future");
    }

    // NB: We want to return an empty list if the provided queue/consumer does not exist,
    // thus we ignore resolution failures.
    Resolve(config, OptionalToPointer(queue), OptionalToPointer(consumer), /*throwOnFailure*/ false);

    return DoListRegistrations(queue, consumer);
}

void TQueueConsumerRegistrationManagerBase::RegisterQueueConsumer(
    NYPath::TRichYPath queue,
    NYPath::TRichYPath consumer,
    bool vital,
    const std::optional<std::vector<int>>& partitions)
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

void TQueueConsumerRegistrationManagerBase::UnregisterQueueConsumer(
    NYPath::TRichYPath queue,
    NYPath::TRichYPath consumer)
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

TConsumerRegistrationTablePtr TQueueConsumerRegistrationManagerBase::CreateRegistrationTableWriteClientOrThrow() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto config = GetDynamicConfig();

    return CreateStateTableClientOrThrow<TConsumerRegistrationTable>(
        Connection_,
        config->StateWritePath.GetCluster(),
        config->StateWritePath.GetPath(),
        config->User);
}

void TQueueConsumerRegistrationManagerBase::Reconfigure(
    const TQueueConsumerRegistrationManagerConfigPtr& /*oldConfig*/,
    const TQueueConsumerRegistrationManagerConfigPtr& newConfig)
{
    VerifyConfigImplementation(newConfig);

    {
        auto guard = WriterGuard(ConfigurationSpinLock_);
        DynamicConfig_ = newConfig;
    }
}

void TQueueConsumerRegistrationManagerBase::VerifyConfigImplementation(const TQueueConsumerRegistrationManagerConfigPtr& config)
{
    YT_LOG_FATAL_UNLESS(
        config->Implementation == GetImplementationType(),
        "Configuration implementation and interface implementation mismatch (ConfigImpl: %v, InterfaceImpl: %v)",
        config->Implementation,
        GetImplementationType());
}

TQueueConsumerRegistrationManagerConfigPtr TQueueConsumerRegistrationManagerBase::GetDynamicConfig() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ConfigurationSpinLock_);
    // NB: Always non-null, since it is initialized from the static config in the constructor.
    return DynamicConfig_;
}

TRichYPath TQueueConsumerRegistrationManagerBase::ResolveObjectPhysicalPath(
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

void TQueueConsumerRegistrationManagerBase::Resolve(
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

void TQueueConsumerRegistrationManagerBase::ValidateClusterNameConfigured() const
{
    if (!ClusterName_) {
        THROW_ERROR_EXCEPTION("Cannot serve request, queue consumer registration manager was not properly configured with a cluster name");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
