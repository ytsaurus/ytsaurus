#include "driver_core.h"

#include "deferred_publication.h"
#include "writer.h"

#include <yt/yt/core/concurrency/system_invokers.h>

#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <ydb-cpp-sdk/client/topic/deferred_publications.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto DeferredPublicationRpcTimeout = TDuration::Seconds(30);

struct TFederatedTopicClientLifetimeGuard
{
    TYdbDriverCorePtr Core;
    std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> Client;

    ~TFederatedTopicClientLifetimeGuard()
    {
        // Discovery completes futures under an SDK callback lock that client destruction also acquires.
        NConcurrency::GetFinalizerInvoker()->Invoke(
            BIND([core = std::move(Core), client = std::move(Client)] () mutable {
                client.reset();
                core.Reset();
            }));
    }
};

// The SDK client depends on its driver lifetime.
class TDeferredPublicationClient
    : public IDeferredPublicationClient
{
public:
    TDeferredPublicationClient(IResourcePtr resource, std::shared_ptr<NYdb::NTopic::TDeferredPublishClient> client)
        : Resource_(std::move(resource))
        , Client_(std::move(client))
    { }

    TFuture<NYdb::NTopic::TBeginPublicationResult> BeginPublication(
        const std::string& externalPublicationId,
        const std::string& writerIdentity) override
    {
        return FromArcadiaFuture(Client_->BeginPublication(
            externalPublicationId,
            NYdb::NTopic::TBeginPublicationSettings()
                .WriterIdentity(writerIdentity)
                .ClientTimeout(DeferredPublicationRpcTimeout)));
    }

    TFuture<NYdb::NTopic::TListPublicationsResult> ListPublications() override
    {
        return FromArcadiaFuture(Client_->ListPublications(
            NYdb::NTopic::TListPublicationsSettings().ClientTimeout(DeferredPublicationRpcTimeout)));
    }

    TFuture<NYdb::NTopic::TCancelPublicationResult> CancelPublication(const NYdb::NTopic::TDeferredPublication& publication) override
    {
        return FromArcadiaFuture(Client_->CancelPublication(
            publication,
            NYdb::NTopic::TCancelPublicationSettings().ClientTimeout(DeferredPublicationRpcTimeout)));
    }

    TFuture<NYdb::NTopic::TPublishResult> Publish(const NYdb::NTopic::TDeferredPublication& publication) override
    {
        return FromArcadiaFuture(Client_->Publish(
            publication,
            NYdb::NTopic::TPublishSettings().ClientTimeout(DeferredPublicationRpcTimeout)));
    }

private:
    const IResourcePtr Resource_;
    const std::shared_ptr<NYdb::NTopic::TDeferredPublishClient> Client_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbDriverCore::TYdbDriverCore(NYdb::TDriverConfig config, bool stopBeforeDestroying)
    : Driver_(config)
    , StopBeforeDestroying_(stopBeforeDestroying)
{ }

TYdbDriverCore::~TYdbDriverCore()
{
    if (StopBeforeDestroying_) {
        Driver_.Stop(true);
    }
}

std::shared_ptr<NYdb::NTopic::TTopicClient> TYdbDriverCore::CreateTopicClient(
    const std::string& endpoint,
    const std::string& database)
{
    auto settings = NYdb::NTopic::TTopicClientSettings()
        .DiscoveryEndpoint(endpoint)
        .Database(database)
        .DefaultCompressionExecutor(NYdb::NTopic::CreateSyncExecutor())
        .DefaultHandlersExecutor(NYdb::NTopic::CreateSyncExecutor());

    return std::make_shared<NYdb::NTopic::TTopicClient>(Driver_, settings);
}

std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> TYdbDriverCore::CreateFederatedTopicClient(
    const std::string& endpoint,
    const std::string& database)
{
    auto settings = NYdb::NFederatedTopic::TFederatedTopicClientSettings()
        .DiscoveryEndpoint(endpoint)
        .Database(database)
        .DefaultCompressionExecutor(NYdb::NTopic::CreateSyncExecutor())
        .DefaultHandlersExecutor(NYdb::NTopic::CreateSyncExecutor());
    return std::make_shared<NYdb::NFederatedTopic::TFederatedTopicClient>(Driver_, settings);
}

IYdbWriterPtr TYdbDriverCore::CreateWriter(
    IResourcePtr owner,
    const std::string& endpoint,
    const std::string& database,
    NYdb::NTopic::TWriteSessionSettings settings,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler)
{
    return CreateYdbWriter(
        std::move(owner),
        CreateTopicClient(endpoint, database),
        std::move(settings),
        std::move(invoker),
        std::move(logger),
        std::move(statusProfiler));
}

IDeferredPublicationClientPtr TYdbDriverCore::CreateDeferredPublicationClient(
    IResourcePtr owner,
    const std::string& endpoint,
    const std::string& database)
{
    auto settings = NYdb::TCommonClientSettings()
        .DiscoveryEndpoint(endpoint)
        .Database(database);

    return New<TDeferredPublicationClient>(
        std::move(owner),
        std::make_shared<NYdb::NTopic::TDeferredPublishClient>(Driver_, settings));
}

TYdbWriteTargetResolution NDetail::SelectYdbWriteTarget(
    const std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>& clusters,
    const TYdbTopologyOptions& options,
    const std::optional<TYdbWriteTarget>& preferredTarget,
    const THashSet<TYdbWriteTarget, TYdbWriteTargetHash>& excludedTargets)
{
    YT_VERIFY(!options.DiscoveryEntries.empty());
    const auto& endpoint = options.DiscoveryEntries.front().Endpoint;
    const auto& database = options.Database;

    TYdbWriteTargetResolution resolution;
    resolution.Federated = options.AllowFederation &&
        (options.ExpectFederation || AnyOf(clusters, [] (const auto& cluster) {
            return !cluster.Name.empty();
        }));
    if (options.ExpectFederation && AnyOf(clusters, [] (const auto& cluster) {
            return cluster.Name.empty();
        })) {
        resolution.Error = TError("Federation discovery returned a nameless fallback for a federated installation")
            .With("endpoint", endpoint);
        return resolution;
    }
    std::vector<TYdbWriteTarget> writable;
    for (const auto& cluster : clusters) {
        if (!cluster.Name.empty() && cluster.Endpoint.empty()) {
            resolution.Error = TError("Discovered writable cluster has an empty endpoint").With("cluster", cluster.Name);
            return resolution;
        }
        if (!cluster.Name.empty() && cluster.Path != database) {
            resolution.Error = TError("Discovered cluster database differs from the configured one")
                .With("endpoint", cluster.Endpoint)
                .With("cluster_database", cluster.Path)
                .With("configured_database", database);
            return resolution;
        }
        if (cluster.IsAvailableForWrite()) {
            TYdbWriteTarget target{
                .Endpoint = cluster.Name.empty() ? endpoint : cluster.Endpoint,
                .Database = cluster.Name.empty() ? database : cluster.Path,
            };
            if (!resolution.Federated || !excludedTargets.contains(target)) {
                writable.push_back(std::move(target));
            }
        }
    }
    if (writable.empty()) {
        resolution.Error = TError("Federation discovery returned no writable cluster");
        resolution.Retryable = true;
        return resolution;
    }
    if (preferredTarget && Contains(writable, *preferredTarget)) {
        resolution.Target = *preferredTarget;
    } else {
        resolution.Target = writable[RandomNumber<size_t>(writable.size())];
    }
    return resolution;
}

TFuture<TYdbWriteTargetResolution> TYdbDriverCore::ResolveWriteTarget(
    const TYdbTopologyOptions& options,
    TDuration timeout,
    IInvokerPtr invoker,
    std::optional<TYdbWriteTarget> preferredTarget,
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets)
{
    YT_VERIFY(!options.DiscoveryEntries.empty());
    const auto& entry = options.DiscoveryEntries.front();
    auto lifetime = std::make_shared<TFederatedTopicClientLifetimeGuard>();
    lifetime->Core = MakeStrong(this);
    lifetime->Client = CreateFederatedTopicClient(entry.Endpoint, entry.Database);
    return FromArcadiaFuture(lifetime->Client->GetAllClusterInfo())
        .WithTimeout(timeout)
        .Apply(BIND([lifetime, options, preferredTarget = std::move(preferredTarget), excludedTargets = std::move(excludedTargets)] (
            const TErrorOr<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>>& result) -> TYdbWriteTargetResolution {
            Y_UNUSED(lifetime);
            TYdbWriteTargetResolution resolution;
            if (!result.IsOK()) {
                if (result.FindMatching(NYT::EErrorCode::Canceled)) {
                    result.ThrowOnError();
                }
                resolution.Error = TError("Failed to discover writable cluster").With(result);
                resolution.Retryable = true;
                return resolution;
            }
            return NDetail::SelectYdbWriteTarget(result.Value(), options, preferredTarget, excludedTargets);
        })
                .AsyncVia(std::move(invoker)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
