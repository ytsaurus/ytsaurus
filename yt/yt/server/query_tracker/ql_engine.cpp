#include "ql_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/driver/proxy_discovery_cache.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NApi;
using namespace NCodegen;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NYPath;
using namespace NYTree;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TQLQueryHandler
    : public TQueryHandlerBase
{
public:
    TQLQueryHandler(
        const IClientPtr& stateClient,
        const TYPath& stateRoot,
        const TEngineConfigBasePtr& config,
        const NRecords::TActiveQuery& activeQuery,
        const TSelectRowsOptions& options,
        const IClientPtr& queryClient,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Query_(activeQuery.Query)
        , QueryClient_(queryClient)
        , Options_(options)
    { }

    void Start() override
    {
        YT_TLOG_DEBUG("Starting QL query");
        OnQueryStarted();
        AsyncQueryResult_ = QueryClient_->SelectRows(Query_, Options_);
        AsyncQueryResult_.Subscribe(BIND(&TQLQueryHandler::OnQueryFinish, MakeWeak(this)).Via(GetCurrentInvoker()));
    }

    void Abort() override
    {
        // Nothing smarter than that for now.
        AsyncQueryResult_.Cancel(TError("Query aborted"));
    }

    void Detach() override
    {
        // Nothing smarter than that for now.
        AsyncQueryResult_.Cancel(TError("Query detached"));
    }

private:
    const std::string Query_;
    const IClientPtr QueryClient_;
    const TSelectRowsOptions Options_;

    TFuture<TSelectRowsResult> AsyncQueryResult_;

    void OnQueryFinish(const TErrorOr<TSelectRowsResult>& queryResultOrError)
    {
        if (queryResultOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }
        if (!queryResultOrError.IsOK()) {
            OnQueryFailed(queryResultOrError);
            return;
        }
        OnQueryCompleted({TRowset{.Rowset = queryResultOrError.Value().Rowset}});
    }
};

class TQLEngine
    : public IQueryEngine
{
public:
    TQLEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("QLEngineControl"))
        , ClusterDirectory_(DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection())->GetClusterDirectory())
    { }

    bool IsSafeToRestartQuery() const override
    {
        return true;
    }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        auto settings = ConvertToAttributes(activeQuery.Settings);
        auto cluster = settings->Find<std::string>("cluster").value_or(Config_->DefaultCluster);
        auto proxyRole = settings->Find<std::string>("rpc_proxy_role").value_or(DefaultRpcProxyRole);
        auto nativeConnection = ClusterDirectory_->GetConnectionOrThrow(cluster);
        auto nativeClient = nativeConnection->CreateNativeClient(NNative::TClientOptions::FromUser(activeQuery.User));
        auto nativeConfig = nativeConnection->GetConfig();

        auto proxyDiscovery = NDriver::CreateProxyDiscoveryCache(New<TAsyncExpiringCacheConfig>(), nativeClient);
        auto proxies = WaitFor(proxyDiscovery->Discover(NDriver::TProxyDiscoveryRequest{.Role = proxyRole}))
            .ValueOrThrow();
        if (proxies.Addresses.empty()) {
            THROW_ERROR_EXCEPTION("No RPC proxies found for cluster %Qv and role %Qv", cluster, proxyRole);
        }

        auto tokenAttributes = CreateEphemeralAttributes();
        tokenAttributes->Set("responsible", "query_tracker");
        tokenAttributes->Set("query_id", activeQuery.Key.QueryId);
        auto tokenOptions = TIssueTemporaryTokenOptions{
            .ExpirationTimeout = nativeConfig->DefaultSelectRowsTimeout + TDuration::Minutes(1),
        };
        auto token = WaitFor(nativeClient->IssueTemporaryToken(activeQuery.User, tokenAttributes, tokenOptions))
            .ValueOrThrow();

        auto rpcConfig = New<NRpcProxy::TConnectionConfig>();
        rpcConfig->ProxyAddresses = std::move(proxies.Addresses);
        rpcConfig->EnableProxyDiscovery = false;
        rpcConfig->DefaultSelectRowsTimeout = nativeConfig->DefaultSelectRowsTimeout;
        rpcConfig->UdfRegistryPath = nativeConfig->UdfRegistryPath;
        auto rpcConnection = NRpcProxy::CreateConnection(rpcConfig);
        auto queryClient = rpcConnection->CreateClient(TClientOptions::FromToken(std::move(token.Token)));

        auto options = GetOptions(settings.Get());
        options.InputRowLimit = options.InputRowLimit.value_or(nativeConfig->DefaultInputRowLimit);
        options.OutputRowLimit = options.OutputRowLimit.value_or(nativeConfig->DefaultOutputRowLimit);
        return New<TQLQueryHandler>(
            StateClient_,
            StateRoot_,
            Config_,
            activeQuery,
            options,
            queryClient,
            ControlQueue_->GetInvoker());
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        Config_ = DynamicPointerCast<TQLEngineConfig>(config);
    }

    std::optional<IProxyEngineProviderPtr> GetProxyEngineProvider() override
    {
        return std::nullopt;
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    TQLEngineConfigPtr Config_;
    TClusterDirectoryPtr ClusterDirectory_;

    static TSelectRowsOptions GetOptions(IAttributeDictionary* settings)
    {
        auto options = TSelectRowsOptions();

        options.InputRowLimit = settings->Find<i64>("input_row_limit");
        options.OutputRowLimit = settings->Find<i64>("output_row_limit");
        options.ExecutionPool = settings->Find<std::string>("execution_pool");
        options.ExecutionBackend = settings->Find<EExecutionBackend>("execution_backend");
        options.UseLookupCache = settings->Find<bool>("use_lookup_cache");
        options.UdfRegistryPath = settings->Find<TYPath>("udf_registry_path");
        if (auto statisticsAggregation = settings->Find<EStatisticsAggregation>("statistics_aggregation")) {
            options.StatisticsAggregation = *statisticsAggregation;
        }
        if (auto enableCodeCache = settings->Find<bool>("enable_code_cache")) {
            options.EnableCodeCache = *enableCodeCache;
        }
        if (auto useCanonicalNullRelations = settings->Find<bool>("use_canonical_null_relations")) {
            options.UseCanonicalNullRelations = *useCanonicalNullRelations;
        }
        if (auto mergeVersionedRows = settings->Find<bool>("merge_versioned_rows")) {
            options.MergeVersionedRows = *mergeVersionedRows;
        }
        if (auto useOrderByInJoinSubqueries = settings->Find<bool>("use_order_by_in_join_subqueries")) {
            options.UseOrderByInJoinSubqueries = *useOrderByInJoinSubqueries;
        }
        if (auto syntaxVersion = settings->Find<int>("syntax_version")) {
            options.SyntaxVersion = *syntaxVersion;
        }
        if (auto expressionBuilderVersion = settings->Find<int>("expression_builder_version")) {
            options.ExpressionBuilderVersion = *expressionBuilderVersion;
        }
        if (auto allowFullScan = settings->Find<bool>("allow_full_scan")) {
            options.AllowFullScan = *allowFullScan;
        }
        if (auto allowJoinWithoutIndex = settings->Find<bool>("allow_join_without_index")) {
            options.AllowJoinWithoutIndex = *allowJoinWithoutIndex;
        }
        if (auto failOnIncompleteResult = settings->Find<bool>("fail_on_incomplete_result")) {
            options.FailOnIncompleteResult = *failOnIncompleteResult;
        }

        return options;
    }
};

IQueryEnginePtr CreateQLEngine(const IClientPtr& stateClient, const TYPath& stateRoot)
{
    return New<TQLEngine>(stateClient, stateRoot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
