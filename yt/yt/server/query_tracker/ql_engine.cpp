#include "ql_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/tablet_client/public.h>

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
        bool failOnIncompleteInput,
        const IClientPtr& queryClient,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Query_(activeQuery.Query)
        , QueryClient_(queryClient)
        , Options_(options)
        , FailOnIncompleteInput_(failOnIncompleteInput)
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
    const bool FailOnIncompleteInput_;

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
        const auto& queryResult = queryResultOrError.Value();

        // SelectRows uses FailOnIncompleteResult for both input and output.
        // We may disable it to allow output truncation, so preserve input failure here.
        if (FailOnIncompleteInput_ && queryResult.Statistics.IncompleteInput) {
            auto error = TError(
                NTabletClient::EErrorCode::QueryInputRowCountLimitExceeded,
                "Query terminated prematurely due to excessive input; "
                "consider rewriting your query or changing input limit");
            if (Options_.InputRowLimit) {
                error = error.With("input_row_limit", *Options_.InputRowLimit);
            }
            OnQueryFailed(error);
            return;
        }

        OnQueryCompleted({TRowset{
            .Rowset = queryResult.Rowset,
            .IsTruncated = queryResult.Statistics.IncompleteOutput,
        }});
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

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        auto settings = ConvertToAttributes(activeQuery.Settings);
        auto cluster = settings->Find<std::string>("cluster").value_or(Config_->DefaultCluster);
        auto failOnIncompleteInput = GetFailOnIncompleteInput(settings.Get());
        auto options = GetOptions(settings.Get(), Config_->RowCountLimit);
        auto queryClient = ClusterDirectory_->GetConnectionOrThrow(cluster)->CreateClient(TClientOptions::FromUser(activeQuery.User));
        return New<TQLQueryHandler>(
            StateClient_,
            StateRoot_,
            Config_,
            activeQuery,
            options,
            failOnIncompleteInput,
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

    static bool GetFailOnIncompleteInput(IAttributeDictionary* settings)
    {
        return settings->Find<bool>("fail_on_incomplete_result")
            .value_or(TSelectRowsOptions().FailOnIncompleteResult);
    }

    static TSelectRowsOptions GetOptions(IAttributeDictionary* settings, i64 rowCountLimit)
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

        // Query Tracker row count limit should truncate the stored result
        // instead of failing the query with QueryOutputRowCountLimitExceeded.
        if (!options.OutputRowLimit || *options.OutputRowLimit > rowCountLimit) {
            options.OutputRowLimit = rowCountLimit;
            options.FailOnIncompleteResult = false;
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
