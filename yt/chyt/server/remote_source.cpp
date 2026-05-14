#include "remote_source.h"

#include "logging_transform.h"
#include "query_analyzer.h"
#include "query_context.h"
#include "secondary_query_header.h"
#include "format.h"

#include <Core/Settings.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Planner/PlannerContext.h>

#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ISink.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Sources/RemoteSource.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <QueryPipeline/RemoteQueryExecutor.h>

namespace DB::Setting {

////////////////////////////////////////////////////////////////////////////////

extern const SettingsBool extremes;

extern const SettingsMaxThreads max_threads;

extern const SettingsUInt64 max_network_bandwidth;
extern const SettingsUInt64 max_network_bytes;

extern const SettingsUInt64 max_concurrent_queries_for_user;
extern const SettingsUInt64 max_memory_usage_for_user;
extern const SettingsUInt64 max_result_bytes;
extern const SettingsUInt64 max_result_rows;
extern const SettingsUInt64 max_query_size;

extern const SettingsBool use_query_cache;

extern const SettingsSeconds max_execution_time;
extern const SettingsMilliseconds queue_max_wait_ms;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::Setting

namespace NYT::NClickHouseServer {

using namespace NTracing;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DB::ThrottlerPtr CreateNetThrottler(const DB::Settings& settings)
{
    DB::ThrottlerPtr throttler;
    if (settings[DB::Setting::max_network_bandwidth] || settings[DB::Setting::max_network_bytes]) {
        throttler = std::make_shared<DB::Throttler>(
            settings[DB::Setting::max_network_bandwidth],
            settings[DB::Setting::max_network_bytes],
            "Limit for bytes to send or receive over network exceeded.");
    }
    return throttler;
}

DB::Settings PrepareLeafJobSettings(const DB::Settings& settings)
{
    auto newSettings = settings;

    newSettings[DB::Setting::queue_max_wait_ms] = DB::Cluster::saturate(
        newSettings[DB::Setting::queue_max_wait_ms],
        settings[DB::Setting::max_execution_time]);

    // Does not matter on remote servers, because queries are sent under different user.
    newSettings[DB::Setting::max_concurrent_queries_for_user] = 0;
    // Same as above.
    newSettings[DB::Setting::max_memory_usage_for_user] = 0;

    // Result limits should not be processed in secondary queries
    // because its results are not final.
    // Otherwise, queries like 'insert into ...' will loose rows (CHYT-621).
    newSettings[DB::Setting::max_result_bytes] = 0;
    // Same as above.
    newSettings[DB::Setting::max_result_rows] = 0;

    // TODO(dakovalkov): Remove it after CHYT-670.
    // Disable query size limit for secondary queries manually
    // because serialized 'ytSubquery(...)' can be large.
    newSettings[DB::Setting::max_query_size] = 0;

    // All secondary queries are the same and have the same query hash, but they
    // process different data slices. Therefore, the query cache might only be used
    // for initial queries.
    newSettings[DB::Setting::use_query_cache] = false;

    return newSettings;
}

////////////////////////////////////////////////////////////////////////////////

class TRemoteSourceWrapper
    : public DB::ISource
{
public:
    TRemoteSourceWrapper(
        const DB::Block& blockHeader,
        std::shared_ptr<DB::ISource> source,
        TCallback<void(const DB::ReadProgress&)> progressCallback,
        TCallback<void()> finishCallback)
        : DB::ISource(blockHeader, /*enable_auto_progress*/ false)
        , Source_(std::move(source))
        , Input_(std::make_shared<DB::InputPort>(blockHeader))
        , ProgressCallback_(std::move(progressCallback))
        , FinishCallback_(std::move(finishCallback))
    {
        DB::connect(Source_->getPort(), *Input_);
        Input_->setNeeded();
    }

    String getName() const override
    {
        return "RemoteSourceWrapper";
    }

    Status prepare() override
    {
        auto selfStatus = DB::ISource::prepare();
        if (selfStatus != Status::Ready) {
            return selfStatus;
        }

        auto status = Source_->prepare();
        if (Input_->hasData()) {
            output.pushData(Input_->pullData());
            return Status::PortFull;
        }
        finished = (status == Status::Finished);

        return Status::Ready;
    }

    void work() override
    {
        if (!finished) {
            Source_->work();
        }

        if (auto workProgress = Source_->getReadProgress()) {
            addTotalRowsApprox(workProgress->counters.total_rows_approx);
            addTotalBytes(workProgress->counters.total_bytes);
            progress(workProgress->counters.read_rows, workProgress->counters.read_bytes);

            if (ProgressCallback_) {
                ProgressCallback_(DB::ReadProgress(
                    workProgress->counters.read_rows,
                    workProgress->counters.read_bytes,
                    workProgress->counters.total_rows_approx,
                    workProgress->counters.total_bytes
                ));
            }
        }

        if (finished && FinishCallback_) {
            FinishCallback_();
        }
    }


    void setRowsBeforeLimitCounter(DB::RowsBeforeStepCounterPtr counter) override
    {
        Source_->setRowsBeforeLimitCounter(counter);
    }

    // Stop reading from stream if output port is finished.
    void onUpdatePorts() override
    {
        if (getPort().isFinished()) {
            Source_->getPort().finish();
            Source_->onUpdatePorts();
        }
    }

    int schedule() override
    {
        return Source_->schedule();
    }

    void setStorageLimits(const std::shared_ptr<const DB::StorageLimitsList> & storageLimits) override
    {
        Source_->setStorageLimits(storageLimits);
    }

private:
    DB::SourcePtr Source_;
    std::shared_ptr<DB::InputPort> Input_;

    TCallback<void(const DB::ReadProgress&)> ProgressCallback_;
    TCallback<void()> FinishCallback_;
};

////////////////////////////////////////////////////////////////////////////////

DB::Pipe CreateRemoteSource(
    const IClusterNodePtr& remoteNode,
    const TSecondaryQuery& secondaryQuery,
    TQueryId remoteQueryId,
    DB::ContextPtr context,
    const DB::ThrottlerPtr& throttler,
    const DB::Tables& externalTables,
    DB::QueryProcessingStage::Enum processingStage,
    const DB::Block& blockHeader,
    TLogger logger,
    TSecondaryQueryReadTaskIteratorPtr taskIterator)
{
    const auto& queryAst = secondaryQuery.Query;
    const auto& Logger = logger;

    auto* queryContext = GetQueryContext(context);

    std::string query = queryAst->formatWithSecretsOneLine();

    auto scalars = context->getQueryContext()->getScalars();

    // If the current query is already secondary, then it can contain 'yt_table_*' scalars in it.
    // These scalars have already been used in ytSubquery() and we do not need them any more.
    // Erase them and then add proper ones.
    std::vector<std::string> scalarNamesToErase;
    for (const auto& [scalarName, _] : scalars) {
        if (scalarName.starts_with("yt_table_")) {
            scalarNamesToErase.push_back(scalarName);
        }
    }
    for (const auto& scalarName : scalarNamesToErase) {
        scalars.erase(scalarName);
    }

    for (const auto& [key, value] : secondaryQuery.Scalars) {
        scalars.emplace(key, value);
    }

    bool isInsert = queryAst->as<DB::ASTInsertQuery>();

    std::optional<DB::RemoteQueryExecutor::Extension> extension;
    if (taskIterator) {
        extension.emplace();
        extension->task_iterator = std::make_shared<std::function<std::string()>>([
            taskIterator = std::move(taskIterator)
        ] {
                return taskIterator->NextTask();
        });
    }

    auto remoteQueryExecutor = std::make_shared<DB::RemoteQueryExecutor>(
        remoteNode->GetConnection(),
        query,
        blockHeader,
        context,
        throttler,
        scalars,
        externalTables,
        processingStage,
        extension);
    remoteQueryExecutor->setPoolMode(DB::PoolMode::GET_MANY);

    auto* traceContext = TryGetCurrentTraceContext();
    if (!traceContext) {
        traceContext = queryContext->TraceContext.Get();
    }
    YT_VERIFY(traceContext);

    auto queryHeader = New<TSecondaryQueryHeader>();
    queryHeader->QueryId = remoteQueryId;
    queryHeader->ParentQueryId = queryContext->QueryId;
    queryHeader->SpanContext = New<TSerializableSpanContext>();
    static_cast<TSpanContext&>(*queryHeader->SpanContext) = traceContext->GetSpanContext();
    queryHeader->QueryDepth = queryContext->QueryDepth + 1;
    queryHeader->SnapshotLocks = queryContext->SnapshotLocks;
    queryHeader->DynamicTableReadTimestamp = queryContext->DynamicTableReadTimestamp;
    queryHeader->ReadTransactionId = queryContext->ReadTransactionId;
    queryHeader->WriteTransactionId = queryContext->WriteTransactionId;
    queryHeader->CreatedTablePath = queryContext->CreatedTablePath;
    queryHeader->RuntimeVariables = queryContext->ForkRuntimeVarialbes();

    auto serializedQueryHeader = ConvertToYsonString(queryHeader, EYsonFormat::Text).ToString();

    YT_LOG_INFO("Subquery header for secondary query constructed (RemoteQueryId: %v, SecondaryQueryHeader: %v)",
        remoteQueryId,
        serializedQueryHeader);
    remoteQueryExecutor->setQueryId(serializedQueryHeader);

    // XXX(max42): should we use this?
    // if (!table_func_ptr)
    //     remote_query_executor->setMainTable(main_table); */

    bool addAggregationInfo = processingStage == DB::QueryProcessingStage::WithMergeableState;
    bool addTotals = false;
    bool addExtremes = false;
    bool asyncRead = false;
    bool asyncQuerySending = false;
    if (!isInsert && processingStage == DB::QueryProcessingStage::Complete) {
        addTotals = queryAst->as<DB::ASTSelectQuery &>().group_by_with_totals;
        addExtremes = context->getSettingsRef()[DB::Setting::extremes];
    }

    auto remoteSource = std::make_shared<DB::RemoteSource>(
        remoteQueryExecutor,
        addAggregationInfo,
        asyncRead,
        asyncQuerySending);

    DB::Pipe pipe(std::make_shared<TRemoteSourceWrapper>(
        blockHeader,
        std::move(remoteSource),
        BIND(&TQueryContext::OnSecondaryProgress,
            MakeStrong(queryContext), remoteQueryId),
        BIND(&TQueryContext::OnSecondaryFinish,
            MakeStrong(queryContext), remoteQueryId)));

    if (addTotals) {
        pipe.addTotalsSource(std::make_shared<DB::RemoteTotalsSource>(remoteQueryExecutor));
    }
    if (addExtremes) {
        pipe.addExtremesSource(std::make_shared<DB::RemoteExtremesSource>(remoteQueryExecutor));
    }

    pipe.addSimpleTransform([&] (const DB::Block& header) {
        return std::make_shared<TLoggingTransform>(
            header,
            queryContext->Logger.WithTag("RemoteQueryId: %v, RemoteNode: %v",
                remoteQueryId,
                remoteNode->GetName().ToString()));
    });

    return pipe;
}

////////////////////////////////////////////////////////////////////////////////

TDistributedQueryExecutor::TDistributedQueryExecutor(
    DB::ContextPtr context,
    TQueryContext* queryContext,
    TDistributedQueryInfo distributeInfo,
    DB::SelectQueryInfo queryInfo,
    TLogger logger,
    i64 threadSubqueryCount,
    std::optional<TQueryAnalysisResult> queryAnalysisResult,
    std::vector<std::shared_ptr<IChytIndexStat>> indexStats)
    : Context_(context)
    , QueryContext_(queryContext)
    , QueryInfo_(std::move(queryInfo))
    , Logger(logger)
    , ThreadSubqueryCount_(threadSubqueryCount)
    , QueryAnalysisResult_(queryAnalysisResult)
    , DistributeInfo_(std::move(distributeInfo))
    , IndexStats_(std::move(indexStats))
{ }

void TDistributedQueryExecutor::ModifySecondaryQueries(std::function<void(DB::ASTPtr& secondaryQueryAst)> callback)
{
    for (size_t index = 0; index < DistributeInfo_.SecondaryQueries.size(); ++index) {
        auto& secondaryQuery = DistributeInfo_.SecondaryQueries[index];
        callback(secondaryQuery.Query);
        YT_LOG_TRACE(
            "Modified subquery AST (SecondaryQueryIndex: %v, AST: %v)",
            index,
            secondaryQuery.Query);
    }
}

void TDistributedQueryExecutor::Fire()
{
    QueryContext_->MoveToPhase(EQueryPhase::Execution);

    const auto& settings = Context_->getSettingsRef();

    YT_LOG_INFO("Starting distribution (NodeCount: %v, MaxThreads: %v, SubqueryCount: %v)",
        DistributeInfo_.CliqueNodes.size(),
        static_cast<ui64>(settings[DB::Setting::max_threads]),
        ThreadSubqueryCount_);

    // Wait for creation of query read transaction (if it's initialized asynchronously)
    // and save its id/timestamp before distribution to be able to read
    // locked tables on worker instances under the transaction.
    // TODO(dakovalkov): When we make the whole execution plan on a coordinator,
    // it doesn't make sense.
    QueryContext_->SaveQueryReadTransaction();

    auto newContext = DB::Context::createCopy(Context_);
    newContext->setSettings(PrepareLeafJobSettings(settings));

    // TODO(max42): do we need them?
    auto throttler = CreateNetThrottler(settings);


    YT_VERIFY(!DistributeInfo_.SecondaryQueries.empty());
    bool isInsert = DistributeInfo_.SecondaryQueries[0].Query->as<DB::ASTInsertQuery>();
    DB::Block blockHeader;
    if (!isInsert) {
        auto queryTree = QueryAnalysisResult_->QueryTree;
        blockHeader = DB::InterpreterSelectQueryAnalyzer::getSampleBlock(
            queryTree,
            Context_,
            DB::SelectQueryOptions(DistributeInfo_.ProcessingStage).analyze());
    }

    Pipes_.reserve(DistributeInfo_.SecondaryQueries.size());
    for (size_t index = 0; index < DistributeInfo_.SecondaryQueries.size(); ++index) {
        // Multiple secondary queries can be executed on the same node.
        const auto& cliqueNode = DistributeInfo_.CliqueNodes[index % DistributeInfo_.CliqueNodes.size()];
        const auto& secondaryQuery = DistributeInfo_.SecondaryQueries[index];

        YT_LOG_DEBUG(
            "Firing subquery (SubqueryIndex: %v, Node: %v)",
            index,
            cliqueNode->GetName().ToString());

        auto remoteQueryId = TQueryId::Create();

        auto pipe = CreateRemoteSource(
            cliqueNode,
            secondaryQuery,
            remoteQueryId,
            newContext,
            throttler,
            Context_->getExternalTables(),
            DistributeInfo_.ProcessingStage,
            blockHeader,
            Logger,
            DistributeInfo_.TaskIterator);

        if (!isInsert && !DB::blocksHaveEqualStructure(blockHeader, DistributeInfo_.OutputHeader)) {
            auto renameActionsDAG = DB::ActionsDAG::makeConvertingActions(
                blockHeader.getColumnsWithTypeAndName(),
                DistributeInfo_.OutputHeader.getColumnsWithTypeAndName(),
                DB::ActionsDAG::MatchColumnsMode::Position,
                true /*ignore_constant_values*/);
            auto renameExpression = std::make_shared<DB::ExpressionActions>(std::move(renameActionsDAG), DB::ExpressionActionsSettings(Context_));
            pipe.addSimpleTransform([&] (const DB::Block& header) {
                return std::make_shared<DB::ExpressionTransform>(header, renameExpression);
            });
        }

        QueryContext_->AddSecondaryQueryId(remoteQueryId);

        Pipes_.emplace_back(std::move(pipe));
    }

    if (QueryAnalysisResult_->ReadInOrderMode == EReadInOrderMode::Backward) {
        std::reverse(Pipes_.begin(), Pipes_.end());
    }
}

DB::Pipes TDistributedQueryExecutor::ExtractPipes()
{
    return std::move(Pipes_);
}

DB::Pipe TDistributedQueryExecutor::ExtractUnitedPipe()
{
    auto pipe = DB::Pipe::unitePipes(std::move(Pipes_));
    if (QueryAnalysisResult_->ReadInOrderMode != EReadInOrderMode::None && !pipe.empty() && pipe.numOutputPorts() > 1) {
        pipe.addTransform(std::make_shared<DB::ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));
    }

    return pipe;
}

DB::QueryPipelineBuilderPtr TDistributedQueryExecutor::ExtractPipeline(std::function<void()> commitCallback)
{
    // We need some sort of async signal indicating that all distributed
    // queries have finished. This may be done by introducing out own sink
    // storing callback which must be called upon all query completion.

    struct TSink
        : public DB::ISink
    {
        TSink(const TLogger& logger, const DB::Block& header, std::function<void()> commitCallback)
            : DB::ISink(header)
            , Logger(logger)
            , CommitCallback_(std::move(commitCallback))
        { }

        void consume(DB::Chunk /*chunk*/) override
        { }

        void onFinish() override
        {
            YT_LOG_DEBUG("All subqueries finished, calling commit callback");
            CommitCallback_();
            YT_LOG_DEBUG("Commit callback succeeded");
        }

        std::string getName() const override
        {
            return "CommitSink";
        }

    private:
        TLogger Logger;
        std::function<void()> CommitCallback_;
    };

    std::vector<DB::QueryPipelineBuilderPtr> pipelines;
    for (size_t index = 0; index < Pipes_.size(); ++index) {
        auto& pipe = Pipes_[index];
        auto& pipeline = pipelines.emplace_back(std::make_unique<DB::QueryPipelineBuilder>());
        pipeline->init(std::move(pipe));
    }
    auto result = std::make_unique<DB::QueryPipelineBuilder>(
        DB::QueryPipelineBuilder::unitePipelines(std::move(pipelines), {}));
    result->addTransform(std::make_shared<DB::ResizeProcessor>(DB::Block(), Pipes_.size(), 1));
    result->setSinks(
        [=, this] (const DB::Block& header, DB::QueryPipelineBuilder::StreamType) mutable -> DB::ProcessorPtr {
            return std::make_shared<TSink>(Logger, header, std::move(commitCallback));
        });

    return result;
}

std::vector<std::shared_ptr<IChytIndexStat>> TDistributedQueryExecutor::ExtractIndexStats()
{
    return std::move(IndexStats_);
}

DB::Header TDistributedQueryExecutor::GetOutputHeader() const
{
    return DistributeInfo_.OutputHeader;
}

bool TDistributedQueryExecutor::PushDownPredicate() const
{
    return QueryAnalysisResult_->AllowPushDownPredicate;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
