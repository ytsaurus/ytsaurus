#include "storage_distributor.h"

#include "block_output_stream.h"
#include "config.h"
#include "format.h"
#include "helpers.h"
#include "host.h"
#include "index.h"
#include "logging_transform.h"
#include "query_analyzer.h"
#include "query_context.h"
#include "query_registry.h"
#include "conversion.h"
#include "storage_base.h"
#include "subquery.h"
#include "secondary_query_header.h"
#include "table.h"

#include <yt/yt/server/lib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/table_client/table_columnar_statistics_cache.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <Core/QueryProcessingStage.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/JoinedTables.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/NullSink.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace NChunkPools;
using namespace NChunkClient;
using namespace NTracing;
using namespace NLogging;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DB::Settings PrepareLeafJobSettings(const DB::Settings& settings)
{
    auto newSettings = settings;

    newSettings.queue_max_wait_ms = DB::Cluster::saturate(
        newSettings.queue_max_wait_ms,
        settings.max_execution_time);

    // Does not matter on remote servers, because queries are sent under different user.
    newSettings.max_concurrent_queries_for_user = 0;
    newSettings.max_memory_usage_for_user = 0;

    // Set as unchanged to avoid sending to remote server.
    newSettings.max_concurrent_queries_for_user.changed = false;
    newSettings.max_memory_usage_for_user.changed = false;

    newSettings.max_query_size = 0;

    return newSettings;
}

DB::ThrottlerPtr CreateNetThrottler(const DB::Settings& settings)
{
    DB::ThrottlerPtr throttler;
    if (settings.max_network_bandwidth || settings.max_network_bytes) {
        throttler = std::make_shared<DB::Throttler>(
            settings.max_network_bandwidth,
            settings.max_network_bytes,
            "Limit for bytes to send or receive over network exceeded.");
    }
    return throttler;
}

DB::BlockInputStreamPtr CreateLocalStream(
    const DB::ASTPtr& queryAst,
    DB::ContextPtr context,
    DB::QueryProcessingStage::Enum processedStage)
{
    DB::InterpreterSelectQuery interpreter(queryAst, context, DB::SelectQueryOptions(processedStage));
    DB::BlockInputStreamPtr stream = interpreter.execute().in;

    // Materialization is needed, since from remote servers the constants come materialized.
    // If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
    // And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
    return std::make_shared<DB::MaterializingBlockInputStream>(stream);
}

DB::Pipe CreateRemoteSource(
    const IClusterNodePtr& remoteNode,
    const DB::ASTPtr& queryAst,
    TQueryId remoteQueryId,
    DB::ContextPtr context,
    const DB::ThrottlerPtr& throttler,
    const DB::Tables& externalTables,
    DB::QueryProcessingStage::Enum processedStage,
    int storageIndex,
    TLogger logger)
{
    const auto& Logger = logger;

    const auto* queryContext = GetQueryContext(context);

    std::string query = queryToString(queryAst);

    // TODO(max42): can be done only once?
    DB::Block blockHeader;

    bool isInsert = queryAst->as<DB::ASTInsertQuery>();

    if (!isInsert) {
        blockHeader = DB::InterpreterSelectQuery(
            queryAst,
            context,
            DB::SelectQueryOptions(processedStage).analyze())
            .getSampleBlock();
    }

    auto remoteQueryExecutor = std::make_shared<DB::RemoteQueryExecutor>(
        remoteNode->GetConnection(),
        query,
        blockHeader,
        context,
        throttler,
        context->getQueryContext()->getScalars(),
        externalTables,
        processedStage);
    remoteQueryExecutor->setPoolMode(DB::PoolMode::GET_MANY);

    auto* traceContext = GetCurrentTraceContext();
    if (!traceContext) {
        traceContext = queryContext->TraceContext.Get();
    }
    YT_VERIFY(traceContext);

    auto queryHeader = New<TSecondaryQueryHeader>();
    queryHeader->QueryId = remoteQueryId;
    queryHeader->ParentQueryId = queryContext->QueryId;
    queryHeader->SpanContext = traceContext->GetSpanContext();
    queryHeader->StorageIndex = storageIndex;
    queryHeader->QueryDepth = queryContext->QueryDepth + 1;

    auto serializedQueryHeader = ConvertToYsonString(queryHeader, EYsonFormat::Text).ToString();

    YT_LOG_INFO("Subquery header for secondary query constructed (RemoteQueryId: %v, SecondaryQueryHeader: %v)",
        remoteQueryId,
        serializedQueryHeader);
    remoteQueryExecutor->setQueryId(serializedQueryHeader);

    // XXX(max42): should we use this?
    // if (!table_func_ptr)
    //     remote_query_executor->setMainTable(main_table); */

    bool addAggregationInfo = processedStage == DB::QueryProcessingStage::WithMergeableState;
    bool addTotals = false;
    bool addExtremes = false;
    bool asyncRead = false;
    if (!isInsert && processedStage == DB::QueryProcessingStage::Complete) {
        addTotals = queryAst->as<DB::ASTSelectQuery &>().group_by_with_totals;
        addExtremes = context->getSettingsRef().extremes;
    }

    auto pipe = createRemoteSourcePipe(remoteQueryExecutor, addAggregationInfo, addTotals, addExtremes, asyncRead);

    pipe.addSimpleTransform([&] (const DB::Block & header) {
        return std::make_shared<TLoggingTransform>(
            header,
            queryContext->Logger.WithTag("RemoteQueryId: %v, RemoteNode: %v",
                remoteQueryId,
                remoteNode->GetName().ToString()));
    });

    return pipe;
}

void ValidateReadPermissions(
    const std::vector<TString>& columnNames,
    const std::vector<TTablePtr>& tables,
    TQueryContext* queryContext)
{
    std::vector<TRichYPath> tablePathsWithColumns;
    tablePathsWithColumns.reserve(tables.size());
    for (const auto& table : tables) {
        auto tablePath = TRichYPath(table->GetPath());
        tablePath.SetColumns(columnNames);
        tablePathsWithColumns.emplace_back(std::move(tablePath));
    }
    queryContext->Host->ValidateReadPermissions(tablePathsWithColumns, queryContext->User);
}

TClusterNodes GetNodesToDistribute(TQueryContext* queryContext, size_t distributionSeed, bool isDistributedJoin)
{
    // Should we distribute query or process it on local node only?
    bool distribute = true;
    // How many nodes we should use to distribute query.
    // By default, we distribute to all available cluster nodes, but behavior can be overriden via settings.
    i64 nodesToChoose = queryContext->GetClusterNodesSnapshot().size();

    if (nodesToChoose == 0) {
        THROW_ERROR_EXCEPTION("There are no instances available through discovery");
    }

    auto settings = queryContext->Settings->Execution;

    if (isDistributedJoin) {
        if (settings->JoinNodeLimit > 0) {
            nodesToChoose = std::min(nodesToChoose, settings->JoinNodeLimit);
        }
    } else {
        if (settings->SelectNodeLimit > 0) {
            nodesToChoose = std::min(nodesToChoose, settings->SelectNodeLimit);
        }
        switch (settings->SelectPolicy) {
            case ESelectPolicy::Local:
                distribute = false;
                break;
            case ESelectPolicy::DistributeInitial:
                distribute = (queryContext->QueryKind == EQueryKind::InitialQuery);
                break;
            case ESelectPolicy::Distribute:
                distribute = true;
                break;
        }
    }

    // Process on local node only, do not need to choose anything.
    if (!distribute) {
        return {queryContext->Host->GetLocalNode(),};
    }

    auto candidates = queryContext->GetClusterNodesSnapshot();

    auto candidateComporator = [distributionSeed] (const IClusterNodePtr& lhs, const IClusterNodePtr& rhs) {
        auto lhash = CombineHashes(distributionSeed, THash<int>()(lhs->GetCookie()));
        auto rhash = CombineHashes(distributionSeed, THash<int>()(rhs->GetCookie()));
        return lhash < rhash;
    };
    // NB: this is important to distribute query deterministically across the cluster.
    std::sort(candidates.begin(), candidates.end(), candidateComporator);

    YT_VERIFY(nodesToChoose > 0);
    YT_VERIFY(nodesToChoose <= std::ssize(candidates));

    candidates.resize(nodesToChoose);

    return candidates;
}

////////////////////////////////////////////////////////////////////////////////

//! This class is extracted for better encapsulation of distributed query call context.
//! Recall that TStorageDistributor may be reused for several subqueries.
class TDistributedQueryPreparer
{
public:
    TDistributedQueryPreparer(
        const std::vector<TString>& realColumnNames,
        const std::vector<TString>& virtualColumnNames,
        DB::SelectQueryInfo& queryInfo,
        DB::ContextPtr context,
        TQueryContext* queryContext,
        TStorageContext* storageContext,
        DB::QueryProcessingStage::Enum processedStage,
        size_t distributionSeed)
        : RealColumnNames_(realColumnNames)
        , VirtualColumnNames_(virtualColumnNames)
        , QueryInfo_(queryInfo)
        , Context_(DB::Context::createCopy(context))
        , QueryContext_(queryContext)
        , StorageContext_(storageContext)
        , ProcessedStage_(processedStage)
        , DistributionSeed_(distributionSeed)
        , Logger(StorageContext_->Logger)
    { }

    void PrepareSecondaryQueries()
    {
        YT_LOG_DEBUG("Preparing distribution (QueryAST: %v)", *QueryInfo_.query, static_cast<void*>(this));

        QueryContext_->MoveToPhase(EQueryPhase::Preparation);

        NTracing::TChildTraceContextGuard guard("ClickHouseYt.Prepare");

        const auto& executionSettings = StorageContext_->Settings->Execution;

        if (executionSettings->QueryDepthLimit > 0 && QueryContext_->QueryDepth >= executionSettings->QueryDepthLimit) {
            THROW_ERROR_EXCEPTION("Query depth limit exceeded; consider optimizing query or changing the limit")
                << TErrorAttribute("query_depth_limit", executionSettings->QueryDepthLimit);
        }

        if (StorageContext_->Settings->Testing->ThrowExceptionInDistributor) {
            THROW_ERROR_EXCEPTION("Testing exception in distributor")
                << TErrorAttribute("storage_index", StorageContext_->Index);
        }

        if (ProcessedStage_ == DB::QueryProcessingStage::FetchColumns) {
            // See getQueryProcessingStage for more details about FetchColumns stage.
            RemoveJoinFromQuery();
        }

        SelectQueryIndex_ = QueryContext_->SelectQueries.size();
        QueryContext_->SelectQueries.push_back(TString(queryToString(QueryInfo_.query)));

        PrepareInput();

        ChooseNodesToDistribute();

        PrepareThreadSubqueries(CliqueNodes_.size());

        PrepareSecondaryQueryAsts();

        YT_LOG_INFO("Query distribution prepared");
    }

    void ModifySecondaryQueries(std::function<void(DB::ASTPtr& secondaryQueryAst)> callback)
    {
        for (size_t index = 0; index < SecondaryQueryAsts_.size(); ++index) {
            auto& secondaryQueryAst = SecondaryQueryAsts_[index];
            callback(secondaryQueryAst);
            YT_LOG_TRACE(
                "Modified subquery AST (SecondaryQueryIndex: %v, AST: %v)",
                index,
                secondaryQueryAst);
        }
    }

    void Fire()
    {
        QueryContext_->MoveToPhase(EQueryPhase::Execution);

        const auto& settings = Context_->getSettingsRef();

        YT_LOG_INFO("Starting distribution (RealColumnNames_: %v, NodeCount: %v, MaxThreads: %v, SubqueryCount: %v)",
            RealColumnNames_,
            CliqueNodes_.size(),
            static_cast<ui64>(settings.max_threads),
            ThreadSubqueries_.size());

        auto newContext = DB::Context::createCopy(Context_);
        newContext->setSettings(PrepareLeafJobSettings(settings));

        // TODO(max42): do we need them?
        auto throttler = CreateNetThrottler(settings);

        for (size_t index = 0; index < SecondaryQueryAsts_.size(); ++index) {
            const auto& cliqueNode = CliqueNodes_[index];
            const auto& subqueryAst = SecondaryQueryAsts_[index];

            YT_LOG_DEBUG(
                "Firing subquery (SubqueryIndex: %v, Node: %v)",
                index,
                cliqueNode->GetName().ToString());

            auto remoteQueryId = TQueryId::Create();

            auto pipe = CreateRemoteSource(
                cliqueNode,
                subqueryAst,
                remoteQueryId,
                newContext,
                throttler,
                Context_->getExternalTables(),
                ProcessedStage_,
                SelectQueryIndex_,
                Logger);

            QueryContext_->SecondaryQueryIds.push_back(ToString(remoteQueryId));

            Pipes_.emplace_back(std::move(pipe));
        }
    }

    DB::Pipes ExtractPipes()
    {
        return std::move(Pipes_);
    }

    DB::QueryPipelinePtr ExtractPipeline(std::function<void()> commitCallback)
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

            void consume(DB::Chunk /* chunk */) override
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

        std::vector<DB::QueryPipelinePtr> pipelines;
        for (size_t index = 0; index < Pipes_.size(); ++index) {
            auto& pipe = Pipes_[index];
            auto& pipeline = pipelines.emplace_back(std::make_unique<DB::QueryPipeline>());
            pipeline->init(std::move(pipe));
        }
        auto result = std::make_unique<DB::QueryPipeline>(
            DB::QueryPipeline::unitePipelines(std::move(pipelines), {}));
        result->addTransform(std::make_shared<DB::ResizeProcessor>(DB::Block(), Pipes_.size(), 1));
        result->setSinks(
            [=] (const DB::Block & header, DB::QueryPipeline::StreamType) mutable -> DB::ProcessorPtr {
                return std::make_shared<TSink>(Logger, header, std::move(commitCallback));
            });

        return result;
    }

private:
    std::vector<TString> RealColumnNames_;
    std::vector<TString> VirtualColumnNames_;
    DB::SelectQueryInfo QueryInfo_;
    DB::ContextPtr Context_;
    TQueryContext* const QueryContext_;
    TStorageContext* const StorageContext_;
    const DB::QueryProcessingStage::Enum ProcessedStage_;
    size_t DistributionSeed_;
    const TLogger Logger;

    TSubquerySpec SpecTemplate_;
    std::vector<TSubquery> ThreadSubqueries_;
    std::optional<TQueryAnalyzer> QueryAnalyzer_;
    std::optional<TQueryAnalysisResult> QueryAnalysisResult_;
    // TODO(max42): YT-11778.
    // TMiscExt is used for better memory estimation in readers, but it is dropped when using
    // TInputChunk, so for now we store it explicitly in a map and use when serializing subquery input.
    THashMap<TChunkId, TRefCountedMiscExtPtr> MiscExtMap_;
    NChunkPools::TChunkStripeListPtr InputStripeList_;
    std::optional<double> SamplingRate_;

    int SelectQueryIndex_ = -1;
    TClusterNodes CliqueNodes_;
    std::vector<DB::ASTPtr> SecondaryQueryAsts_;
    DB::Pipes Pipes_;

    void RemoveJoinFromQuery()
    {
        if (!hasJoin(QueryInfo_.query->as<DB::ASTSelectQuery&>())) {
            return;
        }

        QueryInfo_.query = QueryInfo_.query->clone();
        auto& select = QueryInfo_.query->as<DB::ASTSelectQuery&>();

        DB::TreeRewriterResult newRewriterResult = *QueryInfo_.syntax_analyzer_result;
        YT_VERIFY(removeJoin(select, newRewriterResult, Context_));

        QueryInfo_.syntax_analyzer_result = std::make_shared<DB::TreeRewriterResult>(std::move(newRewriterResult));
    }

    void PrepareInput()
    {
        SpecTemplate_ = TSubquerySpec();
        SpecTemplate_.InitialQuery = SerializeAndMaybeTruncateSubquery(*QueryInfo_.query);
        SpecTemplate_.QuerySettings = StorageContext_->Settings;

        QueryAnalyzer_.emplace(Context_, StorageContext_, QueryInfo_, Logger);
        QueryAnalysisResult_.emplace(QueryAnalyzer_->Analyze());

        auto input = FetchInput(
            StorageContext_,
            *QueryAnalysisResult_,
            RealColumnNames_,
            VirtualColumnNames_,
            TClickHouseIndexBuilder(&QueryInfo_, Context_));

        YT_VERIFY(!SpecTemplate_.DataSourceDirectory);
        SpecTemplate_.DataSourceDirectory = std::move(input.DataSourceDirectory);

        MiscExtMap_ = std::move(input.MiscExtMap);
        InputStripeList_ = std::move(input.StripeList);

        const auto& selectQuery = QueryInfo_.query->as<DB::ASTSelectQuery&>();
        if (auto selectSampleSize = selectQuery.sampleSize()) {
            auto ratio = selectSampleSize->as<DB::ASTSampleRatio&>().ratio;
            auto rate = static_cast<double>(ratio.numerator) / ratio.denominator;
            if (rate > 1.0) {
                rate /= InputStripeList_->TotalRowCount;
            }
            rate = std::clamp(rate, 0.0, 1.0);
            SamplingRate_ = rate;
        }

        bool canUseBlockSampling = StorageContext_->Settings->UseBlockSampling;
        for (const auto& tables : QueryAnalysisResult_->Tables) {
            for (const auto& table : tables) {
                if (table->Dynamic) {
                    canUseBlockSampling = false;
                }
            }
        }
        if (QueryAnalysisResult_->PoolKind != EPoolKind::Unordered) {
            canUseBlockSampling = false;
        }

        auto tableReaderConfig = New<TTableReaderConfig>();
        if (canUseBlockSampling && SamplingRate_) {
            YT_LOG_DEBUG("Using block sampling (SamplingRate: %v)",
                SamplingRate_);
            for (const auto& stripe : InputStripeList_->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                        chunkSlice->ApplySamplingSelectivityFactor(*SamplingRate_);
                    }
                }
            }
            tableReaderConfig->SamplingRate = SamplingRate_;
            tableReaderConfig->SamplingMode = ESamplingMode::Block;
            if (QueryInfo_.prewhere_info) {
                // When PREWHERE is present, same chunk is processed several times
                // (first on PREWHERE phase, then on main phase),
                // so we fix seed for sampling for the sake of determinism.
                tableReaderConfig->SamplingSeed = RandomNumber<ui64>();
            }
            SamplingRate_ = std::nullopt;
        }
        SpecTemplate_.TableReaderConfig = tableReaderConfig;
    }

    void ChooseNodesToDistribute()
    {
        const auto& settings = StorageContext_->Settings->Execution;
        const auto& select = QueryInfo_.query->as<DB::ASTSelectQuery&>();
        bool isDistributedJoin = hasJoin(select);

        auto nodes = GetNodesToDistribute(QueryContext_, DistributionSeed_, isDistributedJoin);

        // This limit can only be applied after fetching chunk specs.
        // That's why we do it here and not in GetNodesToDistribute.
        if (settings->MinDataWeightPerSecondaryQuery > 0) {
            i64 nodeLimit = DivCeil(InputStripeList_->TotalDataWeight, settings->MinDataWeightPerSecondaryQuery);
            nodeLimit = std::clamp<i64>(1, nodes.size(), nodeLimit);
            nodes.resize(nodeLimit);
        }

        CliqueNodes_ = std::move(nodes);

        YT_LOG_DEBUG("Distribution nodes chosen (NodeCount: %v)", CliqueNodes_.size());

        for (const auto& cliqueNode : CliqueNodes_) {
            YT_LOG_DEBUG("Clique node (Host: %v, Port: %v, IsLocal: %v)",
                cliqueNode->GetName().Host,
                cliqueNode->GetName().Port,
                cliqueNode->IsLocal());
        }
    }

    void PrepareThreadSubqueries(int secondaryQueryCount)
    {
        NTracing::GetCurrentTraceContext()->AddTag("chyt.secondary_query_count", secondaryQueryCount);
        NTracing::GetCurrentTraceContext()->AddTag(
            "chyt.real_column_names",
            Format("%v", MakeFormattableView(RealColumnNames_, TDefaultFormatter())));
        NTracing::GetCurrentTraceContext()->AddTag(
            "chyt.virtual_column_names",
            Format("%v", MakeFormattableView(VirtualColumnNames_, TDefaultFormatter())));

        YT_LOG_TRACE("Preparing StorageDistributor for query (Query: %v)", *QueryInfo_.query);

        i64 inputStreamsPerSecondaryQuery = QueryContext_->Settings->Execution->InputStreamsPerSecondaryQuery;
        if (inputStreamsPerSecondaryQuery <= 0) {
            inputStreamsPerSecondaryQuery = Context_->getSettings().max_threads;
        }
        NTracing::GetCurrentTraceContext()->AddTag(
            "chyt.input_streams_per_secondary_query",
            inputStreamsPerSecondaryQuery);

        ThreadSubqueries_ = BuildThreadSubqueries(
            std::move(InputStripeList_),
            QueryAnalysisResult_->KeyColumnCount,
            QueryAnalysisResult_->PoolKind,
            SpecTemplate_.DataSourceDirectory,
            std::max<int>(1, secondaryQueryCount * inputStreamsPerSecondaryQuery),
            SamplingRate_,
            StorageContext_,
            QueryContext_->Host->GetConfig()->Subquery);

        // NB: this is important for queries to distribute deterministically across the cluster.
        std::sort(ThreadSubqueries_.begin(), ThreadSubqueries_.end(), [] (const TSubquery& lhs, const TSubquery& rhs) {
            return lhs.Cookie < rhs.Cookie;
        });

        size_t totalInputDataWeight = 0;
        size_t totalChunkCount = 0;

        for (const auto& subquery : ThreadSubqueries_) {
            totalInputDataWeight += subquery.StripeList->TotalDataWeight;
            totalChunkCount += subquery.StripeList->TotalChunkCount;
        }

        i64 maxDataWeightPerSubquery = QueryContext_->Host->GetConfig()->Subquery->MaxDataWeightPerSubquery;
        if (maxDataWeightPerSubquery > 0) {
            for (const auto& subquery : ThreadSubqueries_) {
                if (subquery.StripeList->TotalDataWeight > maxDataWeightPerSubquery) {
                    THROW_ERROR_EXCEPTION(
                        NClickHouseServer::EErrorCode::SubqueryDataWeightLimitExceeded,
                        "Subquery exceeds data weight limit: %v > %v",
                        subquery.StripeList->TotalDataWeight,
                        maxDataWeightPerSubquery)
                        << TErrorAttribute("total_input_data_weight", totalInputDataWeight);
                }
            }
        }

        NTracing::GetCurrentTraceContext()->AddTag("chyt.total_input_data_weight", totalInputDataWeight);
        NTracing::GetCurrentTraceContext()->AddTag("chyt.total_chunk_count", totalChunkCount);
    }

    void PrepareSecondaryQueryAsts()
    {
        int secondaryQueryCount = std::min(ThreadSubqueries_.size(), CliqueNodes_.size());

        if (secondaryQueryCount == 0) {
            // NB: if we make no secondary queries, there will be a tricky issue around schemas.
            // Namely, we return an empty vector of streams, so the resulting schema will
            // be taken from columns of this storage (which are set via setColumns).
            // Such schema will be incorrect as it will lack aggregates in mergeable state
            // which should normally return from our distributed storage.
            // In order to overcome this, we forcefully make at least one stream, even though it
            // will return empty result for sure.
            secondaryQueryCount = 1;
        }

        for (int index = 0; index < secondaryQueryCount; ++index) {
            int firstSubqueryIndex = index * ThreadSubqueries_.size() / secondaryQueryCount;
            int lastSubqueryIndex = (index + 1) * ThreadSubqueries_.size() / secondaryQueryCount;

            auto threadSubqueries = MakeRange(ThreadSubqueries_.data() + firstSubqueryIndex, ThreadSubqueries_.data() + lastSubqueryIndex);

            YT_LOG_DEBUG("Preparing secondary query (QueryIndex: %v, SecondaryQueryCount: %v)",
                index,
                secondaryQueryCount);
            for (const auto& threadSubquery : threadSubqueries) {
                YT_LOG_DEBUG("Thread subquery (Cookie: %v, LowerBound: %v, UpperBound: %v, DataWeight: %v, RowCount: %v, ChunkCount: %v)",
                    threadSubquery.Cookie,
                    threadSubquery.Bounds.first,
                    threadSubquery.Bounds.second,
                    threadSubquery.StripeList->TotalDataWeight,
                    threadSubquery.StripeList->TotalRowCount,
                    threadSubquery.StripeList->TotalChunkCount);
            }

            YT_VERIFY(!threadSubqueries.Empty() || ThreadSubqueries_.empty());

            auto secondaryQueryAst = QueryAnalyzer_->RewriteQuery(
                threadSubqueries,
                SpecTemplate_,
                MiscExtMap_,
                index,
                index + 1 == secondaryQueryCount /*isLastSubquery*/);

            YT_LOG_DEBUG(
                "Secondary query prepared (ThreadSubqueryCount: %v, QueryIndex: %v, SecondaryQueryCount: %v)",
                lastSubqueryIndex - firstSubqueryIndex,
                index,
                secondaryQueryCount);

            YT_LOG_TRACE(
                "Secondary query AST (SubqueryIndex: %v, AST: %v)",
                index,
                secondaryQueryAst);

            SecondaryQueryAsts_.emplace_back(std::move(secondaryQueryAst));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageDistributor
    : public TYtStorageBase
    , public IStorageDistributor
{
public:
    friend class TQueryAnalyzer;

    TStorageDistributor(
        DB::ContextPtr context,
        std::vector<TTablePtr> tables,
        TTableSchemaPtr schema)
        : TYtStorageBase({"YT", "distributor"})
        , QueryContext_(GetQueryContext(context))
        , Tables_(std::move(tables))
        , Schema_(std::move(schema))
        , Logger(QueryContext_->Logger)
    {
        DistributionSeed_ = QueryContext_->Settings->Execution->DistributionSeed;
        for (const auto& table : Tables_) {
            DistributionSeed_ = CombineHashes(DistributionSeed_, THash<TString>()(table->Path.GetPath()));
        }
        YT_LOG_DEBUG("Distribution seed generated (DistributionSeed: %v)", DistributionSeed_);

        // TODO(dakovalkov): https://st.yandex-team.ru/CHYT-526
        if (Tables_.size() > 1) {
            for (const auto& table : Tables_) {
                if (table->Dynamic) {
                    THROW_ERROR_EXCEPTION("Reading multiple dynamic tables or dynamic table together with static table is not supported (CHYT-526)");
                }
            }
        }
    }

    void startup() override
    {
        TCurrentTraceContextGuard guard(QueryContext_->TraceContext);

        YT_LOG_TRACE("StorageDistributor instantiated (Address: %v)", static_cast<void*>(this));
        if (Schema_->GetColumnCount() == 0) {
            THROW_ERROR_EXCEPTION("CHYT does not support tables without schema")
                << TErrorAttribute("path", getTableName());
        }
        DB::StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(DB::ColumnsDescription(ToNamesAndTypesList(*Schema_, QueryContext_->Settings->Composite)));
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override
    {
        return "StorageDistributor";
    }

    bool supportsPrewhere() const override
    {
        return true;
    }

    bool isRemote() const override
    {
        return true;
    }

    bool supportsIndexForIn() const override
    {
        return Schema_->IsSorted();
    }

    bool mayBenefitFromIndexForIn(const DB::ASTPtr& /* queryAst */, DB::ContextPtr /* context */, const DB::StorageMetadataPtr& /* metadata_snapshot */) const override
    {
        return supportsIndexForIn();
    }

    virtual std::string getTableName() const
    {
        std::string result = "";
        for (size_t index = 0; index < Tables_.size(); ++index) {
            if (index > 0) {
                result += ", ";
            }
            result += std::string(Tables_[index]->Path.GetPath().data());
        }
        return result;
    }

    /*
     * There are different stages in query processing.
     * All table engines should at least read columns, but in some cases they can process query up to higher stages.
     *
     * Read columns -> Join table -> Apply Prewhere/Where conditions -> Aggregate -> Apply Having/OrderBy/Limit
     *
     * Query stages in CH:
     *
     * FetchColumns - table engine only needs to read columns.
     * It can use where/prewhere conditions to filter data, but it's not nessesary.
     * Join/Distinct/GroupBy/Limit/OrderBy will be processed by CH itself on the initiator.
     *
     * WithMergableState - table engine should process Join and do aggregation locally (distinct/group by/limit by).
     * It returns "mergable state", which allows to aggregate data from several streams.
     * ClickHouse will complete aggregation and apply Having/Limit/OrderBy clauses on the initiator.
     *
     * WithMergeableStateAfterAggregation - Some legacy stage, do not use it.
     *
     * WithMergeableStateAfterAggregationAndLimit - Everything is done on remote servers.
     * But coordinator may need to apply Limit clause again and merge several sorted stream.
     *
     * Complete - everything is processed on instances locally. Coordinator works as a proxy.
     */
    //! Calculate the maximum possible queryStage, the engine can proccess to.
    DB::QueryProcessingStage::Enum getQueryProcessingStage(
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum toStage,
        const DB::StorageMetadataPtr& /*inMemoryMetadata*/,
        DB::SelectQueryInfo& queryInfo) const override
    {
        auto* queryContext = GetQueryContext(context);
        auto* storageContext = queryContext->GetOrRegisterStorageContext(this, context);
        const auto& executionSettings = storageContext->Settings->Execution;
        const auto& chSettings = context->getSettingsRef();
        const auto& select = queryInfo.query->as<DB::ASTSelectQuery&>();

        // I cannot imagine why CH can ask us to process query to some intermediate stage.
        // It makes sense for its native Distributed engine, because underlying table can
        // also be distributed, but it's not our case.
        if (toStage != DB::QueryProcessingStage::Complete) {
            THROW_ERROR_EXCEPTION(
                "Unexpected query processing stage: %v; "
                "it's a bug; please, fill a ticket in CHYT queue",
                toString(toStage));
        }

        bool isDistributedJoin = DB::hasJoin(select);

        if (isDistributedJoin) {
            bool distributeJoin = true;

            switch (executionSettings->JoinPolicy) {
                case EJoinPolicy::Local:
                    distributeJoin = false;
                    break;
                case EJoinPolicy::DistributeInitial:
                    distributeJoin = (queryContext->QueryKind == EQueryKind::InitialQuery);
                    break;
                case EJoinPolicy::Distribute:
                    distributeJoin = true;
                    break;
            }

            if (!distributeJoin) {
                // We will only do first query stage (reading columns).
                // Join/Group by and so on will be mady by ClickHouse itself.
                return DB::QueryProcessingStage::FetchColumns;
            }
        }

        // Handle some CH-native options.
        // We do not really need them, but it's not difficult to mimic the original behaviour.
        if (chSettings.distributed_group_by_no_merge) {
            // DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION = 2
            if (chSettings.distributed_group_by_no_merge == 2) {
                if (chSettings.distributed_push_down_limit) {
                    return DB::QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
                } else {
                    return DB::QueryProcessingStage::WithMergeableStateAfterAggregation;
                }
            } else {
                return DB::QueryProcessingStage::Complete;
            }
        }

        auto nodes = GetNodesToDistribute(queryContext, DistributionSeed_, isDistributedJoin);
        // If there is only one node, then its result is final, since
        // we do not need to merge aggregation states from different streams.
        if (nodes.size() == 1) {
            return DB::QueryProcessingStage::Complete;
        }

        // Try to process query up to advanced stages.
        if (executionSettings->OptimizeQueryProcessingStage) {
            TQueryAnalyzer analyzer(context, storageContext, queryInfo, Logger);
            return analyzer.GetOptimizedQueryProcessingStage();
        }

        // Default stage. It's always possible to process up to this stage.
        return DB::QueryProcessingStage::WithMergeableState;
    }

    DB::Pipe read(
        const DB::Names& columnNames,
        const DB::StorageMetadataPtr& metadataSnapshot,
        DB::SelectQueryInfo& queryInfo,
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum processedStage,
        size_t /*maxBlockSize*/,
        unsigned /*numStreams*/) override
    {
        TCurrentTraceContextGuard guard(QueryContext_->TraceContext);

        auto preparer = BuildPreparer(
            columnNames,
            metadataSnapshot,
            queryInfo,
            context,
            processedStage);
        preparer.Fire();
        auto pipes = preparer.ExtractPipes();
        return DB::Pipe::unitePipes(std::move(pipes));
    }

    bool supportsSampling() const override
    {
        return true;
    }

    DB::BlockOutputStreamPtr write(
        const DB::ASTPtr& /*ptr*/,
        const DB::StorageMetadataPtr& /*metadata_snapshot*/,
        DB::ContextPtr /*context*/) override
    {
        TCurrentTraceContextGuard guard(QueryContext_->TraceContext);

        if (Tables_.size() != 1) {
            THROW_ERROR_EXCEPTION("Cannot write to many tables simultaneously")
                << TErrorAttribute("paths", getTableName());
        }
        const auto& table = Tables_.front();
        auto path = table->Path;

        if (table->Dynamic && !table->Path.GetAppend(/*defaultValue*/ true)) {
            THROW_ERROR_EXCEPTION("Overriding dynamic tables is not supported");
        }

        auto dataTypes = ToDataTypes(*table->Schema, QueryContext_->Settings->Composite, /*enableReadOnlyConversions*/ false);
        YT_LOG_DEBUG(
            "Inferred ClickHouse data types from YT schema (Schema: %v, DataTypes: %v)",
            table->Schema,
            dataTypes);

        if (table->Dynamic) {
            return CreateDynamicTableBlockOutputStream(
                path,
                table->Schema,
                dataTypes,
                QueryContext_->Settings->DynamicTable,
                QueryContext_->Settings->Composite,
                QueryContext_->Client(),
                QueryContext_->Logger);
        } else {
            // Set append if it is not set.
            path.SetAppend(path.GetAppend(true /*defaultValue*/));
            return CreateStaticTableBlockOutputStream(
                path,
                table->Schema,
                dataTypes,
                QueryContext_->Host->GetConfig()->TableWriter,
                QueryContext_->Settings->Composite,
                QueryContext_->Client(),
                QueryContext_->Logger);
        }
    }

    DB::QueryPipelinePtr distributedWrite(const DB::ASTInsertQuery& query, DB::ContextPtr context) override
    {
        TCurrentTraceContextGuard guard(QueryContext_->TraceContext);

        // First, validate if SELECT part is suitable for distributed INSERT SELECT.

        auto queryContext = GetQueryContext(context);
        auto* storageContext = queryContext->GetOrRegisterStorageContext(this, context);
        const auto& executionSettings = storageContext->Settings->Execution;

        if (queryContext->QueryKind == EQueryKind::SecondaryQuery) {
            // The query was already distributed.
            // Forbid insert distribution again to avoid lots of requests to the master.
            return nullptr;
        }

        if (Tables_.size() != 1) {
            // We are a concatenation; it is impossible to INSERT at all,
            // but let regular write procedure produce proper error.
            return nullptr;
        }

        const auto& table = Tables_.back();

        auto* selectWithUnion = query.select->as<DB::ASTSelectWithUnionQuery>();
        if (selectWithUnion->list_of_selects->children.size() != 1) {
            // There is non-trivial union in SELECT part, fall back to non-distributed INSERT SELECT.
            return nullptr;
        }

        auto* select = selectWithUnion->list_of_selects->children[0]->as<DB::ASTSelectQuery>();
        YT_VERIFY(select);

        DB::JoinedTables joinedTables(DB::Context::createCopy(context), *select);
        auto sourceStorage = std::dynamic_pointer_cast<TStorageDistributor>(joinedTables.getLeftTableStorage());
        if (!sourceStorage) {
            // Source storage is not a distributor; no distributed INSERT for today, sorry.
            return nullptr;
        }

        bool overwrite = !table->Path.GetAppend(/*defaultValue*/ true);

        if (table->Dynamic && overwrite) {
            // Overwriting dyntables is not supported, let regular write procedure produce proper error.
            return nullptr;
        }

        auto distributedStage = executionSettings->DistributedInsertStage;

        if (distributedStage == EDistributedInsertStage::None) {
            return nullptr;
        }

        // Then, prepare distributed query; we need to interpret SELECT part in order to obtain some additional information
        // for preparer (like required columns or select query info).

        DB::InterpreterSelectQuery selectInterpreter(select->clone(), DB::Context::createCopy(context), DB::SelectQueryOptions());
        selectInterpreter.execute();
        auto selectQueryInfo = selectInterpreter.getQueryInfo();

        auto queryProcessingStage = getQueryProcessingStage(
            context,
            DB::QueryProcessingStage::Complete,
            /*inMemoryMetadata*/ nullptr,
            selectQueryInfo);

        int distributedInsertStageRank = GetDistributedInsertStageRank(distributedStage);
        int queryProcessingStageRank = GetQueryProcessingStageRank(queryProcessingStage);

        if (queryProcessingStageRank < distributedInsertStageRank) {
            return nullptr;
        }

        auto preparer = sourceStorage->BuildPreparer(
            selectInterpreter.getRequiredColumns(),
            /*metadataSnapshot*/ nullptr,
            selectQueryInfo,
            DB::Context::createCopy(context),
            DB::QueryProcessingStage::Complete);

        if (overwrite) {
            // Trying to override destination table in straightforward way would result in lock conflict.
            // In order to fix that we clear table by ourselves and drop that append = %false flag.
            table->Path.SetAppend(true);
            EraseTable(context);
        }

        // Prepend each SELECT query with proper INSERT INTO ...

        preparer.ModifySecondaryQueries([&] (DB::ASTPtr& secondaryQueryAst) {
            auto queryClone = query.clone();
            queryClone->as<DB::ASTInsertQuery>()->table_id.table_name = ToString(table->Path);
            auto insertAst = queryClone->as<DB::ASTInsertQuery>();
            insertAst->select = secondaryQueryAst;
            secondaryQueryAst = queryClone;
        });

        preparer.Fire();

        // Finally, build pipeline of all those pipes.
        auto pipeline = preparer.ExtractPipeline([] {});

        return std::move(pipeline);
    }

    std::unordered_map<std::string, DB::ColumnSize> getColumnSizes() const override
    {
        TCurrentTraceContextGuard guard(QueryContext_->TraceContext);

        for (const auto& table : Tables_) {
            if (table->Dynamic) {
                YT_LOG_DEBUG(
                    "Storage contains dynamic tables, returning empty columnar statistics (Table: %v)",
                    table->Path);
                return {};
            }
        }

        auto tableColumnarStatisticsCache = QueryContext_->Host->GetTableColumnarStatisticsCache();
        std::vector<TTableColumnarStatisticsCache::TRequest> requests;
        for (const auto& table : Tables_) {
            requests.push_back(TTableColumnarStatisticsCache::TRequest{
                .ObjectId = table->ObjectId,
                .ExternalCellTag = table->ExternalCellTag,
                .ChunkCount = table->ChunkCount,
                .Schema = table->Schema,
                .MinRevision = table->Revision,
            });
        }
        auto asyncResult = tableColumnarStatisticsCache->GetFreshStatistics(std::move(requests));
        auto result = WaitFor(asyncResult);

        if (!result.IsOK()) {
            YT_LOG_WARNING(result, "Error getting table columnar statistics");
            return {};
        }

        for (const auto& [table, statisticsOrError] : Zip(Tables_, result.Value())) {
            if (!statisticsOrError.IsOK()) {
                YT_LOG_WARNING(result, "Error getting table columnar statistics for particular table (Table: %v)", table->Path);
                return {};
            }
        }

        std::unordered_map<std::string, DB::ColumnSize> columnSizes;
        THashMap<std::string, ui64> columnDataWeights;
        for (const auto& statisticsOrError : result.Value()) {
            YT_VERIFY(statisticsOrError.IsOK());
            const auto& statistics = statisticsOrError.Value();
            for (const auto& [columnName, dataWeight] : statistics.ColumnDataWeights) {
                // We only set data_compressed as it is used in WHERE to PREWHERE CH optimizer.
                columnSizes[columnName].data_compressed += dataWeight;
                columnDataWeights[columnName] += dataWeight;
            }
        }

        std::vector<std::pair<std::string, ui64>> columnDataWeightsForLogging(columnDataWeights.begin(), columnDataWeights.end());
        std::sort(
            columnDataWeightsForLogging.begin(),
            columnDataWeightsForLogging.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.second > rhs.second;
            });

        constexpr int DefaultShrunkFormattableViewCount = 10;

        YT_LOG_DEBUG(
            "Column data weights calculated (Tables: %v, ColumnDataWeights: %v)",
            MakeShrunkFormattableView(Tables_, TDefaultFormatter(), DefaultShrunkFormattableViewCount),
            MakeShrunkFormattableView(columnDataWeightsForLogging, TDefaultFormatter(), DefaultShrunkFormattableViewCount));

        return columnSizes;
    }

    // IStorageDistributor overrides.

    std::vector<TTablePtr> GetTables() const override
    {
        return Tables_;
    }

    TTableSchemaPtr GetSchema() const override
    {
        return Schema_;
    }

private:
    TQueryContext* QueryContext_;
    std::vector<TTablePtr> Tables_;
    TTableSchemaPtr Schema_;
    size_t DistributionSeed_;
    TLogger Logger;

    TDistributedQueryPreparer BuildPreparer(
        const DB::Names& columnNames,
        DB::StorageMetadataPtr metadataSnapshot,
        DB::SelectQueryInfo& queryInfo,
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum processedStage)
    {
        if (!metadataSnapshot) {
            metadataSnapshot = getInMemoryMetadataPtr();
        }

        auto* queryContext = GetQueryContext(context);
        auto* storageContext = queryContext->GetOrRegisterStorageContext(this, context);

        auto [realColumnNames, virtualColumnNames] = DecoupleColumns(columnNames, metadataSnapshot);

        ValidateReadPermissions(realColumnNames, Tables_, queryContext);

        TDistributedQueryPreparer preparer(
            realColumnNames,
            virtualColumnNames,
            queryInfo,
            context,
            queryContext,
            storageContext,
            processedStage,
            DistributionSeed_);

        preparer.PrepareSecondaryQueries();

        return std::move(preparer);
    }

    //! Erase underlying table (assuming that we have single underlying static table)
    void EraseTable(DB::ContextPtr context)
    {
        auto* queryContext = GetQueryContext(context);

        const auto& client = queryContext->Client();
        const auto& path = Tables_[0]->Path.GetPath();

        YT_LOG_DEBUG("Erasing table (Path: %v)", path);
        WaitFor(client->ConcatenateNodes({}, TRichYPath(path)))
            .ThrowOnError();
        YT_LOG_DEBUG("Table erased (Path: %v)", path);
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateDistributorFromCH(DB::StorageFactory::Arguments args)
{
    auto* queryContext = GetQueryContext(args.getLocalContext());
    const auto& client = queryContext->Client();
    const auto& Logger = queryContext->Logger;

    TCurrentTraceContextGuard guard(queryContext->TraceContext);

    TKeyColumns keyColumns;

    if (args.storage_def->order_by) {
        auto orderByAst = args.storage_def->order_by->ptr();
        orderByAst = DB::MergeTreeData::extractKeyExpressionList(orderByAst);
        for (const auto& child : orderByAst->children) {
            auto* identifier = dynamic_cast<DB::ASTIdentifier*>(child.get());
            if (!identifier) {
                THROW_ERROR_EXCEPTION("CHYT does not support compound expressions as parts of key")
                    << TErrorAttribute("expression", child->getColumnName());
            }
            keyColumns.emplace_back(identifier->getColumnName());
        }
    }

    auto path = TRichYPath::Parse(TString(args.table_id.table_name));
    YT_LOG_INFO("Creating table from CH engine (Path: %v, Columns: %v, KeyColumns: %v)",
        path,
        args.columns.toString(),
        keyColumns);

    auto attributes = ConvertToAttributes(queryContext->Host->GetConfig()->CreateTableDefaultAttributes);
    if (!args.engine_args.empty()) {
        if (static_cast<int>(args.engine_args.size()) > 1) {
            THROW_ERROR_EXCEPTION("YtTable accepts at most one argument");
        }
        const auto* ast = args.engine_args[0]->as<DB::ASTLiteral>();
        if (ast && ast->value.getType() == DB::Field::Types::String) {
            auto extraAttributes = ConvertToAttributes(TYsonString(TString(DB::safeGet<std::string>(ast->value))));
            attributes->MergeFrom(*extraAttributes);
        } else {
            THROW_ERROR_EXCEPTION("Extra attributes must be a string literal");
        }
    }

    // Underscore indicates that the columns should be ignored, and that schema should be taken from the attributes.
    if (args.columns.getNamesOfPhysical() != std::vector<std::string>{"_"}) {
        auto schema = ToTableSchema(args.columns, keyColumns, queryContext->Settings->Composite);
        YT_LOG_DEBUG("Inferred table schema from columns (Schema: %v)", schema);
        attributes->Set("schema", schema);
    } else if (attributes->Contains("schema")) {
        YT_LOG_DEBUG("Table schema is taken from attributes (Schema: %v)", attributes->FindYson("schema"));
    } else {
        THROW_ERROR_EXCEPTION(
            "Table schema should be specified either by column list (possibly with ORDER BY) or by "
            "YT schema in attributes (as the only storage argument in YSON under key `schema`, in this case "
            "column list should consist of the only column named `_`)");
    };

    YT_LOG_DEBUG("Creating table (Attributes: %v)", ConvertToYsonString(attributes->ToMap(), EYsonFormat::Text));

    auto schema = attributes->Get<TTableSchemaPtr>("schema");

    NApi::TCreateNodeOptions options;
    options.Attributes = std::move(attributes);
    options.Recursive = true;
    auto id = WaitFor(client->CreateNode(path.GetPath(), NObjectClient::EObjectType::Table, options))
        .ValueOrThrow();
    YT_LOG_DEBUG("Table created (ObjectId: %v)", id);

    auto table = FetchTables(
        queryContext->Client(),
        queryContext->Host,
        {path},
        /* skipUnsuitableNodes */ false,
        queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
        queryContext->Logger);

    return std::make_shared<TStorageDistributor>(
        args.getLocalContext(),
        std::vector{table},
        schema);
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(
    DB::ContextPtr context,
    std::vector<TTablePtr> tables)
{
    if (tables.empty()) {
        THROW_ERROR_EXCEPTION("No tables to read from");
    }

    auto* queryContext = GetQueryContext(context);

    auto commonSchema = InferCommonSchema(tables, queryContext->Logger);

    auto storage = std::make_shared<TStorageDistributor>(
        context,
        std::move(tables),
        std::move(commonSchema));

    storage->startup();

    return storage;
}

////////////////////////////////////////////////////////////////////////////////

void RegisterStorageDistributor()
{
    auto& factory = DB::StorageFactory::instance();
    factory.registerStorage("YtTable", CreateDistributorFromCH, DB::StorageFactory::StorageFeatures{
        .supports_sort_order = true,
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
