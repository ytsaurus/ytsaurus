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
#include "query_context.h"
#include "query_registry.h"
#include "schema.h"
#include "storage_base.h"
#include "subquery.h"
#include "table.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/client/table_client/name_table.h>

#include <yt/client/ypath/rich.h>

#include <DataStreams/materializeBlock.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ProcessList.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/RemoteSource.h>

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
    const DB::Context& context,
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
    const DB::Context& context,
    const DB::ThrottlerPtr& throttler,
    const DB::Tables& externalTables,
    DB::QueryProcessingStage::Enum processedStage,
    TLogger logger)
{
    const auto& Logger = logger;

    const auto* queryContext = GetQueryContext(context);

    std::string query = queryToString(queryAst);

    // TODO(max42): can be done only once?
    DB::Block header = DB::InterpreterSelectQuery(
        queryAst,
        context,
        DB::SelectQueryOptions(processedStage).analyze()).getSampleBlock();

    auto remoteQueryExecutor = std::make_shared<DB::RemoteQueryExecutor>(
        remoteNode->GetConnection(),
        query,
        header,
        context,
        throttler,
        context.getQueryContext().getScalars(),
        externalTables,
        processedStage);
    remoteQueryExecutor->setPoolMode(DB::PoolMode::GET_MANY);

    auto remoteQueryId = ToString(TQueryId::Create());
    auto* traceContext = GetCurrentTraceContext();
    if (!traceContext) {
        traceContext = queryContext->TraceContext.Get();
    }
    YT_VERIFY(traceContext);
    auto traceId = traceContext->GetTraceId();
    auto spanId = traceContext->GetSpanId();
    auto sampled = traceContext->IsSampled() ? "T" : "F";
    auto compositeQueryId = Format("%v@%v@%" PRIx64 "@%v", remoteQueryId, traceId, spanId, sampled);

    YT_LOG_INFO("Composite query id for secondary query constructed (RemoteQueryId: %v, CompositeQueryId: %v)", remoteQueryId, compositeQueryId);
    remoteQueryExecutor->setQueryId(compositeQueryId);

    // XXX(max42): should we use this?
    // if (!table_func_ptr)
    //     remote_query_executor->setMainTable(main_table); */

    bool addAggregationInfo = processedStage == DB::QueryProcessingStage::WithMergeableState;
    bool addTotals = false;
    bool addExtremes = false;
    if (processedStage == DB::QueryProcessingStage::Complete) {
        addTotals = queryAst->as<DB::ASTSelectQuery &>().group_by_with_totals;
        addExtremes = context.getSettingsRef().extremes;
    }

    auto pipe = createRemoteSourcePipe(remoteQueryExecutor, addAggregationInfo, addTotals, addExtremes);

    pipe.addSimpleTransform([&](const DB::Block & header) {
        return std::make_shared<TLoggingTransform>(header, TLogger(queryContext->Logger)
            .AddTag("RemoteQueryId: %v, RemoteNode: %v",
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

////////////////////////////////////////////////////////////////////////////////

//! This class is extracted for better encapsulation of read() call context.
//! Recall that TStorageDistributor may be reused for several subqueries.
class TDistributionPreparer
{
public:
    TDistributionPreparer(
        const std::vector<TString>& realColumnNames,
        const std::vector<TString>& virtualColumnNames,
        DB::SelectQueryInfo& queryInfo,
        const DB::Context& context,
        TQueryContext* queryContext,
        TStorageContext* storageContext,
        DB::QueryProcessingStage::Enum processedStage)
        : RealColumnNames_(realColumnNames)
        , VirtualColumnNames_(virtualColumnNames)
        , QueryInfo_(queryInfo)
        , Context_(context)
        , QueryContext_(queryContext)
        , StorageContext_(storageContext)
        , ProcessedStage_(processedStage)
        , Logger(StorageContext_->Logger)
    { }

    DB::Pipe Prepare()
    {
        YT_LOG_DEBUG("Preparing distribution (QueryAST: %v)", *QueryInfo_.query, static_cast<void*>(this));

        NTracing::TChildTraceContextGuard guard("ClickHouseYt.Prepare");

        if (StorageContext_->Settings->ThrowTestingExceptionInDistributor) {
            THROW_ERROR_EXCEPTION("Testing exception in distributor")
                << TErrorAttribute("storage_index", StorageContext_->Index);
        }

        SpecTemplate_ = TSubquerySpec();
        SpecTemplate_.InitialQueryId = QueryContext_->QueryId;
        SpecTemplate_.InitialQuery = SerializeAndMaybeTruncateSubquery(*QueryInfo_.query);
        SpecTemplate_.QuerySettings = StorageContext_->Settings;

        auto cliqueNodes = QueryContext_->Host->GetNodes();
        if (cliqueNodes.empty()) {
            THROW_ERROR_EXCEPTION("There are no instances available through discovery");
        }

        QueryContext_->MoveToPhase(EQueryPhase::Preparation);

        PrepareSubqueries(cliqueNodes.size(), QueryInfo_, Context_);

        YT_LOG_INFO("Starting distribution (RealColumnNames_: %v, NodeCount: %v, MaxThreads: %v, SubqueryCount: %v)",
            RealColumnNames_,
            cliqueNodes.size(),
            static_cast<ui64>(Context_.getSettings().max_threads),
            Subqueries_.size());

        const auto& settings = Context_.getSettingsRef();

        DB::Context newContext(Context_);
        newContext.setSettings(PrepareLeafJobSettings(settings));

        // TODO(max42): do we need them?
        auto throttler = CreateNetThrottler(settings);

        DB::Pipes pipes;

        std::sort(Subqueries_.begin(), Subqueries_.end(), [] (const TSubquery& lhs, const TSubquery& rhs) {
            return lhs.Cookie < rhs.Cookie;
        });

        // NB: this is important for queries to distribute deterministically across cluster.
        std::sort(cliqueNodes.begin(), cliqueNodes.end(), [] (const IClusterNodePtr& lhs, const IClusterNodePtr& rhs) {
            return lhs->GetName().ToString() < rhs->GetName().ToString();
        });

        for (const auto& cliqueNode : cliqueNodes) {
            YT_LOG_DEBUG("Clique node (Host: %v, Port: %v, IsLocal: %v)",
                cliqueNode->GetName().Host,
                cliqueNode->GetName().Port,
                cliqueNode->IsLocal());
        }

        QueryContext_->MoveToPhase(EQueryPhase::Execution);

        int subqueryCount = std::min(Subqueries_.size(), cliqueNodes.size());

        if (subqueryCount == 0) {
            // NB: if we make no subqueries, there will be a tricky issue around schemas.
            // Namely, we return an empty vector of streams, so the resulting schema will
            // be taken from columns of this storage (which are set via setColumns).
            // Such schema will be incorrect as it will lack aggregates in mergeable state
            // which should normally return from our distributed storage.
            // In order to overcome this, we forcefully make at least one stream, even though it
            // will return empty result for sure.
            subqueryCount = 1;
        }

        for (int index = 0; index < subqueryCount; ++index) {
            int firstSubqueryIndex = index * Subqueries_.size() / subqueryCount;
            int lastSubqueryIndex = (index + 1) * Subqueries_.size() / subqueryCount;

            auto threadSubqueries = MakeRange(Subqueries_.data() + firstSubqueryIndex, Subqueries_.data() + lastSubqueryIndex);

            YT_LOG_DEBUG("Preparing subquery (SubqueryIndex: %v, ThreadSubqueryCount: %v)",
                index,
                subqueryCount);
            for (const auto& threadSubquery : threadSubqueries) {
                YT_LOG_DEBUG("Thread subquery (Cookie: %v, LowerBound: %v, UpperBound: %v, DataWeight: %v, RowCount: %v, ChunkCount: %v)",
                    threadSubquery.Cookie,
                    threadSubquery.Bounds.first,
                    threadSubquery.Bounds.second,
                    threadSubquery.StripeList->TotalDataWeight,
                    threadSubquery.StripeList->TotalRowCount,
                    threadSubquery.StripeList->TotalChunkCount);
            }

            YT_VERIFY(!threadSubqueries.Empty() || Subqueries_.empty());

            const auto& cliqueNode = cliqueNodes[index];
            auto subqueryAst = QueryAnalyzer_->RewriteQuery(
                threadSubqueries,
                SpecTemplate_,
                MiscExtMap_,
                index,
                index + 1 == subqueryCount /* isLastSubquery */);

            YT_LOG_DEBUG("Subquery prepared (Node: %v, ThreadSubqueryCount: %v, SubqueryIndex: %v, TotalSubqueryCount: %v)",
                cliqueNode->GetName().ToString(),
                lastSubqueryIndex - firstSubqueryIndex,
                index,
                subqueryCount);

            auto pipe = CreateRemoteSource(
                cliqueNode,
                subqueryAst,
                newContext,
                throttler,
                Context_.getExternalTables(),
                ProcessedStage_,
                Logger);

            pipes.emplace_back(std::move(pipe));
        }

        YT_LOG_INFO("Finished distribution");

        return DB::Pipe::unitePipes(std::move(pipes));
    }

    void PrepareSubqueries(
        int subqueryCount,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context)
    {
        NTracing::GetCurrentTraceContext()->AddTag("chyt.subquery_count", ToString(subqueryCount));
        NTracing::GetCurrentTraceContext()->AddTag(
            "chyt.real_column_names",
            Format("%v", MakeFormattableView(RealColumnNames_, TDefaultFormatter())));
        NTracing::GetCurrentTraceContext()->AddTag(
            "chyt.virtual_column_names",
            Format("%v", MakeFormattableView(VirtualColumnNames_, TDefaultFormatter())));

        QueryAnalyzer_.emplace(context, StorageContext_, queryInfo, Logger);
        auto queryAnalysisResult = QueryAnalyzer_->Analyze();

        YT_LOG_TRACE("Preparing StorageDistributor for query (Query: %v)", *queryInfo.query);

        auto input = FetchInput(
            StorageContext_,
            queryAnalysisResult,
            RealColumnNames_,
            VirtualColumnNames_,
            TClickHouseIndexBuilder(&queryInfo, &context));

        YT_VERIFY(!SpecTemplate_.DataSourceDirectory);
        SpecTemplate_.DataSourceDirectory = std::move(input.DataSourceDirectory);

        MiscExtMap_ = std::move(input.MiscExtMap);

        std::optional<double> samplingRate;
        const auto& selectQuery = queryInfo.query->as<DB::ASTSelectQuery&>();
        if (auto selectSampleSize = selectQuery.sampleSize()) {
            auto ratio = selectSampleSize->as<DB::ASTSampleRatio&>().ratio;
            auto rate = static_cast<double>(ratio.numerator) / ratio.denominator;
            if (rate > 1.0) {
                rate /= input.StripeList->TotalRowCount;
            }
            rate = std::clamp(rate, 0.0, 1.0);
            samplingRate = rate;
        }

        bool canUseBlockSampling = StorageContext_->Settings->UseBlockSampling;
        for (const auto& tables : queryAnalysisResult.Tables) {
            for (const auto& table : tables) {
                if (table->Dynamic) {
                    canUseBlockSampling = false;
                }
            }
        }
        if (queryAnalysisResult.PoolKind != EPoolKind::Unordered) {
            canUseBlockSampling = false;
        }

        auto tableReaderConfig = New<TTableReaderConfig>();
        if (canUseBlockSampling && samplingRate) {
            YT_LOG_DEBUG("Using block sampling (SamplingRate: %v)",
                samplingRate);
            for (const auto& stripe : input.StripeList->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                        chunkSlice->ApplySamplingSelectivityFactor(*samplingRate);
                    }
                }
            }
            tableReaderConfig->SamplingRate = samplingRate;
            tableReaderConfig->SamplingMode = ESamplingMode::Block;
            if (queryInfo.prewhere_info) {
                // When PREWHERE is present, same chunk is processed several times
                // (first on PREWHERE phase, then on main phase),
                // so we fix seed for sampling for the sake of determinism.
                tableReaderConfig->SamplingSeed = RandomNumber<ui64>();
            }
            samplingRate = std::nullopt;
        }
        SpecTemplate_.TableReaderConfig = tableReaderConfig;

        Subqueries_ = BuildSubqueries(
            std::move(input.StripeList),
            queryAnalysisResult.KeyColumnCount,
            queryAnalysisResult.PoolKind,
            SpecTemplate_.DataSourceDirectory,
            std::max<int>(1, subqueryCount * context.getSettings().max_threads),
            samplingRate,
            StorageContext_,
            QueryContext_->Host->GetConfig()->Subquery);

        size_t totalInputDataWeight = 0;
        size_t totalChunkCount = 0;

        for (const auto& subquery : Subqueries_) {
            totalInputDataWeight += subquery.StripeList->TotalDataWeight;
            totalChunkCount += subquery.StripeList->TotalChunkCount;
        }

        for (const auto& subquery : Subqueries_) {
            if (subquery.StripeList->TotalDataWeight > QueryContext_->Host->GetConfig()->Subquery->MaxDataWeightPerSubquery)
            {
                THROW_ERROR_EXCEPTION(
                    NClickHouseServer::EErrorCode::SubqueryDataWeightLimitExceeded,
                    "Subquery exceeds data weight limit: %v > %v",
                    subquery.StripeList->TotalDataWeight,
                    QueryContext_->Host->GetConfig()->Subquery->MaxDataWeightPerSubquery)
                    << TErrorAttribute("total_input_data_weight", totalInputDataWeight);
            }
        }

        NTracing::GetCurrentTraceContext()->AddTag("chyt.total_input_data_weight", ToString(totalInputDataWeight));
        NTracing::GetCurrentTraceContext()->AddTag("chyt.total_chunk_count", ToString(totalChunkCount));
    }

private:
    std::vector<TString> RealColumnNames_;
    std::vector<TString> VirtualColumnNames_;
    const DB::SelectQueryInfo& QueryInfo_;
    const DB::Context& Context_;
    TQueryContext* const QueryContext_;
    TStorageContext* const StorageContext_;
    const DB::QueryProcessingStage::Enum ProcessedStage_;
    const TLogger Logger;

    TSubquerySpec SpecTemplate_;
    std::vector<TSubquery> Subqueries_;
    std::optional<TQueryAnalyzer> QueryAnalyzer_;
    // TODO(max42): YT-11778.
    // TMiscExt is used for better memory estimation in readers, but it is dropped when using
    // TInputChunk, so for now we store it explicitly in a map and use when serializing subquery input.
    THashMap<TChunkId, TRefCountedMiscExtPtr> MiscExtMap_;
};

////////////////////////////////////////////////////////////////////////////////

class TStorageDistributor
    : public TYtStorageBase
    , public IStorageDistributor
{
public:
    friend class TQueryAnalyzer;

    TStorageDistributor(
        const DB::Context& context,
        std::vector<TTablePtr> tables,
        TTableSchemaPtr schema)
        : TYtStorageBase({"YT", "distributor"})
        , QueryContext_(GetQueryContext(context))
        , Tables_(std::move(tables))
        , Schema_(std::move(schema))
        , Logger(QueryContext_->Logger)
    { }

    virtual void startup() override
    {
        TTraceContextGuard guard(QueryContext_->TraceContext);

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

    virtual bool supportsIndexForIn() const override
    {
        return Schema_->IsSorted();
    }

    virtual bool mayBenefitFromIndexForIn(const DB::ASTPtr& /* queryAst */, const DB::Context& /* context */, const DB::StorageMetadataPtr& /* metadata_snapshot */) const override
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

    virtual DB::QueryProcessingStage::Enum getQueryProcessingStage(
        const DB::Context& context,
        DB::QueryProcessingStage::Enum toStage,
        DB::SelectQueryInfo &) const override
    {
        // If we use WithMergeableState while using single node, caller would process aggregation functions incorrectly.
        // See also: need_second_distinct_pass at DB::InterpreterSelectQuery::executeImpl().
        if (context.getSettings().distributed_group_by_no_merge) {
            return DB::QueryProcessingStage::Complete;
        }

        if (toStage == DB::QueryProcessingStage::WithMergeableState) {
            return DB::QueryProcessingStage::WithMergeableState;
        }

        if (QueryContext_->Host->GetNodes().size() != 1) {
            return DB::QueryProcessingStage::WithMergeableState;
        } else {
            return DB::QueryProcessingStage::Complete;
        }
    }

    virtual DB::Pipe read(
        const DB::Names& columnNames,
        const DB::StorageMetadataPtr& metadataSnapshot,
        DB::SelectQueryInfo& queryInfo,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum processedStage,
        size_t /* maxBlockSize */,
        unsigned /* numStreams */) override
    {
        TTraceContextGuard guard(QueryContext_->TraceContext);

        auto* queryContext = GetQueryContext(context);
        auto* storageContext = queryContext->GetOrRegisterStorageContext(this, context);

        auto [realColumnNames, virtualColumnNames] = DecoupleColumns(columnNames, metadataSnapshot);

        ValidateReadPermissions(realColumnNames, Tables_, queryContext);

        return TDistributionPreparer(
            realColumnNames,
            virtualColumnNames,
            queryInfo,
            context,
            queryContext,
            storageContext,
            processedStage)
                .Prepare();
    }

    virtual bool supportsSampling() const override
    {
        return true;
    }

    virtual DB::BlockOutputStreamPtr write(const DB::ASTPtr& /* ptr */, const DB::StorageMetadataPtr& /*metadata_snapshot*/, const DB::Context& /* context */) override
    {
        TTraceContextGuard guard(QueryContext_->TraceContext);

        if (Tables_.size() != 1) {
            THROW_ERROR_EXCEPTION("Cannot write to many tables simultaneously")
                << TErrorAttribute("paths", getTableName());
        }
        const auto& table = Tables_.front();
        auto path = table->Path;
        if (table->Dynamic) {
            return CreateDynamicTableBlockOutputStream(
                path,
                table->Schema,
                QueryContext_->Settings->DynamicTable,
                QueryContext_->Client(),
                QueryContext_->Logger);
        } else {
            // Set append if it is not set.
            path.SetAppend(path.GetAppend(true /* defaultValue */));
            return CreateStaticTableBlockOutputStream(
                path,
                table->Schema,
                QueryContext_->Host->GetConfig()->TableWriterConfig,
                QueryContext_->Client(),
                QueryContext_->Logger);
        }
    }

    // IStorageDistributor overrides.

    virtual std::vector<TTablePtr> GetTables() const override
    {
        return Tables_;
    }

    virtual TTableSchemaPtr GetSchema() const override
    {
        return Schema_;
    }

private:
    TQueryContext* QueryContext_;
    std::vector<TTablePtr> Tables_;
    TTableSchemaPtr Schema_;
    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateDistributorFromCH(DB::StorageFactory::Arguments args)
{
    auto* queryContext = GetQueryContext(args.local_context);
    const auto& client = queryContext->Client();
    const auto& Logger = queryContext->Logger;

    TTraceContextGuard guard(queryContext->TraceContext);

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
        auto schema = ConvertToTableSchema(args.columns, keyColumns);
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
        args.local_context,
        std::vector{table},
        schema);
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(
    const DB::Context& context,
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
