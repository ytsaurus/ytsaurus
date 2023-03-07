#include "storage_distributor.h"

#include "config.h"
#include "block_input_stream.h"
#include "block_output_stream.h"
#include "helpers.h"
#include "query_context.h"
#include "subquery.h"
#include "join_workaround.h"
#include "schema.h"
#include "query_registry.h"
#include "query_analyzer.h"
#include "table.h"
#include "host.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/ypath/rich.h>

#include <DataStreams/materializeBlock.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ProcessList.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/queryToString.h>

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

    // This setting is really not for user and should not be sent to remote server.
    newSettings.max_memory_usage_for_all_queries = 0;

    // Set as unchanged to avoid sending to remote server.
    newSettings.max_concurrent_queries_for_user.changed = false;
    newSettings.max_memory_usage_for_user.changed = false;
    newSettings.max_memory_usage_for_all_queries.changed = false;

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

DB::BlockInputStreamPtr CreateRemoteStream(
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
    DB::Block header = materializeBlock(DB::InterpreterSelectQuery(
        queryAst,
        context,
        DB::SelectQueryOptions(processedStage).analyze()).getSampleBlock());

    auto stream = std::make_shared<DB::RemoteBlockInputStream>(
        remoteNode->GetConnection(),
        query,
        header,
        context,
        nullptr,    // will use settings from context
        throttler,
        externalTables,
        processedStage);

    stream->setPoolMode(DB::PoolMode::GET_MANY);
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
    stream->setQueryId(compositeQueryId);

    return CreateBlockInputStreamLoggingAdapter(std::move(stream), TLogger(queryContext->Logger)
        .AddTag("RemoteQueryId: %v, RemoteNode: %v, RemoteStreamId: %v",
            remoteQueryId,
            remoteNode->GetName().ToString(),
            TGuid::Create()));
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

class TStorageDistributor
    : public DB::IStorage
    , public IStorageDistributor
{
public:
    friend class TQueryAnalyzer;

    TStorageDistributor(
        TQueryContext* queryContext,
        std::vector<TTablePtr> tables,
        TTableSchema schema)
        : QueryContext_(queryContext)
        , Tables_(std::move(tables))
        , Schema_(std::move(schema))
        , Logger(QueryContext_->Logger)
    { }

    virtual void startup() override
    {
        YT_LOG_TRACE("StorageDistributor instantiated (Address: %v)", static_cast<void*>(this));
        if (Schema_.GetColumnCount() == 0) {
            THROW_ERROR_EXCEPTION("CHYT does not support tables without schema")
                << TErrorAttribute("path", getTableName());
        }
        setColumns(DB::ColumnsDescription(ToNamesAndTypesList(Schema_)));
    }

    std::string getName() const override
    {
        return "StorageDistributor";
    }

    virtual std::string getDatabaseName() const override
    {
        return "";
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
        return Schema_.IsSorted();
    }

    virtual bool mayBenefitFromIndexForIn(const DB::ASTPtr& /* queryAst */, const DB::Context& /* context */) const override
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

    virtual DB::QueryProcessingStage::Enum getQueryProcessingStage(const DB::Context& context) const override
    {
        // If we use WithMergeableState while using single node, caller would process aggregation functions incorrectly.
        // See also: need_second_distinct_pass at DB::InterpreterSelectQuery::executeImpl().
        if (!context.getSettingsRef().distributed_group_by_no_merge &&
            QueryContext_->Host->GetNodes().size() != 1)
        {
            return DB::QueryProcessingStage::WithMergeableState;
        }
        return DB::QueryProcessingStage::Complete;
    }

    virtual DB::BlockInputStreams read(
        const DB::Names& columnNames,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum processedStage,
        size_t /* maxBlockSize */,
        unsigned /* numStreams */) override
    {
        ValidateReadPermissions(ToVectorString(columnNames), Tables_, QueryContext_);

        YT_LOG_TRACE("StorageDistributor started reading (Address: %v)", static_cast<void*>(this));

        SpecTemplate_ = TSubquerySpec();
        SpecTemplate_.InitialQueryId = QueryContext_->QueryId;
        SpecTemplate_.InitialQuery = ToString(queryInfo.query);

        auto cliqueNodes = QueryContext_->Host->GetNodes();
        if (cliqueNodes.empty()) {
            THROW_ERROR_EXCEPTION("There are no instances available through discovery");
        }

        QueryContext_->MoveToPhase(EQueryPhase::Preparation);

        Prepare(cliqueNodes.size(), queryInfo, columnNames, context);

        YT_LOG_INFO("Starting distribution (ColumnNames: %v, TableName: %v, NodeCount: %v, MaxThreads: %v, SubqueryCount: %v)",
            columnNames,
            getTableName(),
            cliqueNodes.size(),
            static_cast<ui64>(context.getSettings().max_threads),
            Subqueries_.size());

        const auto& settings = context.getSettingsRef();

        DB::Context newContext(context);
        newContext.setSettings(PrepareLeafJobSettings(settings));

        // TODO(max42): do we need them?
        auto throttler = CreateNetThrottler(settings);

        DB::BlockInputStreams streams;

        // TODO(max42): CHYT-154.
        SpecTemplate_.MembershipHint = DumpMembershipHint(*queryInfo.query, Logger);

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
                YT_LOG_DEBUG("Thread subquery (Cookie: %v, LowerLimit: %v, UpperLimit: %v, DataWeight: %v, RowCount: %v, ChunkCount: %v)",
                    threadSubquery.Cookie,
                    threadSubquery.Limits.first,
                    threadSubquery.Limits.second,
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

            bool isLocal = cliqueNode->IsLocal();
            // XXX(max42): weird workaround.
            isLocal = false;
            auto substream = isLocal
                ? CreateLocalStream(
                    subqueryAst,
                    newContext,
                    processedStage)
                : CreateRemoteStream(
                    cliqueNode,
                    subqueryAst,
                    newContext,
                    throttler,
                    context.getExternalTables(),
                    processedStage,
                    Logger);

            streams.push_back(std::move(substream));
        }

        YT_LOG_INFO("Finished distribution");

        return streams;
    }

    virtual bool supportsSampling() const override
    {
        return true;
    }

    virtual DB::BlockOutputStreamPtr write(const DB::ASTPtr& /* ptr */, const DB::Context& /* context */) override
    {
        // Set append if it is not set.

        if (Tables_.size() != 1) {
            THROW_ERROR_EXCEPTION("Cannot write to many tables simultaneously")
                << TErrorAttribute("paths", getTableName());
        }

        auto path = Tables_.front()->Path;
        path.SetAppend(path.GetAppend(true /* defaultValue */));
        auto writer = WaitFor(CreateSchemalessTableWriter(
            QueryContext_->Host->GetConfig()->TableWriterConfig,
            New<TTableWriterOptions>(),
            path,
            New<TNameTable>(),
            QueryContext_->Client(),
            nullptr /* transaction */))
            .ValueOrThrow();
        return CreateBlockOutputStream(std::move(writer), QueryContext_->Logger);
    }

    // IStorageDistributor overrides.

    virtual std::vector<TTablePtr> GetTables() const override
    {
        return Tables_;
    }

    virtual TTableSchema GetSchema() const override
    {
        return Schema_;
    }

private:
    TQueryContext* QueryContext_;
    std::vector<TTablePtr> Tables_;
    TTableSchema Schema_;
    TSubquerySpec SpecTemplate_;
    std::vector<TSubquery> Subqueries_;
    std::optional<TQueryAnalyzer> QueryAnalyzer_;
    // TODO(max42): YT-11778.
    // TMiscExt is used for better memory estimation in readers, but it is dropped when using
    // TInputChunk, so for now we store it explicitly in a map and use when serializing subquery input.
    THashMap<TChunkId, TRefCountedMiscExtPtr> MiscExtMap_;
    TLogger Logger;

    void Prepare(
        int subqueryCount,
        const DB::SelectQueryInfo& queryInfo,
        const std::vector<std::string>& columnNames,
        const DB::Context& context)
    {
        NTracing::TChildTraceContextGuard guard("ClickHouseYt.Prepare", /* forceTracing */ true);
        NTracing::GetCurrentTraceContext()->AddTag("chyt.subquery_count", ToString(subqueryCount));
        NTracing::GetCurrentTraceContext()->AddTag("chyt.column_names", Format("%v", MakeFormattableView(columnNames, TDefaultFormatter())));

        QueryAnalyzer_.emplace(context, queryInfo);
        auto queryAnalysisResult = QueryAnalyzer_->Analyze();

        auto input = FetchInput(
            QueryContext_,
            queryAnalysisResult,
            SpecTemplate_);

        MiscExtMap_ = std::move(input.MiscExtMap);

        std::optional<double> samplingRate;
        const auto& selectQuery = queryInfo.query->as<DB::ASTSelectQuery&>();
        if (auto selectSampleSize = selectQuery.sample_size()) {
            auto ratio = selectSampleSize->as<DB::ASTSampleRatio&>().ratio;
            auto rate = static_cast<double>(ratio.numerator) / ratio.denominator;
            if (rate > 1.0) {
                rate /= input.StripeList->TotalRowCount;
            }
            rate = std::clamp(rate, 0.0, 1.0);
            samplingRate = rate;
        }

        Subqueries_ = BuildSubqueries(
            std::move(input.StripeList),
            queryAnalysisResult.KeyColumnCount,
            queryAnalysisResult.PoolKind,
            std::max<int>(1, subqueryCount * context.getSettings().max_threads),
            samplingRate,
            context,
            QueryContext_->Host->GetConfig()->Subquery);

        size_t totalInputDataWeight = 0;

        for (const auto& subquery : Subqueries_) {
            totalInputDataWeight += subquery.StripeList->TotalDataWeight;
        }

        for (const auto& subquery : Subqueries_) {
            if (static_cast<ui64>(subquery.StripeList->TotalDataWeight) >
                QueryContext_->Host->GetConfig()->Subquery->MaxDataWeightPerSubquery)
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
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateDistributorFromCH(DB::StorageFactory::Arguments args)
{
    auto* queryContext = GetQueryContext(args.local_context);
    const auto& client = queryContext->Client();
    const auto& Logger = queryContext->Logger;

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

    auto path = TRichYPath::Parse(TString(args.table_name));
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

    auto schema = attributes->Get<TTableSchema>("schema");

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
        queryContext->Logger);

    return std::make_shared<TStorageDistributor>(
        queryContext,
        std::vector<TTablePtr>{table},
        schema);
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(
    TQueryContext* queryContext,
    std::vector<TTablePtr> tables)
{
    if (tables.empty()) {
        THROW_ERROR_EXCEPTION("No tables to read from");
    }

    auto commonSchema = InferCommonSchema(tables, queryContext->Logger);

    auto storage = std::make_shared<TStorageDistributor>(
        queryContext,
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
