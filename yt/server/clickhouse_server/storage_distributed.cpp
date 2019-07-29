#include "storage_distributed.h"

#include "config.h"
#include "bootstrap.h"
#include "block_input_stream.h"
#include "type_helpers.h"
#include "helpers.h"
#include "query_context.h"
#include "subquery.h"
#include "join_workaround.h"

#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <Common/Exception.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSampleRatio.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <library/string_utils/base64/base64.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

BlockInputStreams TStorageDistributedBase::read(
    const Names& columnNames,
    const SelectQueryInfo& queryInfo,
    const Context& context,
    QueryProcessingStage::Enum processedStage,
    size_t /* maxBlockSize */,
    unsigned /* numStreams */)
{
    auto* queryContext = GetQueryContext(context);
    const auto& Logger = queryContext->Logger;

    SpecTemplate.InitialQueryId = queryContext->QueryId;

    auto cliqueNodes = queryContext->Bootstrap->GetHost()->GetNodes();
    Prepare(cliqueNodes.size(), queryInfo, context);

    YT_LOG_INFO("Preparing query to YT table storage (ColumnNames: %v, TableName: %v, NodeCount: %v, StripeCount: %v)",
        columnNames,
        getTableName(),
        cliqueNodes.size(),
        StripeList->Stripes.size());

    if (StripeList->Stripes.size() > cliqueNodes.size()) {
        throw Exception("Cluster is too small", ErrorCodes::LOGICAL_ERROR);
    }

    // Prepare settings and context for subqueries.

    const auto& settings = context.getSettingsRef();

    processedStage = settings.distributed_group_by_no_merge
        ? QueryProcessingStage::Complete
        : QueryProcessingStage::WithMergeableState;

    Context newContext(context);
    newContext.setSettings(PrepareLeafJobSettings(settings));

    auto throttler = CreateNetThrottler(settings);

    BlockInputStreams streams;

    // TODO(max42): CHYT-154.
    SpecTemplate.MembershipHint = DumpMembershipHint(*queryInfo.query, Logger);

    for (int index = 0; index < static_cast<int>(StripeList->Stripes.size()); ++index) {
        const auto& stripe = StripeList->Stripes[index];
        const auto& cliqueNode = cliqueNodes[index];
        auto spec = SpecTemplate;
        FillDataSliceDescriptors(spec, stripe);

        auto protoSpec = NYT::ToProto<NProto::TSubquerySpec>(spec);
        auto encodedSpec = Base64Encode(protoSpec.SerializeAsString());

        YT_LOG_DEBUG("Rewriting query (OriginalQuery: %v)", *queryInfo.query);
        auto subqueryAst = RewriteSelectQueryForTablePart(queryInfo.query, encodedSpec);
        YT_LOG_DEBUG("Query rewritten (Subquery: %v)", *subqueryAst);

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
                processedStage);

        streams.push_back(std::move(substream));
    }

    YT_LOG_INFO("Finished query preparation");

    return streams;
}

bool TStorageDistributedBase::supportsSampling() const
{
    return true;
}

QueryProcessingStage::Enum TStorageDistributedBase::getQueryProcessingStage(const Context& context) const
{
    const auto& settings = context.getSettingsRef();

    // Set processing stage

    return settings.distributed_group_by_no_merge
        ? QueryProcessingStage::Complete
        : QueryProcessingStage::WithMergeableState;
}

void TStorageDistributedBase::Prepare(
    int subqueryCount,
    const SelectQueryInfo& queryInfo,
    const Context& context)
{
    auto* queryContext = GetQueryContext(context);

    std::unique_ptr<KeyCondition> keyCondition;
    if (ClickHouseSchema.HasPrimaryKey()) {
        keyCondition = std::make_unique<KeyCondition>(CreateKeyCondition(context, queryInfo, ClickHouseSchema));
    }

    auto tablePaths = GetTablePaths();
    auto dataSlices = FetchDataSlices(
        queryContext->Client(),
        queryContext->Bootstrap->GetSerializedWorkerInvoker(),
        tablePaths,
        keyCondition.get(),
        queryContext->RowBuffer,
        queryContext->Bootstrap->GetConfig()->Engine->Subquery,
        SpecTemplate);

    i64 totalRowCount = 0;
    for (const auto& dataSlice : dataSlices) {
        totalRowCount += dataSlice->GetRowCount();
    }

    std::optional<double> samplingRate;
    const auto& selectQuery = queryInfo.query->as<ASTSelectQuery&>();
    if (auto selectSampleSize = selectQuery.sample_size()) {
        auto ratio = selectSampleSize->as<ASTSampleRatio&>().ratio;
        auto rate = static_cast<double>(ratio.numerator) / ratio.denominator;
        if (rate > 1.0) {
            rate /= totalRowCount;
        }
        rate = std::max(0.0, std::min(1.0, rate));
        samplingRate = rate;
    }

    StripeList = SubdivideDataSlices(dataSlices, subqueryCount, samplingRate);
}

Settings TStorageDistributedBase::PrepareLeafJobSettings(const Settings& settings)
{
    Settings newSettings = settings;

    newSettings.queue_max_wait_ms = Cluster::saturate(
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

ThrottlerPtr TStorageDistributedBase::CreateNetThrottler(
    const Settings& settings)
{
    ThrottlerPtr throttler;
    if (settings.max_network_bandwidth || settings.max_network_bytes) {
        throttler = std::make_shared<Throttler>(
            settings.max_network_bandwidth,
            settings.max_network_bytes,
            "Limit for bytes to send or receive over network exceeded.");
    }
    return throttler;
}

BlockInputStreamPtr TStorageDistributedBase::CreateLocalStream(
    const ASTPtr& queryAst,
    const Context& context,
    QueryProcessingStage::Enum processedStage)
{
    InterpreterSelectQuery interpreter(queryAst, context, SelectQueryOptions(processedStage));
    BlockInputStreamPtr stream = interpreter.execute().in;

    // Materialization is needed, since from remote servers the constants come materialized.
    // If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
    // And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
    return std::make_shared<MaterializingBlockInputStream>(stream);
}

BlockInputStreamPtr TStorageDistributedBase::CreateRemoteStream(
    const IClusterNodePtr& remoteNode,
    const ASTPtr& queryAst,
    const Context& context,
    const ThrottlerPtr& throttler,
    const Tables& externalTables,
    QueryProcessingStage::Enum processedStage)
{
    const auto* queryContext = GetQueryContext(context);

    std::string query = queryToString(queryAst);

    // TODO(max42): can be done only once?
    Block header = materializeBlock(InterpreterSelectQuery(queryAst, context, SelectQueryOptions(processedStage).analyze()).getSampleBlock());

    auto stream = std::make_shared<RemoteBlockInputStream>(
        remoteNode->GetConnection(),
        query,
        header,
        context,
        nullptr,    // will use settings from context
        throttler,
        externalTables,
        processedStage);

    stream->setPoolMode(PoolMode::GET_MANY);
    auto remoteQueryId = ToString(TQueryId::Create());
    stream->setRemoteQueryId(remoteQueryId);

    return CreateBlockInputStreamLoggingAdapter(std::move(stream), TLogger(queryContext->Logger)
        .AddTag("RemoteQueryId: %v", remoteQueryId)
        .AddTag("RemoteNode: %v", remoteNode->GetName().ToString())
        .AddTag("RemoteStreamId: %v", TGuid::Create()));
}

void TStorageDistributedBase::startup()
{
    if (ClickHouseSchema.Columns.empty()) {
        THROW_ERROR_EXCEPTION("CHYT does not support tables without schema")
            << TErrorAttribute("path", getTableName());
    }
    setColumns(DB::ColumnsDescription(ClickHouseSchema.Columns));
    SpecTemplate.Columns = ClickHouseSchema.Columns;
    SpecTemplate.ReadSchema = ReadSchema;
}

std::string TStorageDistributedBase::getName() const
{
    return "YTStaticTable";
}

bool TStorageDistributedBase::isRemote() const
{
    return true;
}

bool TStorageDistributedBase::supportsIndexForIn() const
{
    return ClickHouseSchema.HasPrimaryKey();
}

bool TStorageDistributedBase::mayBenefitFromIndexForIn(const DB::ASTPtr& /* queryAst */, const DB::Context& /* context */) const
{
    return supportsIndexForIn();
}

const TClickHouseTableSchema& TStorageDistributedBase::GetSchema() const
{
    return ClickHouseSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
