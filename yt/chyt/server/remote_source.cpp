#include "remote_source.h"

#include "logging_transform.h"
#include "query_analyzer.h"
#include "query_context.h"
#include "secondary_query_header.h"

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

namespace NYT::NClickHouseServer {

using namespace NTracing;
using namespace NLogging;
using namespace NYson;

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


    void setRowsBeforeLimitCounter(DB::RowsBeforeLimitCounterPtr counter) override
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
    TLogger logger)
{
    const auto& queryAst = secondaryQuery.Query;
    const auto& Logger = logger;

    auto* queryContext = GetQueryContext(context);

    std::string query = queryToString(queryAst);

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

    auto remoteQueryExecutor = std::make_shared<DB::RemoteQueryExecutor>(
        remoteNode->GetConnection(),
        query,
        blockHeader,
        context,
        throttler,
        scalars,
        externalTables,
        processingStage);
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
        addExtremes = context->getSettingsRef().extremes;
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

} // namespace NYT::NClickHouseServer
