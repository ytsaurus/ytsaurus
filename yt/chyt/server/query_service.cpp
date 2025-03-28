#include "query_service.h"

#include "config.h"
#include "conversion.h"
#include "helpers.h"
#include "host.h"
#include "query_context.h"
#include "query_registry.h"

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <Core/Types.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProto;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// TODO(dakovalkov): log something.
static const TLogger Logger("RpcQueryService");

////////////////////////////////////////////////////////////////////////////////

struct TRowset
{
    TSharedRef Rowset;
    i64 TotalRowCount;
};

template <class TRequest>
class TExecuteQueryCall
{
public:
    using TThis = TExecuteQueryCall<TRequest>;

    TExecuteQueryCall(const TRequest* request, const std::string& user, TQueryId queryId, THost* host)
        : Request_(request)
        , Host_(host)
        , User_(user)
        , InitialQueryId_(queryId)
        , CompositeSettings_(TCompositeSettings::Create(/*convertUnsupportedTypesToString*/ true))
    { }

    TErrorOr<std::vector<TRowset>> Execute()
    {
        auto completedPromise = NewPromise<void>();
        auto completedFuture = completedPromise.ToFuture();

        ThreadFromGlobalPool masterThread([this, promise = std::move(completedPromise)] () mutable {
            try {
                Run();
                promise.Set();
            } catch (const std::exception& ex) {
                promise.Set(TError(ex));
            }
        });

        TError error = WaitFor(completedFuture);

        masterThread.join();

        if (!error.IsOK()) {
            return error;
        }

        return Result_;
    }

private:
    const TRequest* Request_;
    THost* Host_;

    std::string User_;
    TQueryId InitialQueryId_;
    DB::ContextMutablePtr QueryContext_;
    DB::BlockIO BlockIO_;
    TCompositeSettingsPtr CompositeSettings_;
    std::vector<TRowset> Result_;

    void Run()
    {
        PrepareContext();

        auto queries = SplitClickHouseQueries(Request_->chyt_request().query(), QueryContext_);
        std::vector<TQueryId> additionalQueryIds;
        additionalQueryIds.reserve(queries.size());
        while (additionalQueryIds.size() < queries.size()) {
            additionalQueryIds.push_back(TQueryId::Create());
        }
        auto traceContext = NTracing::TTraceContext::NewRoot("ChytRpcQueryHandler");
        if (queries.size() > 1) {
            // Need fake context to get additional_query_ids from queryRegistry while polling progress.
            SetupHostContext(Host_, QueryContext_, InitialQueryId_, traceContext, std::nullopt, std::nullopt, nullptr, {}, additionalQueryIds);
        } else if (queries.size() == 1) {
            additionalQueryIds[0] = InitialQueryId_;
        }
        // Move fake context if has one to keep it alive while processing queries.
        auto initialHostContext = std::move(QueryContext_->getHostContext());

        DB::CurrentThread::QueryScope queryScope(QueryContext_);
        for (size_t i = 0; i < queries.size(); ++i) {
            SetupHostContext(Host_, QueryContext_, additionalQueryIds[i], traceContext);
            SetNextQueryId(additionalQueryIds[i]);
            BuildPipeline(TString(queries[i]));
            ProcessPipeline();
        }
    }

    void SetNextQueryId(TQueryId queryId)
    {
        QueryContext_->setCurrentQueryId(ToString(queryId));
        QueryContext_->setInitialQueryId(ToString(InitialQueryId_));
    }

    void PrepareContext()
    {
        RegisterNewUser(
            Host_->GetContext()->getAccessControl(),
            User_,
            Host_->GetUserDefinedDatabaseNames(),
            Host_->HasUserDefinedSqlObjectStorage());

        // Query context is inherited from session context like it was made in ClickHouse gRPC server.
        DB::Session session(Host_->GetContext(), DB::ClientInfo::Interface::GRPC);
        session.authenticate(User_, /*password=*/ "", DBPoco::Net::SocketAddress());
        session.makeSessionContext();
        QueryContext_ = session.makeQueryContext();
        QueryContext_->makeSessionContext();

        QueryContext_->setInitialUserName(User_);
        QueryContext_->setQueryKind(DB::ClientInfo::QueryKind::INITIAL_QUERY);
        QueryContext_->setCurrentQueryId(ToString(InitialQueryId_));

        DB::SettingsChanges settingsChanges;
        auto& chytRequest = Request_->chyt_request();
        for (const auto& [key, value] : chytRequest.settings()) {
            std::string_view view(value);
            settingsChanges.emplace_back(key, DB::Field(view));
        }
        QueryContext_->checkSettingsConstraints(settingsChanges, DB::SettingSource::QUERY);
        QueryContext_->applySettingsChanges(settingsChanges);
    }

    void BuildPipeline(const TString& query)
    {
        BlockIO_ = DB::executeQuery(query, QueryContext_).second;
    }

    void ProcessPipeline()
    {
        if (!BlockIO_.pipeline.initialized()) {
            BlockIO_.onFinish();
            return;
        }

        // insert into, create table ...
        if (BlockIO_.pipeline.completed()) {
            DB::CompletedPipelineExecutor executor(BlockIO_.pipeline);
            executor.execute();
            BlockIO_.onFinish();
            return;
        }

        DB::PullingAsyncPipelineExecutor executor(BlockIO_.pipeline);

        const auto& header = executor.getHeader();
        auto schema = GetTableSchema(header);
        auto dataTypes = header.getDataTypes();
        auto columnIndexToId = GetColumnIndexToId(header);

        DB::Block block;
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rowset;
        auto rowCountLimitExceeded = [&] {
            if (!Request_->has_row_count_limit()) {
                return false;
            }
            return std::ssize(rowset) >= Request_->row_count_limit();
        };
        while (!rowCountLimitExceeded() && executor.pull(block)) {
            if (!block) {
                continue;
            }
            auto rowRange = ToRowRange(block, dataTypes, columnIndexToId, CompositeSettings_);
            auto capturedRows = rowBuffer->CaptureRows(rowRange);
            rowset.insert(rowset.end(), capturedRows.begin(), capturedRows.end());
        }

        if (rowCountLimitExceeded()) {
            rowset.resize(Request_->row_count_limit());
        }

        auto totalRowCount = std::ssize(rowset);

        auto wireRowset = ConvertToWireRowset(schema, rowset);
        Result_.emplace_back(TRowset{wireRowset, totalRowCount});
        BlockIO_.onFinish();
    }

    TSharedRef ConvertToWireRowset(const TTableSchema& schema, const std::vector<TUnversionedRow>& rowset)
    {
        auto writer = CreateWireProtocolWriter();
        writer->WriteTableSchema(schema);
        writer->WriteSchemafulRowset(rowset);

        struct TChytRefMergeTag {};
        return MergeRefsToRef<TChytRefMergeTag>(writer->Finish());
    }

    std::vector<int> GetColumnIndexToId(const DB::Block& block)
    {
        std::vector<int> columnIndexToId(block.columns());
        for (size_t id = 0; id < block.columns(); ++id) {
            columnIndexToId[id] = id;
        }
        return columnIndexToId;
    }

    TTableSchema GetTableSchema(const DB::Block& block)
    {
        std::vector<TColumnSchema> columnSchemas;
        columnSchemas.reserve(block.columns());
        for (auto& column : block) {
            columnSchemas.emplace_back(column.name, ToLogicalType(column.type, CompositeSettings_));
        }
        return TTableSchema(columnSchemas);
    }

    static std::vector<std::string> SplitClickHouseQueries(const std::string& input, DB::ContextMutablePtr context)
    {
        std::vector<std::string> queries;

        const auto & settings = context->getSettingsRef();

        auto parseRes = splitMultipartQuery(input, queries,
            settings.max_query_size,
            settings.max_parser_depth,
            settings.max_parser_backtracks,
            settings.allow_settings_after_format_in_insert);
        if (!parseRes.second) {
            THROW_ERROR_EXCEPTION("Cannot parse and execute the following part of query: %s", parseRes.first);
        }

        return queries;
    }
};
////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public TServiceBase
{
public:
    TQueryService(THost* host, IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TQueryServiceProxy::GetDescriptor(),
            ClickHouseYtLogger())
        , Host_(host)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecuteQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryProgress));
    }

private:
    THost* const Host_;

    DECLARE_RPC_SERVICE_METHOD(NProto, ExecuteQuery)
    {
        const auto& user = context->GetAuthenticationIdentity().User;
        auto queryId = FromProto<TQueryId>(request->query_id());

        context->SetRequestInfo("QueryId: %v, Query: %v, RowCountLimit: %v",
            queryId,
            request->chyt_request().query(),
            request->row_count_limit());

        ToProto(response->mutable_query_id(), queryId);

        TExecuteQueryCall call(request, user, queryId, Host_);
        auto rowsetsOrError = call.Execute();

        if (rowsetsOrError.IsOK()) {
            context->SetResponseInfo("QueryId: %v, ResultsCount: %v",
                queryId,
                rowsetsOrError.Value().size());
            std::vector<TSharedRef> attachments;
            for (auto& rowset : rowsetsOrError.Value()) {
                attachments.push_back(std::move(rowset.Rowset));
            }
            response->Attachments() = std::move(attachments);
        } else {
            context->SetResponseInfo("QueryId: %v, Error: %v",
                queryId,
                rowsetsOrError);
            ToProto(response->mutable_error(), rowsetsOrError);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetQueryProgress)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());

        context->SetRequestInfo("QueryId: %v", queryId);

        auto isFinishedCount = 0;
        auto additionalQueryIds = WaitFor(Host_->GetQueryRegistry()->GetAdditionalQueryIds(queryId)).ValueOrThrow();
        response->mutable_multi_progress()->set_queries_count(additionalQueryIds.size());
        for (const auto& additionalQueryId : additionalQueryIds) {
            auto queryProgress = WaitFor(Host_->GetQueryRegistry()->GetQueryProgress(additionalQueryId)).ValueOrThrow();
            if (queryProgress && queryProgress->TotalProgress.Finished) {
                ++isFinishedCount;
            }
            ToProto(response->mutable_multi_progress()->mutable_progresses()->Add(), additionalQueryId, queryProgress);
        }
        if (response->multi_progress().progresses().size() > 0) {
            context->SetResponseInfo("QueryId: %v, ProgressesCount: %v, IsFinishedCount: %v", queryId, response->multi_progress().progresses().size(), isFinishedCount);
        } else {
            context->SetResponseInfo(
                "No progress found because the query has already finished or was initiated on another instance");
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateQueryService(THost* host, const IInvokerPtr& invoker)
{
    return New<TQueryService>(host, invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
