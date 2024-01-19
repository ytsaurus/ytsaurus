#include "query_service.h"

#include "config.h"
#include "conversion.h"
#include "helpers.h"
#include "host.h"
#include "query_context.h"

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <Core/Types.h>
#include <Common/ThreadPool.h>
#include <Common/SettingsChanges.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
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

    TExecuteQueryCall(const TRequest* request, const TString& user, TQueryId queryId, THost* host)
        : Request_(request)
        , Host_(host)
        , User_(user)
        , QueryId_(queryId)
        , CompositeSettings_(Host_->GetConfig()->QuerySettings->Composite)
    { }

    TErrorOr<TRowset> Execute()
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

        return TRowset{WireRowset_, TotalRowCount_};
    }

private:
    const TRequest* Request_;
    THost* Host_;

    TString User_;
    TQueryId QueryId_;
    DB::ContextMutablePtr QueryContext_;
    TString Query_;
    DB::BlockIO BlockIO_;
    TCompositeSettingsPtr CompositeSettings_;

    TSharedRef WireRowset_;
    i64 TotalRowCount_ = 0;

    void Run()
    {
        PrepareContext();

        DB::CurrentThread::QueryScope queryScope(QueryContext_);

        BuildPipeline();
        ProcessPipeline();
    }

    void PrepareContext()
    {
        auto& chytRequest = Request_->chyt_request();
        Query_ = chytRequest.query();

        RegisterNewUser(
            Host_->GetContext()->getAccessControl(),
            User_,
            Host_->HasUserDefinedSqlObjectStorage());

        // Query context is inherited from session context like it was made in ClickHouse gRPC server.
        DB::Session session(Host_->GetContext(), DB::ClientInfo::Interface::GRPC);
        session.authenticate(User_, /*password=*/ "", Poco::Net::SocketAddress());
        QueryContext_ = session.makeQueryContext();

        QueryContext_->setInitialQueryId(ToString(QueryId_));
        QueryContext_->setCurrentQueryId(ToString(QueryId_));
        QueryContext_->setInitialUserName(User_);
        QueryContext_->setQueryKind(DB::ClientInfo::QueryKind::INITIAL_QUERY);

        DB::SettingsChanges settingsChanges;
        for (const auto& [key, value] : chytRequest.settings()) {
            std::string_view view(value);
            settingsChanges.emplace_back(key, DB::Field(view));
        }
        QueryContext_->checkSettingsConstraints(settingsChanges, DB::SettingSource::QUERY);
        QueryContext_->applySettingsChanges(settingsChanges);

        auto traceContext = NTracing::TTraceContext::NewRoot("ChytRPCQueryHandler");

        SetupHostContext(Host_, QueryContext_, QueryId_, std::move(traceContext));
    }

    void BuildPipeline()
    {
        BlockIO_ = DB::executeQuery(Query_, QueryContext_);
    }

    void ProcessPipeline()
    {
        if (!BlockIO_.pipeline.initialized()) {
            return;
        }

        // insert into, create table ...
        if (BlockIO_.pipeline.completed()) {
            DB::CompletedPipelineExecutor executor(BlockIO_.pipeline);
            executor.execute();
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

        TotalRowCount_ = std::ssize(rowset);

        ConvertToWireRowset(schema, rowset);
    }

    void ConvertToWireRowset(const TTableSchema& schema, const std::vector<TUnversionedRow>& rowset)
    {
        auto writer = CreateWireProtocolWriter();
        writer->WriteTableSchema(schema);
        writer->WriteSchemafulRowset(rowset);

        struct TChytRefMergeTag {};
        WireRowset_ = MergeRefsToRef<TChytRefMergeTag>(writer->Finish());
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
            columnSchemas.emplace_back(ToString(column.name), ToLogicalType(column.type, CompositeSettings_));
        }
        return TTableSchema(columnSchemas);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public TServiceBase
{
public:
    explicit TQueryService(THost* host, const IInvokerPtr& invoker)
        : TServiceBase(
            invoker,
            TQueryServiceProxy::GetDescriptor(),
            ClickHouseYtLogger)
        , Host_(host)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecuteQuery));
    }

private:
    THost* Host_;

    DECLARE_RPC_SERVICE_METHOD(NProto, ExecuteQuery)
    {
        const auto& user = context->GetAuthenticationIdentity().User;
        auto queryId = request->has_query_id()
            ? FromProto<TQueryId>(request->query_id())
            : TQueryId::Create();
        ToProto(response->mutable_query_id(), queryId);

        context->SetRequestInfo("RPC request received (QueryId: %v, User: %v, Query: %v, RowCountLimit: %v)",
            queryId,
            user,
            request->chyt_request().query(),
            request->row_count_limit());

        context->SetRequestInfo("Starting to execute query (QueryId: %v)", queryId);

        TExecuteQueryCall call(request, user, queryId, Host_);
        auto rowsetOrError = call.Execute();

        if (rowsetOrError.IsOK()) {
            context->SetRequestInfo("Query execution finished successfully (QueryId: %v, RowCount: %v)",
                queryId,
                rowsetOrError.Value().TotalRowCount);
            response->Attachments() = {std::move(rowsetOrError.Value().Rowset)};
        } else {
            context->SetRequestInfo("Query execution finished with error (QueryId: %v, Error: %v)",
                queryId,
                rowsetOrError);
            ToProto(response->mutable_error(), rowsetOrError);
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
