#include <yt/yt/library/program/program.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT {

using namespace NBus;
using namespace NLogging;
using namespace NRpc;
using namespace NRpc::NBus;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NClickHouseServer;

static const auto Logger = TLogger("ChytQuery");

////////////////////////////////////////////////////////////////////////////////

class TChytQuery
    : public TProgram
{
public:
    TChytQuery()
    {
        Opts_.AddLongOption("address").StoreResult(&Address_).Required();
        Opts_.AddLongOption("query").StoreResult(&Query_);
        Opts_.AddLongOption("query-path").StoreResult(&QueryPath_);
        Opts_.AddLongOption("row-count-limit").StoreResult(&RowCountLimit_);
        Opts_.AddLongOption("poll-progress").StoreTrue(&PollProgressFlag_);
        Opts_.AddLongOption("poll-period-ms").StoreResult(&PollPeriodMs_);
        Opts_.AddLongOption("hide-results").StoreTrue(&HideResultsFlag_);
    }

protected:
    void DoRun() override
    {

        auto pool = CreateThreadPool(2, "ChytQuery");

        if (!Query_ && !QueryPath_) {
            THROW_ERROR_EXCEPTION("Either \"query\" or \"query_path\" must be specified");
        }
        if (Query_ && QueryPath_) {
            THROW_ERROR_EXCEPTION("\"query\" or \"query_path\" cannot be specified simultaneously");
        }

        if (QueryPath_) {
            Query_ = TFileInput(QueryPath_).ReadAll();
        }

        auto channelFactory = CreateTcpBusChannelFactory(New<TBusConfig>());
        auto channel = channelFactory->CreateChannel(Address_);
        TQueryServiceProxy proxy(channel);
        auto req = proxy.ExecuteQuery();
        auto queryId = TGuid::Create();
        ToProto(req->mutable_query_id(), queryId);
        auto* chytRequest = req->mutable_chyt_request();
        chytRequest->set_query(Query_);

        auto* settings = chytRequest->mutable_settings();
        (*settings)["limit"] = "1000";
        (*settings)["chyt.list_dir.max_size"] = "1000";

        if (RowCountLimit_ != -1) {
            req->set_row_count_limit(RowCountLimit_);
        }

        auto rsp = req->Invoke();
        if (PollProgressFlag_) {
            Cout << "Polling progress..." << Endl;
            WaitFor(BIND(&TChytQuery::PollProgress, this, proxy, queryId)
                .AsyncVia(pool->GetInvoker())
                .Run()).ThrowOnError();
        }
        auto result = WaitFor(rsp)
            .ValueOrThrow();
        if (!HideResultsFlag_) {
            Cout << Format("Query id: %v", FromProto<TGuid>(result->query_id())) << Endl;

            const auto& error = FromProto<TError>(result->error());
            Cout << "Result: " << ((error.IsOK()) ? "OK" : "Error") << Endl;
            if (error.IsOK()) {
                for (size_t i = 0; i < result->Attachments().size(); ++i) {
                    if (result->Attachments()[i].Empty()) {
                        Cout << "Empty attachment" << Endl;
                        continue;
                    }
                    auto wireRowset = result->Attachments()[i];
                    auto wireReader = CreateWireProtocolReader(wireRowset);
                    auto schema = wireReader->ReadTableSchema();
                    auto schemaData = IWireProtocolReader::GetSchemaData(schema);
                    auto rowset = wireReader->ReadSchemafulRowset(schemaData, /*captureData*/ true);
                    Cout << "Schema: " << ConvertToYsonString(schema, EYsonFormat::Pretty).ToString() << Endl;
                    Cout << "Rowset:" << Endl;
                    for (const auto& row : rowset) {
                        Cout << ToString(row) << Endl;
                    }
                }
            } else {
                Cout << ToString(error) << Endl;
            }
        }
    }

private:
    void PrintProgressAndTs(const NClickHouseServer::NProto::TMultiQueryProgressValues& progress, const TInstant& ts)
    {
        Cout << "Queries count: " << progress.queries_count() << Endl;
        for (int i = 0; i < progress.progresses_size(); ++i) {
            auto queryProgress = progress.progresses()[i];
            std::string isFinishedStr = "";
            if (queryProgress.total_progress().finished()) {
                isFinishedStr = " - [FINISHED] - ";
            }
            Cout << "Query_id: " << Format("%v", FromProto<TGuid>(queryProgress.query_id())) << isFinishedStr << Endl;
            Cout << '[' << ts << ']' << Endl;
            int secondaryQueriesCount = queryProgress.secondary_query_ids_size();
        for (int i = 0; i < secondaryQueriesCount; ++i) {
            auto queryId = queryProgress.secondary_query_ids()[i];
            auto secondaryQueryProgress = queryProgress.secondary_query_progresses()[i];

            isFinishedStr = "";
            if (secondaryQueryProgress.finished()) {
                isFinishedStr = " - [FINISHED] - ";
            }

            Cout << "   " << Format("%v", FromProto<TGuid>(queryId)) << isFinishedStr << ": Read-row = "<< secondaryQueryProgress.read_rows() << '/' << secondaryQueryProgress.total_rows_to_read() <<
                "; Read-bytes = " << secondaryQueryProgress.read_bytes() << '/' << secondaryQueryProgress.total_bytes_to_read() << Endl;
        }
        Cout << "   Total progress: Read-rows = " << queryProgress.total_progress().read_rows() << '/' << queryProgress.total_progress().total_rows_to_read() <<
            "; Read-bytes = " << queryProgress.total_progress().read_bytes() << '/' << queryProgress.total_progress().total_bytes_to_read() << Endl;
        }
    }

    void PollProgress(TQueryServiceProxy proxy, TGuid queryId)
    {
        do {
            auto req = proxy.GetQueryProgress();
            ToProto(req->mutable_query_id(), queryId);

            auto result = WaitFor(req->Invoke())
                .ValueOrThrow();

            auto start = TInstant::Now();
            if (result->has_multi_progress()) {
                PrintProgressAndTs(result->multi_progress(), start);
                auto finished_count = 0;
                for (int i = 0; i < result->multi_progress().progresses_size(); ++i) {
                    auto queryProgress = result->multi_progress().progresses()[i];
                    if (queryProgress.total_progress().finished()) {
                        finished_count++;
                    }
                }
                Cout << "Finished queries: " << finished_count << Endl << "***" << Endl;
                if (finished_count == result->multi_progress().progresses_size()) {
                    break;
                }
            } else {
                Cout << '[' << start << "] Empty progress" << Endl << "***" << Endl;
            }

            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(PollPeriodMs_));
        } while (true);
    }

private:
    TString Address_;
    TString Query_;
    TString QueryPath_;
    i64 RowCountLimit_ = -1;
    bool PollProgressFlag_ = false;
    bool HideResultsFlag_ = false;
    i64 PollPeriodMs_ = 100;
    THashMap<TString, TString> Settings_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TChytQuery().Run(argc, argv);
}
