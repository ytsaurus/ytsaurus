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
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
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
            WaitFor(BIND(&TChytQuery::PollProgress, this, proxy, queryId)
                .AsyncVia(pool->GetInvoker())
                .Run()).ThrowOnError();
        }
        auto result = WaitFor(rsp)
            .ValueOrThrow();

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

private:
    void PrintProgressAndTs(const NClickHouseServer::NProto::TQueryProgressValues& progress, const TInstant& ts)
    {
        Cout << '[' << ts << ']' << Endl;
        int secondaryQueriesCount = progress.secondary_query_ids_size();
        for (int i = 0; i < secondaryQueriesCount; ++i) {
            auto queryId = progress.secondary_query_ids()[i];
            auto queryProgress = progress.secondary_query_progresses()[i];

            std::string isFinishedStr = "";
            if (queryProgress.finished()) {
                isFinishedStr = " - [FINISHED] - ";
            }

            Cout << "   " << Format("%v", FromProto<TGuid>(queryId)) << isFinishedStr << ": Read-row = "<< queryProgress.read_rows() << '/' << queryProgress.total_rows_to_read() <<
                "; Read-bytes = " << queryProgress.read_bytes() << '/' << queryProgress.total_bytes_to_read() << Endl;
        }
        Cout << "   Total progress: Read-rows = " << progress.total_progress().read_rows() << '/' << progress.total_progress().total_rows_to_read() <<
            "; Read-bytes = " << progress.total_progress().read_bytes() << '/' << progress.total_progress().total_bytes_to_read() << Endl;
    }

    void PollProgress(TQueryServiceProxy proxy, TGuid queryId)
    {
        do {
            auto req = proxy.GetQueryProgress();
            ToProto(req->mutable_query_id(), queryId);

            auto result = WaitFor(req->Invoke())
                .ValueOrThrow();

            auto start = TInstant::Now();
            if (result->has_progress()) {
                PrintProgressAndTs(result->progress(), start);
                if (result->progress().total_progress().finished()) {
                    break;
                }
            } else {
                Cout << '[' << start << "] Empty progress" << Endl;
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
    i64 PollPeriodMs_ = 100;
    THashMap<TString, TString> Settings_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TChytQuery().Run(argc, argv);
}
