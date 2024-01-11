#include <yt/yt/library/program/program.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/chyt/client/query_service_proxy.h>

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
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
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
        auto* chytRequest = req->mutable_chyt_request();
        chytRequest->set_query(Query_);

        auto* settings = chytRequest->mutable_settings();
        (*settings)["limit"] = "1000";
        (*settings)["chyt.list_dir.max_size"] = "1000";

        if (RowCountLimit_ != -1) {
            req->set_row_count_limit(RowCountLimit_);
        }

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        Cout << Format("Query id: %v", FromProto<TGuid>(rsp->query_id())) << Endl;

        const auto& error = FromProto<TError>(rsp->error());
        Cout << "Result: " << ((error.IsOK()) ? "OK" : "Error") << Endl;
        if (error.IsOK()) {
            for (size_t i = 0; i < rsp->Attachments().size(); ++i) {
                if (rsp->Attachments()[i].Empty()) {
                    Cout << "Empty attachment" << Endl;
                    continue;
                }
                auto wireRowset = rsp->Attachments()[i];
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
    TString Address_;
    TString Query_;
    TString QueryPath_;
    i64 RowCountLimit_ = -1;
    THashMap<TString, TString> Settings_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TChytQuery().Run(argc, argv);
}
