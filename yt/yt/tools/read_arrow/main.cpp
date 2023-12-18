#include <yt/yt/library/program/program.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/core/misc/serialize_dump.h>

namespace NYT {

using namespace NAuth;
using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TReadArrow
    : public TProgram
{
public:
    TReadArrow()
    {
        Opts_.AddLongOption("cluster").StoreResult(&Cluster_);
        Opts_.AddLongOption("proxy-address").StoreResult(&ProxyAddress_);
        Opts_.AddLongOption("path").StoreResult(&Path_).Required();
        Opts_.AddLongOption("fallback-format").StoreResult(&FallbackFormat_);
        Opts_.AddLongOption("hexify-format").StoreResult(&HexifyFormat_).NoArgument()   ;
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
        if (!Cluster_.empty()) {
            connectionConfig->ClusterUrl = Cluster_;
        }
        if (!ProxyAddress_.empty()) {
            connectionConfig->ProxyAddresses = {ProxyAddress_};
        }
        auto connection = CreateConnection(connectionConfig);

        auto clientOptions = TClientOptions();
        clientOptions.Token = LoadToken();

        auto client = DynamicPointerCast<TClient>(connection->CreateClient(clientOptions));
        YT_VERIFY(client);

        auto apiServiceProxy = client->CreateApiServiceProxy();

        auto request = apiServiceProxy.ReadTable();
        client->InitStreamingRequest(*request);
        request->set_path(Path_);
        request->set_desired_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_ARROW);
        request->set_arrow_fallback_rowset_format(NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
        request->set_format(FallbackFormat_);

        auto stream = WaitFor(CreateRpcClientInputStream(std::move(request)))
            .ValueOrThrow();

        auto metaRef = WaitFor(stream->Read())
            .ValueOrThrow();

        NRpcProxy::NProto::TRspReadTableMeta meta;
        if (!TryDeserializeProto(&meta, metaRef)) {
            THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
        }

        Cerr << meta.DebugString() << Endl;

        i64 refIndex = 0;
        while (auto block = WaitFor(stream->Read()).ValueOrThrow()) {
            Cout << Endl;
            Cout << "Block " << refIndex << Endl;
            ++refIndex;

            NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
            NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
            auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

            Cout << "Descriptor:" << Endl << descriptor.DebugString() << Endl;
            Cout << "Statistics:" << Endl << statistics.DebugString() << Endl;

            if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_FORMAT) {
                Cout << "Format data:" << Endl;
                if (HexifyFormat_) {
                    Cout << DumpRangeToHex(payloadRef) << Endl;
                } else {
                    Cout << payloadRef.ToStringBuf() << Endl;
                }
            } else if (descriptor.rowset_format() == NApi::NRpcProxy::NProto::RF_ARROW) {
                Cout << "Arrow data:" << Endl << DumpRangeToHex(payloadRef) << Endl;
            }
        }
    }

private:
    TString Cluster_;
    TString ProxyAddress_;
    TString Path_;
    TString FallbackFormat_ = "<format=text>yson";
    bool HexifyFormat_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TReadArrow().Run(argc, argv);
}
