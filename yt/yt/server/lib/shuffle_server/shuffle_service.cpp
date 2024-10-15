#include "shuffle_service.h"

#include <yt/yt/ytlib/shuffle_client/shuffle_service_proxy.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NLogging;
using namespace NRpc;
using namespace NRpcProxy;
using namespace NShuffleClient;

////////////////////////////////////////////////////////////////////////////////

class TShuffleService
    : public TServiceBase
{
public:
    TShuffleService(
        IInvokerPtr invoker,
        TLogger logger,
        TString localServerAddress)
        : TServiceBase(
            invoker,
            TShuffleServiceProxy::GetDescriptor(),
            std::move(logger))
        , LocalServerAddress_(std::move(localServerAddress))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartShuffle));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishShuffle));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterChunks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FetchChunks));
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, StartShuffle)
    {
        ThrowUnimplemented("StartShuffle");
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, FinishShuffle)
    {
        ThrowUnimplemented("FinishShuffle");
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, RegisterChunks)
    {
        ThrowUnimplemented("RegisterChunks");
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, FetchChunks)
    {
        ThrowUnimplemented("FetchChunks");
    }

private:
    const TString LocalServerAddress_;
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateShuffleService(
    IInvokerPtr invoker,
    TLogger logger,
    TString localServerAddress)
{
    return New<TShuffleService>(
        std::move(invoker),
        std::move(logger),
        std::move(localServerAddress));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
