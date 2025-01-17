#include "distributed_chunk_session_service.h"

#include "private.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NDistributedChunkSessionClient;
using namespace NRpc;

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionService
    : public TServiceBase
{
public:
    TDistributedChunkSessionService(
        TDistributedChunkSessionServiceConfigPtr /*config*/,
        IInvokerPtr invoker,
        IConnectionPtr /*connection*/)
        : TServiceBase(
            invoker,
            TDistributedChunkSessionServiceProxy::GetDescriptor(),
            DistributedChunkSessionServiceLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishSession));
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, StartSession)
    {
        YT_UNIMPLEMENTED();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, PingSession)
    {
        YT_UNIMPLEMENTED();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, SendBlocks)
    {
        YT_UNIMPLEMENTED();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, FinishSession)
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDistributedChunkSessionService(
    TDistributedChunkSessionServiceConfigPtr config,
    IInvokerPtr invoker,
    IConnectionPtr connection)
{
    return New<TDistributedChunkSessionService>(
        std::move(config),
        std::move(invoker),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
