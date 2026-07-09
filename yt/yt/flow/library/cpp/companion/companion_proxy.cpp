#include "companion_proxy.h"

#include <contrib/libs/grpc/include/grpc/impl/grpc_types.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, NYTree::INodePtr> BuildCompanionGrpcArguments()
{
    THashMap<std::string, NYTree::INodePtr> grpcArguments;
    // Max gRPC message size equals to std::numeric_limits<i32>::max() (2^31 - 1 bytes).
    // Non-unlimited for Java compatibility and error handling simplicity.
    grpcArguments[GRPC_ARG_MAX_SEND_MESSAGE_LENGTH] = NYT::NYTree::ConvertToNode(std::numeric_limits<i32>::max());
    grpcArguments[GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH] = NYT::NYTree::ConvertToNode(std::numeric_limits<i32>::max());
    // Per-computation channels all target 0.0.0.0:<port> with identical args; gRPC's global
    // subchannel pool would dedup them to one connection, pinning all RPCs to a single worker.
    // A local subchannel pool keeps each channel a distinct connection so SO_REUSEPORT fans
    // them across the forked workers.
    grpcArguments[GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL] = NYT::NYTree::ConvertToNode(1);
    return grpcArguments;
}

NRpc::NGrpc::TChannelConfigPtr BuildCompanionChannelConfig(const std::string& address)
{
    auto channelConfig = New<NYT::NRpc::NGrpc::TChannelConfig>();
    channelConfig->Address = address;
    channelConfig->GrpcArguments = BuildCompanionGrpcArguments();
    return channelConfig;
}

TCompanionProxy CreateCompanionProxy(const std::string& address)
{
    auto channel = NYT::NRpc::NGrpc::CreateGrpcChannel(BuildCompanionChannelConfig(address));
    auto proxy = TCompanionProxy(std::move(channel));
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
