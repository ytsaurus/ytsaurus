#pragma once

#include <yt/ytlib/controller_agent/controller_agent_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TControllerAgentServiceProxy, ControllerAgentService,
        .SetProtocolVersion(4));

    DEFINE_RPC_PROXY_METHOD(NProto, GetOperationInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, GetJobInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, RegisterOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, InitializeOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, PrepareOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, MaterializeOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, ReviveOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, CommitOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, CompleteOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, WriteOperationControllerCoreDump);
    DEFINE_RPC_PROXY_METHOD(NProto, UnregisterOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateOperationRuntimeParameters);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

