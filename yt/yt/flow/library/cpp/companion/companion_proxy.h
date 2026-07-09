#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

class TCompanionProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCompanionProxy, NYT.NFlow.NProto.NCompanion.CompanionService);

    DEFINE_RPC_PROXY_METHOD(NProto::NCompanion, ProcessBatch);

    DEFINE_RPC_PROXY_METHOD(NProto::NCompanion, CompanionInfo);

    DEFINE_RPC_PROXY_METHOD(NProto::NCompanion, PutJob);

    DEFINE_RPC_PROXY_METHOD(NProto::NCompanion, GetJfr);
};

////////////////////////////////////////////////////////////////////////////////

//! gRPC channel arguments shared by every companion client channel.
//! Exposed for testing so a regression dropping an argument is caught without a
//! live companion.
THashMap<std::string, NYTree::INodePtr> BuildCompanionGrpcArguments();

//! Channel config every companion client channel is created from.
//! Exposed for testing so a regression in the wiring (config not carrying the
//! arguments) is caught without a live companion.
NRpc::NGrpc::TChannelConfigPtr BuildCompanionChannelConfig(const std::string& address);

TCompanionProxy CreateCompanionProxy(const std::string& address);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
