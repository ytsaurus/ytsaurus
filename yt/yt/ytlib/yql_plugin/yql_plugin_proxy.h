#pragma once

#include <yt/yt/core/rpc/client.h>

#include <yt/yt/ytlib/yql_plugin/proto/yql_plugin.pb.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TYqlPluginProxy, TYqlPluginService,
                     .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NProto, Start);
    DEFINE_RPC_PROXY_METHOD(NProto, RunQuery);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortQuery);
    DEFINE_RPC_PROXY_METHOD(NProto, GetQueryProgress);
    DEFINE_RPC_PROXY_METHOD(NProto, GetUsedClusters);
    DEFINE_RPC_PROXY_METHOD(NProto, OnDynamicConfigChanged);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
