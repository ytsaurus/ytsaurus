#include "connection.h"

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/config.h>

#include <yt/client/api/rpc_proxy/config.h>
#include <yt/client/api/rpc_proxy/connection.h>

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(INodePtr config)
{
    if (config->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Cluster configuration must be a map node");
    }

    auto genericConfig = ConvertTo<TConnectionConfigPtr>(config);
    switch (genericConfig->ConnectionType) {
        case EConnectionType::Native: {
            auto typedConfig = ConvertTo<NNative::TConnectionConfigPtr>(config);
            return NNative::CreateConnection(typedConfig);
        }
        case EConnectionType::Rpc: {
            auto typedConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(config);
            return NRpcProxy::CreateConnection(typedConfig);
        }
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

