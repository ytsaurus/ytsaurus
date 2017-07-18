#include "connection.h"
#include "config.h"
#include "native_connection.h"

#include <yt/ytlib/rpc_proxy/config.h>
#include <yt/ytlib/rpc_proxy/rpc_proxy_connection.h>

namespace NYT {
namespace NApi {

using namespace NYTree;
using namespace NRpcProxy;

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(INodePtr config)
{
    if (config->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Cluster configuration must be a map node");
    }

    auto genericConfig = ConvertTo<TConnectionConfigPtr>(config);
    switch (genericConfig->ConnectionType) {
        case EConnectionType::Native: {
            auto typedConfig = ConvertTo<TNativeConnectionConfigPtr>(config);
            return CreateNativeConnection(typedConfig);
        }
        case EConnectionType::Rpc: {
            auto typedConfig = ConvertTo<TRpcProxyConnectionConfigPtr>(config);
            return CreateRpcProxyConnection(typedConfig);
        }
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

