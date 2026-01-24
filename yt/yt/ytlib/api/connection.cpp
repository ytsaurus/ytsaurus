#include "connection.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

namespace NYT::NApi {

using namespace NAuth;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(
    INodePtr config,
    NNative::TConnectionOptions options)
{
    if (config->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Cluster configuration must be a map node");
    }

    auto genericConfig = ConvertTo<TConnectionConfigPtr>(config);
    switch (genericConfig->ConnectionType) {
        case EConnectionType::Native: {
            auto typedConfig = ConvertTo<NNative::TConnectionCompoundConfigPtr>(config);
            return NNative::CreateConnection(
                std::move(typedConfig),
                std::move(options));
        }

        case EConnectionType::Rpc: {
            auto typedConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(config);
            return NRpcProxy::CreateConnection(
                std::move(typedConfig),
                std::move(options));
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
