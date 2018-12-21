#include <yt/client/api/rpc_proxy/protocol_version.h>
#include <yt/client/api/rpc_proxy/protocol_version_variables.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::TProtocolVersion GetCurrentProtocolVersion()
{
    return {
        YTRpcProxyProtocolVersionMajor,
        YTRpcProxyProtocolVersionMinor
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
