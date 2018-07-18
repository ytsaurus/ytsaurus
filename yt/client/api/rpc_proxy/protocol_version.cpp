#include <yt/client/api/rpc_proxy/protocol_version.h>
#include <yt/client/api/rpc_proxy/protocol_version_variables.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::TProtocolVersion GetCurrentProtocolVersion()
{
    return {
        YTRpcProxyProtocolVersionMajor,
        YTRpcProxyProtocolVersionMinor
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
