#include <yt/ytlib/rpc_proxy/protocol_version.h>
#include <yt/ytlib/rpc_proxy/protocol_version_variables.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::TProtocolVersion GetCurrentProtocolVersion()
{
    return {
        YtRpcProxyProtocolVersionMajor,
        YtRpcProxyProtocolVersionMinor
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
