#include "private.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger RpcServerLogger("RpcServer");
const NLogging::TLogger RpcClientLogger("RpcClient");

const NProfiling::TRegistry RpcServerProfiler("yt/rpc/server");
const NProfiling::TRegistry RpcClientProfiler("yt/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
