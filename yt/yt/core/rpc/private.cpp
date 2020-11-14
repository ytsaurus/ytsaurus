#include "private.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger RpcServerLogger("RpcServer");
const NLogging::TLogger RpcClientLogger("RpcClient");

const NProfiling::TRegistry RpcServerProfiler("/rpc/server");
const NProfiling::TRegistry RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
