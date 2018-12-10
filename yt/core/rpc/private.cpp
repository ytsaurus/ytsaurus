#include "private.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger RpcServerLogger("RpcServer");
const NLogging::TLogger RpcClientLogger("RpcClient");

const NProfiling::TProfiler RpcServerProfiler("/rpc/server");
const NProfiling::TProfiler RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
