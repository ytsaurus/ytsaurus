#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger RpcServerLogger("RpcServer");
const NLogging::TLogger RpcClientLogger("RpcClient");

NProfiling::TProfiler RpcServerProfiler("/rpc/server");
NProfiling::TProfiler RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
