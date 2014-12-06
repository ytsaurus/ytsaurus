#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

const NLog::TLogger RpcServerLogger("RpcServer");
const NLog::TLogger RpcClientLogger("RpcClient");

NProfiling::TProfiler RpcServerProfiler("/rpc/server");
NProfiling::TProfiler RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
