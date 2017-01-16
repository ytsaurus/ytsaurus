#include "private.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger RpcServerLogger("RpcServer");
const NLogging::TLogger RpcClientLogger("RpcClient");

const NProfiling::TProfiler RpcServerProfiler("/rpc/server");
const NProfiling::TProfiler RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
