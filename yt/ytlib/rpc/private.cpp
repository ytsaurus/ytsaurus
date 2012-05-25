#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger RpcServerLogger("RpcServer");
NLog::TLogger RpcClientLogger("RpcClient");

NProfiling::TProfiler RpcServerProfiler("/rpc/server");
NProfiling::TProfiler RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
