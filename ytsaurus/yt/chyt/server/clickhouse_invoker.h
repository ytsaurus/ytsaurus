#include "private.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! This method is one of the most beautiful parts of CH -> YT interop.
//! Many CH functionality is based on DB::ThreadStatus TLS state which
//! stores current memory tracker, profiling stuff, query id, query context, etc.
//! This invoker steals DB::current_thread from caller, sets it in before invoking
//! callback and properly restores it on context switches.
//!
//! NB: this wrapper is intended only for passing execution from CH to YT invokers
//! in synchronous manner. You should immediately call WaitFor on obtained future
//! making caller sleep. Remember that caller stack defines lifetime of DB::ThreadStatus
//! object pointed by DB::current_thread. If caller stack unwinds while wrapped
//! callback is still running, you will end up with segfault.
IInvokerPtr CreateClickHouseInvoker(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
