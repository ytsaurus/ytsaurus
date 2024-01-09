#include "clickhouse_invoker.h"

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/invoker_detail.h>

#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>

#include <iostream>
#include <thread>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

// NB: AttachThreadStatus and DetachThreadStatus access a thread local variable
// DB::current_thread. If those function are inlined, the compiller may calculate
// an absolute variable address and use it both times to access the variable.
// However, since there might be context switches between those calls, we could
// end up in a diffrent thread, and previously calculated address would not be
// valid anymore. So, noinline is important here to force the compiller to
// calculate current thread local address every time the function is called.

Y_NO_INLINE void AttachThreadStatus(DB::ThreadStatus* status)
{
    YT_VERIFY(DB::current_thread == nullptr);
    DB::current_thread = status;
}

Y_NO_INLINE void DetachThreadStatus()
{
    YT_VERIFY(DB::current_thread != nullptr);
    DB::current_thread = nullptr;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TClickHouseInvoker
    : public TInvokerWrapper
{
public:
    using TInvokerWrapper::TInvokerWrapper;

    void Invoke(TClosure callback) override
    {
        // Why do you use TClickHouseInvoker from a non-ClickHouse context?
        // YT_VERIFY(DB::current_thread != nullptr);
        // TODO(dakovalkov): HealthChecker starts queries from a non-ClickHouse context. Eliminate it.
        if (!DB::current_thread) {
            UnderlyingInvoker_->Invoke(callback);
            return;
        }

        auto doInvoke = [threadStatus = DB::current_thread, callback = std::move(callback), this_ = MakeStrong(this)] {
            // CH has per-thread state stored in thread local variable DB::current_thread.
            // We need this state to be per-fiber in our code, so we store it within a fiber
            // and replace DB::current_thread variable with it every time we enter the fiber.
            auto currentThreadRestoreGuard = NConcurrency::TContextSwitchGuard(DetachThreadStatus, BIND(AttachThreadStatus, threadStatus));

            AttachThreadStatus(threadStatus);
            auto detachGuard = Finally(DetachThreadStatus);

            callback.Run();
        };

        UnderlyingInvoker_->Invoke(BIND(doInvoke));
    }
};

/////////////////////////////////////////////////////////////////////////////

IInvokerPtr CreateClickHouseInvoker(IInvokerPtr invoker)
{
    return New<TClickHouseInvoker>(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
