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

class TClickHouseInvoker
    : public TInvokerWrapper
{
public:
    using TInvokerWrapper::TInvokerWrapper;

    void Invoke(TClosure callback) override
    {
        auto doInvoke = [currentThread = DB::current_thread, callback = std::move(callback), this_ = MakeStrong(this)] {
            // CH has per-thread state stored in thread local variable DB::current_thread.
            // We need this state to be per-fiber in our code, so we store it within a fiber
            // and replace DB::current_thread variable with it every time we enter the fiber.
            DB::ThreadStatus* fiberThreadStatus = currentThread;

            auto attach = [&fiberThreadStatus] {
                // TODO(dakovalkov): investigate why we enter it several times.
                // YT_VERIFY(DB::current_thread == nullptr);
                std::swap(DB::current_thread, fiberThreadStatus);
            };

            auto detach = [&fiberThreadStatus] {
                std::swap(DB::current_thread, fiberThreadStatus);
                // YT_VERIFY(DB::current_thread == nullptr);
            };

            auto currentThreadRestoreGuard = NConcurrency::TContextSwitchGuard(detach, attach);

            attach();
            auto detachGuard = Finally(detach);

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
