#include "clickhouse_invoker.h"

#include <yt/core/misc/finally.h>

#include <yt/core/concurrency/fiber_api.h>

#include <yt/core/actions/invoker_detail.h>

#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseInvoker
    : public TInvokerWrapper
{
public:
    using TInvokerWrapper::TInvokerWrapper;

    virtual void Invoke(TClosure callback) override
    {
        auto doInvoke = [currentThread = DB::current_thread, callback = std::move(callback), this_ = MakeStrong(this)] {
            auto attach = [currentThread] {
                DB::current_thread = currentThread;
            };

            auto detach = [currentThread] {
                YT_ASSERT(DB::current_thread == currentThread);
                DB::current_thread = nullptr;
            };

            auto currentThreadRestoreGuard = NConcurrency::TContextSwitchGuard(detach, attach);

            attach();
            auto detachGuard = Finally(detach);

            TCurrentInvokerGuard guard(this_);

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
