#include "clickhouse_invoker.h"

#include <yt/core/misc/finally.h>

#include <yt/core/concurrency/scheduler_api.h>

#include <yt/core/actions/invoker_detail.h>

#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static thread_local i32 ClickHouseFrameCount = 0;

class TClickHouseInvoker
    : public TInvokerWrapper
{
public:
    using TInvokerWrapper::TInvokerWrapper;

    virtual void Invoke(TClosure callback) override
    {
        if (!DB::current_thread) {
            YT_ASSERT(ClickHouseFrameCount == 0);
            UnderlyingInvoker_->Invoke(callback);
            return;
        }

        auto doInvoke = [currentThread = DB::current_thread, callback = std::move(callback), this_ = MakeStrong(this)] {
            auto attach = [currentThread] {
                if (ClickHouseFrameCount == 0) {
                    YT_ASSERT(DB::current_thread == nullptr);
                    DB::current_thread = currentThread;
                } else {
                    YT_ASSERT(DB::current_thread == currentThread);
                }
                ++ClickHouseFrameCount;
            };

            auto detach = [currentThread] {
                YT_ASSERT(ClickHouseFrameCount > 0);
                YT_ASSERT(DB::current_thread == currentThread);
                --ClickHouseFrameCount;
                if (ClickHouseFrameCount == 0) {
                    DB::current_thread = nullptr;
                }
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
