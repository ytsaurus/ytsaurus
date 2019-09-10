#include "scheduler.h"
#include "fiber.h"
#include "fls.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

Y_POD_THREAD(IScheduler*) CurrentScheduler;
Y_POD_THREAD(TFiberId) CurrentFiberId;
Y_POD_THREAD(const TFiber*) CurrentFiber;

////////////////////////////////////////////////////////////////////////////////

NProfiling::TCpuDuration GetCurrentRunCpuTime()
{
    const auto* fiber = GetCurrentFiber();
    return fiber->GetRunCpuTime();
}

/////////////////////////////////////////////////////////////////////////////

static class TFiberIdGenerator
{
public:
    TFiberIdGenerator()
    {
        Seed_.store(static_cast<TFiberId>(::time(nullptr)));
    }

    TFiberId Generate()
    {
        const TFiberId Factor = std::numeric_limits<TFiberId>::max() - 173864;
        YT_ASSERT(Factor % 2 == 1); // Factor must be coprime with 2^n.

        while (true) {
            auto seed = Seed_++;
            auto id = seed * Factor;
            if (id != InvalidFiberId) {
                return id;
            }
        }
    }

private:
    std::atomic<TFiberId> Seed_;

} FiberIdGenerator;

TFiberId GenerateFiberId()
{
    return FiberIdGenerator.Generate();
}

////////////////////////////////////////////////////////////////////////////////

bool CheckFreeStackSpace(size_t space)
{
    auto* fiber = TryGetCurrentFiber();
    return !fiber || fiber->CheckFreeStackSpace(space);
}

////////////////////////////////////////////////////////////////////////////////

void Yield()
{
    Y_UNUSED(WaitFor(VoidFuture));
}

void SwitchTo(IInvokerPtr invoker)
{
    YT_ASSERT(invoker);
    GetCurrentScheduler()->SwitchTo(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(std::function<void()> out, std::function<void()> in)
{
    auto* fiber = TryGetCurrentFiber();
    if (fiber) {
        const_cast<TFiber*>(fiber)->PushContextHandler(std::move(out), std::move(in));
    }
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    auto* fiber = TryGetCurrentFiber();
    if (fiber) {
        const_cast<TFiber*>(fiber)->PopContextHandler();
    }
}

////////////////////////////////////////////////////////////////////////////////

TOneShotContextSwitchGuard::TOneShotContextSwitchGuard(std::function<void()> handler)
    : TContextSwitchGuard(
        [this, handler = std::move(handler)] () noexcept {
            if (!Active_) {
                return;
            }
            Active_ = false;
            handler();
        },
        [] () noexcept { })
    , Active_(true)
{ }

////////////////////////////////////////////////////////////////////////////////

TForbidContextSwitchGuard::TForbidContextSwitchGuard()
    : TOneShotContextSwitchGuard( [] { YT_ABORT(); })
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
