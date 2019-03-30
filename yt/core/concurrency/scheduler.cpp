#include "scheduler.h"
#include "fiber.h"
#include "fls.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

Y_POD_THREAD(IScheduler*) CurrentScheduler;
Y_POD_THREAD(TFiberId) CurrentFiberId;
Y_POD_THREAD(const TFiber*) CurrentFiber;

////////////////////////////////////////////////////////////////////////////////

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
        Y_ASSERT(Factor % 2 == 1); // Factor must be coprime with 2^n.

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

void Yield()
{
    Y_UNUSED(WaitFor(VoidFuture));
}

void SwitchTo(IInvokerPtr invoker)
{
    Y_ASSERT(invoker);
    GetCurrentScheduler()->SwitchTo(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(std::function<void()> out, std::function<void()> in)
{
    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        scheduler->PushContextSwitchHandler(std::move(out), std::move(in));
    }
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        scheduler->PopContextSwitchHandler();
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
    : TOneShotContextSwitchGuard( [] { Y_UNREACHABLE(); })
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
