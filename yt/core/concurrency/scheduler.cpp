#include "scheduler.h"
#include "fiber.h"
#include "fls.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

PER_THREAD IScheduler* CurrentScheduler = nullptr;

IScheduler* GetCurrentScheduler()
{
    YCHECK(CurrentScheduler);
    return CurrentScheduler;
}

IScheduler* TryGetCurrentScheduler()
{
    return CurrentScheduler;
}

void SetCurrentScheduler(IScheduler* scheduler)
{
    YCHECK(!CurrentScheduler);
    CurrentScheduler = scheduler;
}

////////////////////////////////////////////////////////////////////////////////

PER_THREAD TFiberId CurrentFiberId = InvalidFiberId;

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

TFiberId GetCurrentFiberId()
{
    return CurrentFiberId;
}

void SetCurrentFiberId(TFiberId id)
{
    CurrentFiberId = id;
}

////////////////////////////////////////////////////////////////////////////////

void Yield()
{
    WaitFor(VoidFuture);
}

void SwitchTo(IInvokerPtr invoker)
{
    Y_ASSERT(invoker);
    GetCurrentScheduler()->SwitchTo(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(std::function<void()> handler)
    : Active_(true)
{
    GetCurrentScheduler()->PushContextSwitchHandler([this, handler = std::move(handler)] () noexcept {
        Y_ASSERT(Active_);
        handler();
        Active_ = false;
    });
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    if (Active_) {
        GetCurrentScheduler()->PopContextSwitchHandler();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

