#include "spin_wait_hook.h"

#include <atomic>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

static std::atomic<TSpinWaitSlowPathHook> Hook;

TSpinWaitSlowPathHook GetSpinWaitSlowPathHook()
{
    return Hook.load();
}

void SetSpinWaitSlowPathHook(TSpinWaitSlowPathHook hook)
{
    Hook.store(hook);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
