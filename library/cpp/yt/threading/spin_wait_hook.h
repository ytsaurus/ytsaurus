#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using TSpinWaitSlowPathHook = void(*)(TCpuDuration cpuDelay);

TSpinWaitSlowPathHook GetSpinWaitSlowPathHook();
void SetSpinWaitSlowPathHook(TSpinWaitSlowPathHook hook);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
