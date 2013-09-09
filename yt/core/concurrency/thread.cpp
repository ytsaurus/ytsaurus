#include "stdafx.h"
#include "thread.h"

#ifndef _win_
      #include <pthread.h>
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

bool RaiseCurrentThreadPriority()
{
#ifdef _win_
    return SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST) != 0;
#else
    struct sched_param param = { };
    param.sched_priority = 31;
    return pthread_setschedparam(pthread_self(), SCHED_RR, &param) == 0;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
