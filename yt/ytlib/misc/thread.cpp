#include "stdafx.h"
#include "thread.h"

#include <util/system/thread.i>

#ifdef _unix_
    #include <sys/prctl.h>
#endif

namespace NYT {
namespace NThread {

////////////////////////////////////////////////////////////////////////////////

#ifdef _win_
// copied from http://msdn.microsoft.com/en-us/library/xcb2z8hs%28VS.80%29.aspx

#define MS_VC_EXCEPTION 0x406D1388

namespace {

typedef struct tagTHREADNAME_INFO
{
   DWORD dwType; // Must be 0x1000.
   LPCSTR szName; // Pointer to name (in user addr space).
   DWORD dwThreadID; // Thread ID (-1=caller thread).
   DWORD dwFlags; // Reserved for future use, must be zero.
} THREADNAME_INFO;

void SetThreadName(DWORD dwThreadID, LPCSTR szThreadName)
{
   THREADNAME_INFO info;
   info.dwType = 0x1000;
   info.szName = szThreadName;
   info.dwThreadID = dwThreadID;
   info.dwFlags = 0;

   __try
   {
      RaiseException(MS_VC_EXCEPTION, 0, sizeof(info)/sizeof(DWORD), (DWORD*)&info);
   }
   __except(EXCEPTION_CONTINUE_EXECUTION)
   {
   }
}

}

#undef MS_VC_EXCEPTION

#endif

void SetCurrentThreadName(const char* name)
{
#ifdef _win_
    SetThreadName(-1, name);
#else
    prctl(PR_SET_NAME, name, 0, 0, 0);
#endif
}

size_t GetCurrentThreadId()
{
    return SystemCurrentThreadIdImpl();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NThread
} // namespace NYT
