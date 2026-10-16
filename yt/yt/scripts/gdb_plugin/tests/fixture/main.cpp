// Harness for the gdb_plugin tests: set up each fixture group (ref-counted
// retention scenarios, pretty-printer objects, tcmalloc allocations -- each in its
// own translation unit), then freeze at a clean breakpoint so the test can dump a
// core.

#include "refcount_fixtures.h"
#include "printer_fixtures.h"
#include "tcmalloc_fixtures.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>  // full TIntrusivePtr for the returned TRefCountedPtr

#include <util/system/compiler.h>

// A clean breakpoint site reached with everything set up.
void StopHere()
{
    volatile int dummy = 0;
    Y_UNUSED(dummy);
}

int main()
{
    // The ref-counted scenarios return the object that must be pinned only by
    // this (main) thread's stack -- keep it live as a local across the breakpoint.
    auto onMainStack = SetupGdbRefCountFixtures();

    // Pretty-printer fixtures (table-client rows, guid, error, yson).
    SetupGdbPrinterFixtures();

    // tcmalloc page-map fixture (a large single-span allocation).
    SetupGdbTcmallocFixtures();

    StopHere();
    Y_UNUSED(onMainStack);
    return 0;
}
