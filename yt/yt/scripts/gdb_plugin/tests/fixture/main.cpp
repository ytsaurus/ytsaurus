// Harness for the gdb_plugin tests: set up both fixture groups (ref-counted
// retention scenarios and pretty-printer objects, each in its own translation
// unit), then freeze at a clean breakpoint so the test can dump a core.

#include "refcount_fixtures.h"
#include "printer_fixtures.h"

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

    StopHere();
    Y_UNUSED(onMainStack);
    return 0;
}
