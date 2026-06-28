#include "tcmalloc_fixtures.h"

#include <vector>

namespace {

std::vector<char> RootedLargeAlloc;  // a multi-MB single-span (large) tcmalloc block

} // namespace

// C linkage so the gdb test can name the address without mangling.
extern "C" {
void* GdbLargeAllocAddress = nullptr;  // start of a large (single-span) tcmalloc allocation
} // extern "C"

void SetupGdbTcmallocFixtures()
{
    // A large allocation: bigger than the largest tcmalloc size class, so it is
    // served as a single (size-class 0) span rather than carved into objects. The
    // tcmalloc page-map walk must snap an interior pointer to the span start.
    RootedLargeAlloc.resize(4 << 20); // 4 MiB
    GdbLargeAllocAddress = RootedLargeAlloc.data();
}
