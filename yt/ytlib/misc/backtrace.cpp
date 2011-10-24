#include "stdafx.h"
#include "backtrace.h"

#if defined(__GNUC__)
#include <execinfo.h>
#endif

#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void PrintCallStackAux(TOutputStream& output, const char* file, int line)
{
#if defined(__GNUC__)
    const size_t maximalDepth = 64;

    size_t stackDepth;
    void *stackAddresses[maximalDepth];
    char **stackStrings;

    stackDepth = backtrace(stackAddresses, maximalDepth);
    stackStrings = backtrace_symbols(stackAddresses, stackDepth);

    output << "** Call Stack from " << file << ":" << line << Endl;

    // NB: Omitting topmost frame: PrintCallStack
    for (size_t i = 1; i < stackDepth; ++i) {
        output << "    " << stackStrings[i] << Endl;
    }

    free(stackStrings); // malloc()ed by backtrace_symbols.
#else
    UNUSED(output);
    UNUSED(file);
    UNUSED(line);
    abort();
#endif
}

void Break()
{
#if defined(__GNUC__)
    asm("int3");
#else
    abort();
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
