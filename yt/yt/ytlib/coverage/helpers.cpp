#include "helpers.h"

#include <library/cpp/yt/error/error.h>

#ifdef __AFL_COMPILER

// The following entities are defined at the instrumenting compiler.
extern "C" {
    void __afl_copy_coverage(ui8* buf, ui32 len);
}

extern ui32 __afl_map_size;

#endif

namespace NYT::NCoverage {

////////////////////////////////////////////////////////////////////////////////

std::string ReadCoverageOrThrow()
{
#ifdef __AFL_COMPILER
    std::string result;
    result.resize(__afl_map_size);
    __afl_copy_coverage(reinterpret_cast<ui8*>(result.data()), __afl_map_size);
    return result;
#else
    THROW_ERROR_EXCEPTION("Unsupported, consider instrumenting the binary first");
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoverage
