#include "libunwind.h"

#include <stdlib.h>

#include <cstdio>

extern "C" {

#define UNW_LOCAL_ONLY
#include <contrib/libs/libunwind/include/libunwind.h>

} // extern "C"

namespace NYT::NLibunwind {

////////////////////////////////////////////////////////////////////////////////

int GetStackTrace(void** result, int maxFrames, int skipFrames)
{
    // Ignore this frame.
    skipFrames += 1;

    unw_cursor_t cursor;
    unw_context_t context;

    int frames = 0;

    static thread_local bool AlreadyUnwinding;
    if (AlreadyUnwinding) {
        return 0;
    }
    AlreadyUnwinding = true;

    if (unw_getcontext(&context) != 0) {
        fprintf(stderr, "unw_getcontext failed; terminating\n");
        abort();
    }

    if (unw_init_local(&cursor, &context) != 0) {
        fprintf(stderr, "unw_init_local failed; terminating\n");
        abort();
    }

    while (frames < maxFrames) {
        unw_word_t ip = 0;
        int rv = unw_get_reg(&cursor, UNW_REG_IP, &ip);
        if (rv < 0) {
            break;
        }

        if (skipFrames > 0) {
            --skipFrames;
        } else {
            result[frames] = reinterpret_cast<void*>(ip);
            ++frames;
        }

        rv = unw_step(&cursor);
        if (rv <= 0) {
            break;
        }
    }

    AlreadyUnwinding = false;
    return frames;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLibunwind

