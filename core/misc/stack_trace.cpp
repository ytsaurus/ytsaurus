#include "stack_trace.h"

#include <atomic>

#include <stdlib.h>

#ifndef _win_

extern "C" {
    #define UNW_LOCAL_ONLY
    #include <contrib/libs/libunwind_master/include/libunwind.h>
} // extern "C"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static std::atomic<bool> AlreadyUnwinding;

int GetStackTrace(void** result, int maxFrames, int skipFrames)
{
    // Ignore this frame.
    skipFrames += 1;

    void* ip;
    unw_cursor_t cursor;
    unw_context_t context;

    int frames = 0;

    bool expected = false;
    if (!AlreadyUnwinding.compare_exchange_strong(expected, true)) {
        return 0;
    }

    if (unw_getcontext(&context) != 0) {
        abort();
    }
    
    if (unw_init_local(&cursor, &context) != 0) {
        abort();
    }

    while (frames < maxFrames) {
        int rv = unw_get_reg(&cursor, UNW_REG_IP, (unw_word_t *)&ip);
        if (rv < 0) {
            break;
        }

        if (skipFrames > 0) {
            --skipFrames;
        } else {
            result[frames] = ip;
            ++frames;
        }

        rv = unw_step(&cursor);
        if (rv <= 0) {
            break;
        }
    }

    AlreadyUnwinding.store(false);
    return frames;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#endif

