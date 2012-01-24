#pragma once

#include <util/system/yassert.h>

#include "preprocessor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Evaluates the expression #expr.
//! In debug mode also throws an error if #expr is false.
#define YVERIFY(expr) \
    do { \
        try { \
            if (EXPECT_FALSE( !(expr) )) { \
                if (YaIsDebuggerPresent()) { \
                    __debugbreak(); \
                } else { \
                    assert(0&&(expr)); \
                } \
            } \
        } catch (const std::exception& ex) { \
            if (YaIsDebuggerPresent()) { \
                __debugbreak(); \
            } else { \
                assert(0&&"Exception was thrown while evaluating YVERIFY: " \
                    __FILE__ ":" PP_STRINGIZE(__LINE__) ":" PP_STRINGIZE(expr) \
                ); \
            } \
        } \
    } while (0)

// TODO: Add extended diagnostics why the process was terminated.

//! Marker for unreachable code. Abnormally terminates current process.
#ifdef __GNUC__
#define YUNREACHABLE() \
    do { \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while(0)
#else
#define YUNREACHABLE() \
    ::std::terminate()
#endif

//! Marker for unimplemented methods. Abnormally terminates current process.
#ifdef __GNUC__
#define YUNIMPLEMENTED() \
    do { \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while(0)
#else
#define YUNIMPLEMENTED() \
    ::std::terminate()
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
