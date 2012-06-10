#pragma once

#include <util/system/yassert.h>

#include "preprocessor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Evaluates the expression #expr.
//! In debug mode also throws an error if #expr is false.
#define YVERIFY(expr) \
    do { \
        if (EXPECT_FALSE( !(expr) )) { \
            if (YaIsDebuggerPresent()) { \
                __debugbreak(); \
            } else { \
                assert(0&&(expr)); \
            } \
        } \
    } while (0)

// TODO: Add extended diagnostics why the process was terminated.

//! Marker for unreachable code. Abnormally terminates current process.
#ifdef __GNUC__
#define YUNREACHABLE() \
    do { \
        ::std::fputs( \
            "YUNREACHABLE(): " __FILE__ ":" PP_STRINGIZE(__LINE__) "\n", \
            stderr); \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while (0)
#else
#define YUNREACHABLE() \
    ::std::terminate()
#endif

//! Marker for unimplemented methods. Abnormally terminates current process.
#ifdef __GNUC__
#define YUNIMPLEMENTED() \
    do { \
        ::std::fputs( \
            "YUNIMPLEMENTED(): " __FILE__ ":" PP_STRINGIZE(__LINE__) "\n", \
            stderr); \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while (0)
#else
#define YUNIMPLEMENTED() \
    ::std::terminate()
#endif


// TODO(panin): extract common part from YASSERT and YCHECK
//! Do the same as YASSERT but both in release and debug mode
#define YCHECK( a ) \
    do { \
        try { \
            if ( EXPECT_FALSE( !(a) ) ) { \
                if (YaIsDebuggerPresent()) __debugbreak(); else assert(0&&(a)); \
            } \
        } catch (...) { \
            if (YaIsDebuggerPresent()) __debugbreak(); else assert(false && "Exception during assert"); \
        } \
    } while (0)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
