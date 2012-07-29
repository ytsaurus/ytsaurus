#pragma once

#include <util/system/yassert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Evaluates the expression #expr.
//! In debug mode also throws an error if #expr is false.
#ifndef __clang__
#define YVERIFY(expr) \
    do { \
        if (UNLIKELY( !(expr) )) { \
            if (YaIsDebuggerPresent()) { \
                __debugbreak(); \
            } else { \
                assert(0&&(expr)); \
            } \
        } \
    } while (0)
#else
#define YVERIFY(expr) \
    do { \
        if (UNLIKELY( !(expr) )) { \
            assert(0&&(expr)); \
        } \
    } while (0)
#endif

// TODO: Add extended diagnostics why the process was terminated.
#define _ASSERT_X(s) _ASSERT_Y(s)
#define _ASSERT_Y(s) #s
#define _ASSERT_AT __FILE__ ":" _ASSERT_X(s)

//! Marker for unreachable code. Abnormally terminates current process.
#ifdef __GNUC__
#define YUNREACHABLE() \
    do { \
        ::std::fputs( \
            "YUNREACHABLE(): " _ASSERT_AT "\n", \
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
            "YUNIMPLEMENTED(): " _ASSERT_AT "\n", \
            stderr); \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while (0)
#else
#define YUNIMPLEMENTED() \
    ::std::terminate()
#endif


// TODO(panin): extract common part from YASSERT and YCHECK
//! Do the same as YASSERT but both in release and debug mode.
#define YCHECK( a ) \
    do { \
        try { \
            if ( UNLIKELY( !(a) ) ) { \
                if (YaIsDebuggerPresent()) __debugbreak(); else assert(0&&(a)); \
            } \
        } catch (...) { \
            if (YaIsDebuggerPresent()) __debugbreak(); else assert(false && "Exception during assert"); \
        } \
    } while (0)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
