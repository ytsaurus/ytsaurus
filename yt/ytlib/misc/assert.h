#pragma once

#include <util/system/yassert.h>

// TODO(ignat): delete namespace, because it doesn't depend on define declarations
namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: Add extended diagnostics why the process was terminated.
#define _ASSERT_X(s) _ASSERT_Y(s)
#define _ASSERT_Y(s) #s
#define _ASSERT_AT __FILE__ ":" _ASSERT_X(__LINE__)

#ifdef __GNUC__

#define YCHECK_FAILED(expr) \
    do { \
        ::std::fputs( \
            "YCHECK(" #expr "): " _ASSERT_AT "\n", \
            stderr); \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while (0)

#define YVERIFY_FAILED(expr) \
    do { \
        ::std::fputs( \
            "YVERIFY(" #expr "): " _ASSERT_AT "\n", \
            stderr); \
        __builtin_trap(); \
        __builtin_unreachable(); \
    } while (0)

#else

#define YCHECK_FAILED(expr) \
    ::std::terminate()

#define YVERIFY_FAILED(expr) \
    ::std::terminate()

#endif

//! Evaluates the expression #expr.
//! In debug mode also throws an error if #expr is false.

#ifdef NDEBUG

#define YVERIFY(expr) \
    (void) (expr)

#else

#define YVERIFY(expr) \
    do { \
        if (UNLIKELY( !(expr) )) { \
            YVERIFY_FAILED(expr); \
        } \
    } while (0)

#endif

//! Do the same as |YASSERT| but both in release and debug mode.
#define YCHECK(expr) \
    do { \
        if (UNLIKELY(!(expr))) { \
            YCHECK_FAILED(expr); \
        } \
    } while (0)

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
