#pragma once

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void AssertTrapImpl(
    const char* trapType,
    const char* expr,
    const char* file,
    int line);

} // namespace NDetail

#ifdef __GNUC__
    #define BUILTIN_UNREACHABLE() __builtin_unreachable()
    #define BUILTIN_TRAP()        __builtin_trap()
#else
    #define BUILTIN_UNREACHABLE() std::terminate()
    #define BUILTIN_TRAP()        std::terminate()
#endif

#define ASSERT_TRAP(trapType, expr) \
    ::NYT::NDetail::AssertTrapImpl(trapType, expr, __FILE__, __LINE__); \
    BUILTIN_UNREACHABLE() \

#undef Y_ASSERT

#ifdef NDEBUG
    #define Y_ASSERT(expr) \
        do { \
            if (false) { \
                (void) (expr); \
            } \
        } while (false)
#else
    #define Y_ASSERT(expr) \
        do { \
            if (Y_UNLIKELY(!(expr))) { \
                ASSERT_TRAP("Y_ASSERT", #expr); \
            } \
        } while (false)
#endif

//! Same as |Y_ASSERT| but evaluates and checks the expression in both release and debug mode.
#define YCHECK(expr) \
    do { \
        if (Y_UNLIKELY(!(expr))) { \
            ASSERT_TRAP("YCHECK", #expr); \
        } \
    } while (false)

//! Unreachable code marker. Abnormally terminates the current process.
#ifdef Y_UNREACHABLE
#undef Y_UNREACHABLE
#endif
#ifdef YT_COMPILING_UDF
    #define Y_UNREACHABLE() __builtin_unreachable()
#else
    #define Y_UNREACHABLE() \
        do { \
            ASSERT_TRAP("Y_UNREACHABLE", ""); \
        } while (false)
#endif

//! Unimplemented code marker. Abnormally terminates the current process.
#define Y_UNIMPLEMENTED() \
    do { \
        ASSERT_TRAP("Y_UNIMPLEMENTED", ""); \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
