#pragma once

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

#undef YASSERT

#ifdef NDEBUG
    #define YASSERT(expr) \
        do { \
            if (false) { \
                (void) (expr); \
            } \
        } while (false)
#else
    #define YASSERT(expr) \
        do { \
            if (UNLIKELY(!(expr))) { \
                ASSERT_TRAP("YASSERT", #expr); \
            } \
        } while (false)
#endif

//! Same as |YASSERT| but evaluates and checks the expression in both release and debug mode.
#define YCHECK(expr) \
    do { \
        if (UNLIKELY(!(expr))) { \
            ASSERT_TRAP("YCHECK", #expr); \
        } \
    } while (false)

//! Unreachable code marker. Abnormally terminates the current process.
#define YUNREACHABLE() \
    do { \
        ASSERT_TRAP("YUNREACHABLE", ""); \
    } while (false)

//! Unimplemented code marker. Abnormally terminates the current process.
#define YUNIMPLEMENTED() \
    do { \
        ASSERT_TRAP("YUNIMPLEMENTED", ""); \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
