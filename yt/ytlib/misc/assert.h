#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Evaluate the expression a. In debug mode throws an error if (a == false)
#define YVERIFY( a ) do { \
        try { \
            if ( EXPECT_FALSE( !(a) ) ) { \
                if (YaIsDebuggerPresent()) __debugbreak(); else assert(0&&(a)); \
            } \
        } catch (...) { \
            if (YaIsDebuggerPresent()) __debugbreak(); else assert(false && "Exception during verification"); \
        } \
    } while (0)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
