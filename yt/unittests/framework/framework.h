#pragma once

// STLport does not have std::tr1::tuple, so we have to fallback to custom
// implementation.
#define GTEST_HAS_TR1_TUPLE 1
#define GTEST_USE_OWN_TR1_TUPLE 1

// Preconfigure all the namespaces; i. e. bind ::std to ::NStl
#include <util/private/stl/config.h>
#include <util/private/stl/stlport-5.1.4/stlport/stl/config/features.h>

namespace NStl {
    namespace tr1 {
    }
}

#include <util/random/random.h>

#include "framework/gtest.h"
#include "framework/gmock.h"

#undef EXPECT_TRUE
#define EXPECT_TRUE(x) EXPECT_IS_TRUE(x)
#undef EXPECT_FALSE
#define EXPECT_FALSE(x) EXPECT_IS_FALSE(x)

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka GenerateRandomFileName(const char* prefix)
{
    return Sprintf("%s-%016" PRIx64 "-%016" PRIx64,
        prefix
        MicroSeconds(),
        RandomNumber<ui64>());
}

////////////////////////////////////////////////////////////////////////////////
 
} // namespace NYT

