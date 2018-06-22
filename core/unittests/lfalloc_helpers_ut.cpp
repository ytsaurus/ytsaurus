#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/lfalloc_helpers.h>

#include <util/system/compiler.h>

#ifndef _asan_enabled_

TEST(TLFAllocHelpersTest, DynamicBinding)
{
    auto stuff = std::make_unique<int>(0);
    EXPECT_GT(NYT::NLFAlloc::GetUserAllocated(), 0);
}

#endif
