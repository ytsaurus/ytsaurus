#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/lfalloc_helpers.h>

TEST(TLFAllocHelpersTest, DynamicBinding)
{
    auto stuff = std::make_unique<int>(0);
    EXPECT_GT(NYT::NLFAlloc::GetUserAllocated(), 0);
}
