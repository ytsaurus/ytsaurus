#include <gtest/gtest.h>

#include <yt/yt/library/mlock/mlock.h>

TEST(Mlock, Call)
{
    ASSERT_TRUE(NYT::MlockFileMappings(false));
    ASSERT_TRUE(NYT::MlockFileMappings(true));
}