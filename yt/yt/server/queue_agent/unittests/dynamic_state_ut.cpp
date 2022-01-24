#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/queue_agent/dynamic_state.h>

namespace NYT::NQueueAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCrossClusterReferenceTest, FromString)
{
    EXPECT_EQ(
        (TCrossClusterReference{"kek", "keker"}),
        TCrossClusterReference::FromString("kek:keker"));

    EXPECT_EQ(
        (TCrossClusterReference{"haha", "haha:haha:"}),
        TCrossClusterReference::FromString("haha:haha:haha:"));

    EXPECT_THROW(TCrossClusterReference::FromString("hahahaha"), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueAgent
