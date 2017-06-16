#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/url.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TUrlUnescapeTest, Simple)
{
    EXPECT_EQ("hello world !", UnescapeUrl("hello%20world%20!"));
    EXPECT_EQ("\"quoted\"", UnescapeUrl("%22quoted%22"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
