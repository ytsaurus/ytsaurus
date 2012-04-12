
#include <ytlib/misc/url.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TUrlUnescapeTest, Simple)
{
    EXPECT_EQ("hello world !", UnescapeUrl("hello%20world%20!"));
    EXPECT_EQ("\"quoted\"", UnescapeUrl("%22quoted%22"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
